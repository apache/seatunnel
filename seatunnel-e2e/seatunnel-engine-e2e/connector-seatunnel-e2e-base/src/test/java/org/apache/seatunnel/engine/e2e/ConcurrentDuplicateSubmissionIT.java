/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage for the admission contract of a job id that is submitted more than once at
 * the same time.
 *
 * <p><b>The contract.</b> A client retry of the same submission must never turn into a second
 * execution. Concretely, when several clients submit the same {@code jobId} concurrently before the
 * coordinator has published the first {@code JobMaster}:
 *
 * <ol>
 *   <li>the redundant submissions must not fail the caller merely because they lost the race: a
 *       redundant submission of an already accepted job resolves with that job, like the existing
 *       "the job is currently running; no need to submit again" path;
 *   <li>the job id must be admitted exactly once, so the sink sees each source row exactly once
 *       (here exactly {@code row.num} lines, not a multiple of it);
 *   <li>the job must still reach its natural terminal state. The duplicate admissions used to leave
 *       several {@code PhysicalPlan}s driving the same task groups: the tasks finish and write all
 *       rows, but the job stays {@code RUNNING} forever because the competing state machines never
 *       agree on the terminal transition.
 * </ol>
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code CoordinatorService#submitJob}
 * performs the "is this job id already known" check on the RPC thread, but publishes the new {@code
 * JobMaster} from an asynchronous admission task. A client retry (network timeout before the
 * response, an operator re-running the same command, or two submitters using the same explicit job
 * id) that arrives inside that window passes the check, creates a second {@code JobMaster} for the
 * same id and enqueues it next to the first. Both plans then request slots, both deploy the same
 * task groups (the worker skips the redundant deploy), and both update the shared job/pipeline/task
 * state. The failure is silent from the client's point of view — every submission returns success —
 * and only shows up as a job that never finishes, so it is exactly the kind of lifecycle boundary a
 * regression test should pin.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code ClusterSeaTunnelEngineContainer}
 * covers custom/duplicate job ids through the CLI one submission at a time; {@code
 * SplitClusterFaultToleranceIT#testDeployNotDuplicatedWhenMasterKilledDuringDeploy} covers a master
 * failure during deployment, not a second client submission; {@code
 * SplitClusterPendingJobLifecycleFailoverIT} covers queued-job lifecycle and master failover. No
 * existing test submits the same job id from more than one client at the same time, so the window
 * between the synchronous duplicate check and the asynchronous publication is untested.
 *
 * <p>The job itself is deliberately trivial (a bounded FakeSource into a LocalFile sink). The test
 * is about admission, not about the connectors: it only needs a job whose execution is observable
 * (row count) and terminates (so a wedged job is distinguishable from a slow one).
 */
@Slf4j
public class ConcurrentDuplicateSubmissionIT {

    private static final String TEMPLATE_CONF =
            "duplicate-submission/batch_fake_to_localfile_duplicate_submission.conf";

    /**
     * Must match {@code source.FakeSource.row.num} in the template. The sink writes one line per
     * row, so the final line count is the exact execution count multiplied by this value.
     */
    private static final long ROW_NUM = 200;

    /**
     * Enough submitters to make the race reachable on a normally loaded CI machine: all of them are
     * released by the same barrier, while admitting the first job takes tens of milliseconds
     * (deserialize the DAG, build the physical plan and the checkpoint manager).
     */
    private static final int CONCURRENT_SUBMISSIONS = 8;

    private static final long JOB_FINISH_TIMEOUT_SECONDS = 120;

    private static String stackTraceMessages(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            messages.append(current).append('\n');
            current = current.getCause();
        }
        return messages.toString();
    }

    @Test
    public void testConcurrentDuplicateSubmissionsAdmitTheJobOnce() throws Exception {
        String testClusterName =
                "ConcurrentDuplicateSubmissionIT_testConcurrentDuplicateSubmissionsAdmitTheJobOnce";

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        String targetDir = "/tmp/hive/warehouse/" + testClusterName;
        String generatedConf =
                System.getProperty("user.dir")
                        + File.separator
                        + "target"
                        + File.separator
                        + "duplicate-submission.conf";

        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Dynamic slots so every duplicate admission can acquire capacity immediately instead
            // of queueing behind the fixed pool; the test must observe the admission race itself,
            // not a resource-contention side effect.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(true);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            FileUtils.createNewDir(targetDir);
            Map<String, String> values = new HashMap<>();
            values.put("dynamic_test_case_name", targetDir);
            TestUtils.createTestConfigFileFromTemplate(TEMPLATE_CONF, values, generatedConf);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            final long jobId = JobIdGenerator.newJobId();
            final SeaTunnelClient sharedClient = engineClient;
            final CountDownLatch startSubmitting = new CountDownLatch(1);
            final List<ClientJobProxy> proxies = Collections.synchronizedList(new ArrayList<>());
            final List<Throwable> submissionErrors =
                    Collections.synchronizedList(new ArrayList<>());
            List<Thread> submitters = new ArrayList<>();

            for (int i = 0; i < CONCURRENT_SUBMISSIONS; i++) {
                Thread submitter =
                        new Thread(
                                () -> {
                                    try {
                                        JobConfig jobConfig = new JobConfig();
                                        jobConfig.setName("concurrent_duplicate_submission");
                                        ClientJobExecutionEnvironment environment =
                                                sharedClient.createExecutionContext(
                                                        generatedConf,
                                                        null,
                                                        jobConfig,
                                                        seaTunnelConfig,
                                                        jobId);
                                        startSubmitting.await();
                                        proxies.add(environment.execute());
                                    } catch (Throwable e) {
                                        submissionErrors.add(e);
                                    }
                                },
                                "duplicate-submitter");
                submitter.start();
                submitters.add(submitter);
            }

            startSubmitting.countDown();
            for (Thread submitter : submitters) {
                submitter.join(TimeUnit.SECONDS.toMillis(120));
                Assertions.assertFalse(
                        submitter.isAlive(),
                        "A duplicate submission did not return within 120 seconds");
            }

            Assertions.assertTrue(
                    submissionErrors.isEmpty(),
                    "A redundant submission of an in-flight job must resolve with that job instead "
                            + "of failing the caller, but got: "
                            + submissionErrors);
            Assertions.assertEquals(
                    CONCURRENT_SUBMISSIONS,
                    proxies.size(),
                    "Every submitter must receive a job proxy for the accepted job");

            // All the proxies refer to the one accepted job id; a duplicate admission would be
            // invisible here, which is why the terminal state and the row count below are the real
            // assertions.
            for (ClientJobProxy proxy : proxies) {
                Assertions.assertEquals(
                        jobId, proxy.getJobId(), "Every proxy must point at the submitted job id");
            }

            ClientJobProxy observer = proxies.get(0);
            Awaitility.await()
                    .atMost(JOB_FINISH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED,
                                            observer.getJobStatus(),
                                            "A job submitted concurrently under one job id must "
                                                    + "reach FINISHED; a duplicate admission "
                                                    + "leaves it RUNNING forever"));

            JobResult jobResult = observer.waitForJobCompleteV2();
            Assertions.assertEquals(JobStatus.FINISHED, jobResult.getStatus());

            Assertions.assertEquals(
                    ROW_NUM,
                    FileUtils.getFileLineNumberFromDir(targetDir),
                    "Exactly one execution may be admitted for one job id, so the sink must "
                            + "contain exactly one copy of the source rows");

            // The successful duplicates were deduplicated to the same job, not merely delayed: the
            // job id must not be re-runnable by a late retry now that it has finished, and the sink
            // must not grow. This also proves the in-flight registry is released on the success
            // path instead of pinning the job id forever.
            JobConfig lateJobConfig = new JobConfig();
            lateJobConfig.setName("concurrent_duplicate_submission_late");
            ClientJobExecutionEnvironment lateEnvironment =
                    engineClient.createExecutionContext(
                            generatedConf, null, lateJobConfig, seaTunnelConfig, jobId);
            CompletableFuture<ClientJobProxy> lateSubmission =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return lateEnvironment.execute();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(lateSubmission.isDone()));
            ClientJobProxy lateProxy = null;
            try {
                lateProxy = lateSubmission.get();
            } catch (java.util.concurrent.ExecutionException e) {
                // Rejecting a re-submission of a finished job id is the other acceptable outcome;
                // what must not happen is a successful second execution or a submission that waits
                // forever on a stale in-flight entry. Both diagnostics below prove the rejection:
                // a retained history rejects the duplicate directly, and a not-yet-cleaned
                // terminal state reports the cleanup window.
                String failure = stackTraceMessages(e);
                Assertions.assertTrue(
                        failure.contains("already been submitted")
                                || failure.contains("waiting for terminal state cleanup"),
                        "A late re-submission of a finished job id must fail fast with a "
                                + "duplicate-submission diagnostic, but got: "
                                + failure);
            }
            if (lateProxy != null) {
                Assertions.assertEquals(
                        JobStatus.FINISHED,
                        lateProxy.getJobStatus(),
                        "A late duplicate submission must observe the finished job, not run it "
                                + "again");
            }
            Assertions.assertEquals(
                    ROW_NUM,
                    FileUtils.getFileLineNumberFromDir(targetDir),
                    "A late duplicate submission must not re-run a finished job");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
            FileUtils.deleteFile(targetDir);
            FileUtils.deleteFile(generatedConf);
        }
    }
}
