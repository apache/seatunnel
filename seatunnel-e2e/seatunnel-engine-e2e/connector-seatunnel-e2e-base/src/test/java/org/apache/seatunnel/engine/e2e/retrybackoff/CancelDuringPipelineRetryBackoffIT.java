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

package org.apache.seatunnel.engine.e2e.retrybackoff;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end regression coverage for cancelling a job while its pipeline is sleeping in the restore
 * backoff ({@code job.retry.interval.seconds}).
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A failed pipeline does not restore
 * immediately: {@code SubPlan#prepareRestorePipeline()} resets the pipeline and then waits out the
 * configured retry interval. That wait used to be a straight {@code Thread.sleep} executed inside
 * the {@code synchronized} {@code stateProcess()} of the {@code SubPlan}, while {@code
 * cancelPipeline()} is {@code synchronized} on the same object. A cancel issued during the backoff
 * therefore blocked on the pipeline monitor for the whole remaining interval; because the job's
 * {@code needRestore} flag was already cleared before that block, the woken retry path still
 * allocated a fresh set of slots and deployed another attempt of a job the user had already
 * cancelled. The user-visible damage is a cancellation that appears hung for the length of the
 * retry interval, plus slot pressure and real side effects (connections opened, splits read) from a
 * zombie attempt. The engine's only outward signal is the terminal status arriving late, which is
 * exactly what this test pins.
 *
 * <p><b>Why the existing coverage does not reach it.</b> The fault-tolerance ITs cancel jobs in
 * {@code RUNNING} or wait for a restore to complete; the pending-job ITs cancel jobs that never
 * started; {@code RestApiIT} stops jobs without a retry interval in flight. No test cancels a job
 * while a pipeline is inside its retry backoff, so the monitor/wait interaction and the missing
 * post-backoff revocation check are unobserved end to end.
 *
 * <p><b>How the contract is pinned.</b> A batch job whose sink always fails runs with {@code
 * job.retry.times = 3} and a deliberately long {@code job.retry.interval.seconds = 60}. The test
 * waits until attempt 1 has failed and the pipeline is in the backoff (restore counter 1, pipeline
 * status {@code CREATED}), then cancels the job and requires that: the job reaches {@code CANCELED}
 * well before the retry interval could expire; the terminal {@code CANCELED} state is stable
 * afterwards; the pipeline ends {@code CANCELED} too; and the engine never created a second sink
 * writer, i.e. no further attempt was deployed for the cancelled job.
 *
 * <p>As a control, the same failing pipeline is then submitted with a short retry interval and a
 * single retry and is *not* cancelled: it must still wait for its interval before deploying the
 * next attempt and must still consume its retry budget. Both halves are needed: waking a backoff
 * must not silently turn every retry into an immediate redeploy.
 *
 * <p>Kept as its own class rather than folded into an existing IT because the assertion needs a
 * dedicated fixture with an unusual (one minute) retry interval and a deterministic always-failing
 * sink, and because the in-JVM master/worker cluster here is what makes the pipeline state, restore
 * counter and retry deployment directly observable.
 */
@Slf4j
public class CancelDuringPipelineRetryBackoffIT {

    private static final String CONF_FILE = "batch_retry_backoff_failing_sink.conf";

    /** Same failing pipeline with a {@code job.retry.times = 1} / 3s retry control setup. */
    private static final String SHORT_RETRY_CONF_FILE =
            "batch_retry_backoff_short_interval_failing_sink.conf";

    /** Must match {@code job.retry.interval.seconds} of {@link #CONF_FILE}. */
    private static final long RETRY_INTERVAL_SECONDS = 60L;

    /**
     * How long the cancellation may take. Half the retry interval: a correct cancellation is a
     * couple of seconds, while the unfixed engine cannot finish before the whole interval expires.
     */
    private static final long CANCEL_DEADLINE_SECONDS = RETRY_INTERVAL_SECONDS / 2;

    private static final long CLUSTER_TIMEOUT_SECONDS = 60L;
    private static final long BACKOFF_TIMEOUT_SECONDS = 180L;

    /** Must match {@code job.retry.interval.seconds} of {@link #SHORT_RETRY_CONF_FILE}. */
    private static final long SHORT_RETRY_INTERVAL_SECONDS = 3L;

    @Test
    public void testCancelDuringRestoreBackoffSkipsTheRestore() throws Exception {
        RetryBackoffFailingSinkWriter.reset();
        String testClusterName = "CancelDuringPipelineRetryBackoffIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The HTTP endpoint is irrelevant here and would only contend for fixed ports with other
        // test classes running in the same CI job.
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);
            HazelcastInstanceImpl[] nodes = new HazelcastInstanceImpl[] {node1, node2};

            Common.setDeployMode(DeployMode.CLIENT);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("cancel-during-restore-backoff");
            ClientJobProxy jobProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CONF_FILE), jobConfig, clusterConfig)
                            .execute();
            long jobId = jobProxy.getJobId();
            log.warn("Submitted failing job {} for the retry backoff boundary", jobId);

            // Attempt 1 must have failed and the pipeline must be inside the retry backoff. The
            // pipeline is reset to CREATED right before the backoff sleep, so the restore counter
            // and this status together identify the window unambiguously.
            SubPlan pipeline = awaitPipelineInRetryBackoff(nodes, jobId);
            int writersBeforeCancel = RetryBackoffFailingSinkWriter.OPENED.get();
            Assertions.assertTrue(
                    writersBeforeCancel >= 1,
                    "The first attempt must have deployed its sink writer before the backoff"
                            + " starts, but none was created");
            Assertions.assertEquals(
                    PipelineStatus.CREATED,
                    pipeline.getCurrPipelineStatus(),
                    "The pipeline must be reset to CREATED while it waits for its restore");

            long cancelStartedNanos = System.nanoTime();
            jobProxy.cancelJob();
            awaitJobStatus(jobProxy, JobStatus.CANCELED);
            long cancelMillis =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cancelStartedNanos);
            log.warn("Job {} reached CANCELED {} ms after the cancel request", jobId, cancelMillis);

            Assertions.assertTrue(
                    cancelMillis < TimeUnit.SECONDS.toMillis(CANCEL_DEADLINE_SECONDS),
                    "Cancelling a job during its "
                            + RETRY_INTERVAL_SECONDS
                            + "s restore backoff must not wait for the backoff to expire, but it"
                            + " took "
                            + cancelMillis
                            + " ms");

            // No zombie attempt may be deployed for a job that is already being cancelled. Watch
            // for a short while so a late deployment cannot slip in after the terminal state.
            Awaitility.await()
                    .during(2, TimeUnit.SECONDS)
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            writersBeforeCancel,
                                            RetryBackoffFailingSinkWriter.OPENED.get(),
                                            "The cancelled job must not deploy another attempt"));

            // The terminal state must stay stable and the pipeline must end CANCELED, not FAILED.
            Assertions.assertEquals(JobStatus.CANCELED, jobProxy.getJobStatus());
            Assertions.assertEquals(
                    PipelineStatus.CANCELED,
                    pipeline.getCurrPipelineStatus(),
                    "The cancelled pipeline must end CANCELED instead of being restored");
            Assertions.assertEquals(
                    1,
                    pipeline.getPipelineRestoreNum(),
                    "Cancelling during the backoff must not start a further restore attempt");

            // Control: a retry that is not cancelled must still wait for the configured interval
            // and use up the configured retry budget. This pins that the wake-up above did not
            // turn the backoff into a no-op for everyone else.
            JobConfig controlJobConfig = new JobConfig();
            controlJobConfig.setName("retry-backoff-control");
            ClientJobProxy controlJob =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(SHORT_RETRY_CONF_FILE),
                                    controlJobConfig,
                                    clusterConfig)
                            .execute();
            long controlJobId = controlJob.getJobId();
            SubPlan controlPipeline = awaitPipelineInRetryBackoff(nodes, controlJobId);
            int writersBeforeRetry = RetryBackoffFailingSinkWriter.OPENED.get();
            long backoffObservedNanos = System.nanoTime();
            Awaitility.await()
                    .atMost(BACKOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            RetryBackoffFailingSinkWriter.OPENED.get()
                                                    > writersBeforeRetry,
                                            "The uncancelled job must deploy its second attempt"));
            long retryDelayMillis =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - backoffObservedNanos);
            Assertions.assertTrue(
                    retryDelayMillis >= TimeUnit.SECONDS.toMillis(2),
                    "An uncancelled retry must still wait for its "
                            + SHORT_RETRY_INTERVAL_SECONDS
                            + "s backoff, but the next attempt started after "
                            + retryDelayMillis
                            + " ms");
            awaitJobStatus(controlJob, JobStatus.FAILED);
            Assertions.assertEquals(
                    1,
                    controlPipeline.getPipelineRestoreNum(),
                    "The control job must consume its single configured retry");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node2 != null) {
                node2.shutdown();
            }
            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    private static SubPlan awaitPipelineInRetryBackoff(HazelcastInstanceImpl[] nodes, long jobId) {
        AtomicReference<SubPlan> backoffPipeline = new AtomicReference<>();
        Awaitility.await()
                .atMost(BACKOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SubPlan pipeline = findPipeline(nodes, jobId);
                            Assertions.assertNotNull(
                                    pipeline, "Job " + jobId + " has no observable pipeline yet");
                            Assertions.assertEquals(
                                    1,
                                    pipeline.getPipelineRestoreNum(),
                                    "Waiting for attempt 1 of job "
                                            + jobId
                                            + " to fail and enter its retry backoff, current status "
                                            + pipeline.getCurrPipelineStatus());
                            Assertions.assertEquals(
                                    PipelineStatus.CREATED,
                                    pipeline.getCurrPipelineStatus(),
                                    "Waiting for job " + jobId + " to sleep in its retry backoff");
                            backoffPipeline.set(pipeline);
                        });
        return backoffPipeline.get();
    }

    private static SubPlan findPipeline(HazelcastInstanceImpl[] nodes, long jobId) {
        for (HazelcastInstanceImpl node : nodes) {
            JobMaster jobMaster = findJobMaster(node, jobId);
            if (jobMaster == null) {
                continue;
            }
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            if (!pipelines.isEmpty()) {
                return pipelines.get(0);
            }
        }
        return null;
    }

    private static JobMaster findJobMaster(HazelcastInstanceImpl node, long jobId) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        if (server == null) {
            return null;
        }
        CoordinatorService coordinatorService = server.getCoordinatorService();
        if (coordinatorService == null) {
            return null;
        }
        return coordinatorService.getJobMaster(jobId);
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(CANCEL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job "
                                                + jobProxy.getJobId()
                                                + " to reach "
                                                + expectedStatus));
    }
}
