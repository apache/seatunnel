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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Restores thirty streaming jobs after a master failover and verifies that lifecycle work
 * progresses independently of admission. Lifecycle workers remain unbounded while nested blocking
 * callbacks exist; the admission pool must remain idle on the promoted standby.
 */
@Slf4j
public class CoordinatorExecutorMassFailoverStormIT {

    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";

    // Large enough to exercise restore fan-out while fitting in one CI test JVM.
    private static final int CONCURRENT_JOB_COUNT = 30;

    private static final int POST_RESTORE_STABILITY_SECONDS = 5;

    private static final int TEARDOWN_TIMEOUT_SECONDS = 150;

    /**
     * Pure CI-runner safety backstop, not a claim that today's design bounds growth in general --
     * it does not, since the lifecycle executor has an unbounded maximum. This only guards against
     * a pathological regression far beyond what {@link #CONCURRENT_JOB_COUNT} concurrent restores
     * could plausibly need, so a future regression fails this test loudly instead of quietly
     * spawning an unbounded number of threads on a shared runner.
     */
    private static final int POOL_SIZE_CI_SAFETY_CEILING = 500;

    @Test
    public void testMassFailoverRestoreDoesNotOccupyAdmissionWorkers() throws Exception {
        String testClusterName =
                "CoordinatorExecutorMassFailoverStormIT_"
                        + "testMassFailoverRestoreDoesNotOccupyAdmissionWorkers";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        List<Long> jobIds = new ArrayList<>();

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try {
            // Split-role cluster (master-only + master-only + worker-only), mirroring
            // SplitClusterPendingJobLifecycleFailoverIT: this keeps the standby master's
            // CoordinatorService executor free of any task-execution activity of its own, so
            // every thread we observe on it after promotion is attributable to coordination work
            // (state restore, scheduling), not to running one of the CONCURRENT_JOB_COUNT jobs'
            // actual tasks.
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode1 = masterNode1;
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            for (int i = 0; i < CONCURRENT_JOB_COUNT; i++) {
                jobIds.add(submitJob(engineClient, masterNode1Config, "mass_retry_job_" + i));
            }

            // Submission only means the active master accepted and scheduled a job; wait until
            // every one of them has genuinely reached RUNNING, so all CONCURRENT_JOB_COUNT jobs
            // are guaranteed to already be replicated into runningJobInfoIMap -- and therefore
            // genuinely in need of restoring -- by the time the active master is killed below.
            awaitAllJobsInStatus(engineClient, jobIds, JobStatus.RUNNING, 180);

            // Trigger the mass-simultaneous-restore condition: kill the active master while all
            // CONCURRENT_JOB_COUNT jobs are RUNNING and their worker is still healthy. Master
            // failover while jobs are live is the same realistic trigger already proven throughout
            // this test family; what is new here is the concurrent job count and what gets sampled
            // on the new master immediately afterward.
            activeMaster.shutdown();

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertTrue(
                                        isCoordinatorActive(standbyMaster),
                                        "Standby master should become active after failover");
                            });

            CoordinatorService standbyCoordinatorService = getCoordinatorService(standbyMaster);
            awaitAllJobsStableInStatus(engineClient, jobIds, JobStatus.RUNNING, 180);
            ThreadPoolStatus lifecycle =
                    standbyCoordinatorService.getLifecycleThreadPoolStatusMetrics();
            ThreadPoolStatus admission = standbyCoordinatorService.getThreadPoolStatusMetrics();
            int lifecyclePoolSize = lifecycle.getPoolSize();
            Assertions.assertTrue(
                    lifecycle.getActiveCount() >= CONCURRENT_JOB_COUNT,
                    "Each restored job should have a lifecycle worker waiting for completion");
            Assertions.assertEquals(
                    0, admission.getActiveCount(), "Restore must not occupy admission workers");
            Assertions.assertEquals(
                    0,
                    admission.getTaskCount(),
                    "Restore must not submit work to the standby admission pool");
            Assertions.assertEquals(
                    0, lifecycle.getRejectionCount(), "Lifecycle restore must not reject tasks");
            Assertions.assertEquals(
                    0, admission.getRejectionCount(), "Restore must not cause admission rejection");

            // Pure CI-runner safety backstop -- see the constant's Javadoc above.
            Assertions.assertTrue(
                    lifecyclePoolSize < POOL_SIZE_CI_SAFETY_CEILING,
                    String.format(
                            "Observed lifecycle pool size %d is far beyond what %d "
                                    + "concurrent job restores should plausibly need; capping here "
                                    + "so a regression fails loudly instead of destabilizing the CI "
                                    + "runner",
                            lifecyclePoolSize, CONCURRENT_JOB_COUNT));

            // Clean up every restored job before the test returns.
            cancelAllAndAwaitTerminal(engineClient, jobIds);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode1 != null && masterNode1.getLifecycleService().isRunning()) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null && masterNode2.getLifecycleService().isRunning()) {
                masterNode2.shutdown();
            }
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static long submitJob(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(
                        TestUtils.getResource(JOB_CONFIG_FILE), jobConfig, seaTunnelConfig);
        try {
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            return clientJobProxy.getJobId();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job " + jobName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted when submitting job " + jobName, e);
        }
    }

    /**
     * Polls every job's current status by id until all of them report {@code expectedStatus}.
     * Re-fetching the proxy by id on every poll (rather than reusing a {@link ClientJobProxy}
     * obtained before the master failover) mirrors this test family's established post-failover
     * access pattern, since a proxy obtained against the old active master is not guaranteed to
     * keep working correctly against the new one.
     */
    private static void awaitAllJobsInStatus(
            SeaTunnelClient engineClient,
            List<Long> jobIds,
            JobStatus expectedStatus,
            long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (Long jobId : jobIds) {
                                JobStatus actual =
                                        engineClient
                                                .createJobClient()
                                                .getJobProxy(jobId)
                                                .getJobStatus();
                                Assertions.assertEquals(
                                        expectedStatus,
                                        actual,
                                        "Job " + jobId + " should be " + expectedStatus);
                            }
                        });
    }

    /**
     * Requires the restored jobs to stay running for a short settle window. A one-off RUNNING
     * sample can race a pipeline re-deploy; cancelling during that transition can leave a task in a
     * worker-side invocation wait and make teardown depend on its heartbeat timeout.
     */
    private static void awaitAllJobsStableInStatus(
            SeaTunnelClient engineClient,
            List<Long> jobIds,
            JobStatus expectedStatus,
            long timeoutSeconds) {
        Awaitility.await()
                .during(POST_RESTORE_STABILITY_SECONDS, TimeUnit.SECONDS)
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (Long jobId : jobIds) {
                                JobStatus actual =
                                        engineClient
                                                .createJobClient()
                                                .getJobProxy(jobId)
                                                .getJobStatus();
                                Assertions.assertEquals(
                                        expectedStatus,
                                        actual,
                                        "Job "
                                                + jobId
                                                + " should remain stable in "
                                                + expectedStatus);
                            }
                        });
    }

    /**
     * Cancels every job and waits for each one's own terminal result.
     *
     * <p>The cancel request and the wait for that same job's completion future are issued
     * back-to-back, per job, while it is still known to be tracked by the coordinator (it was just
     * observed RUNNING by the caller). {@link ClientJobProxy#doWaitForJobComplete()} resolves off
     * that job's own {@code JobMaster#jobMasterCompleteFuture} on the server -- server-side {@code
     * CoordinatorService#waitForJobComplete(long)} looks the job up the same way {@code
     * cancelJob}/{@code getJobMaster} do (pending queue, then the running-job map) and, as long as
     * the {@code JobMaster} instance is found either way, hands back that instance's own
     * event-driven completion future instead of re-deriving a status from separate tracking
     * structures. This is the same primitive {@code
     * SplitClusterPendingJobLifecycleFailoverIT#assertEventuallyCanceled} already relies on for the
     * identical "cancel right after a master failover" scenario, per its own Javadoc: cancellation
     * can leave the client-observed status at CANCELING after the cancel request has already
     * completed, so the terminal *result* must be awaited rather than sampled.
     *
     * <p>A raw repeated {@code getJobStatus()} poll, by contrast, takes a fresh point-in-time
     * snapshot of the pending queue / running-job map / finished-job history on every attempt, and
     * can observe the {@code UNKNOWABLE} sentinel (job absent from all three) in the narrow window
     * before those structures settle under this test's mass-cancel load -- even though the job's
     * own completion future already carries the real terminal result throughout that window.
     */
    private static void cancelAllAndAwaitTerminal(SeaTunnelClient engineClient, List<Long> jobIds) {
        Map<Long, PassiveCompletableFuture<JobResult>> completeFutures = new LinkedHashMap<>();
        for (Long jobId : jobIds) {
            ClientJobProxy proxy = engineClient.createJobClient().getJobProxy(jobId);
            try {
                proxy.cancelJob();
            } catch (Exception e) {
                log.warn("Failed to send cancel for job {} during teardown", jobId, e);
            }
            completeFutures.put(jobId, proxy.doWaitForJobComplete());
        }

        Map<JobStatus, Integer> terminalStatusCounts = new EnumMap<>(JobStatus.class);
        long teardownDeadlineNanos =
                System.nanoTime() + TimeUnit.SECONDS.toNanos(TEARDOWN_TIMEOUT_SECONDS);
        for (Map.Entry<Long, PassiveCompletableFuture<JobResult>> entry :
                completeFutures.entrySet()) {
            JobResult jobResult;
            try {
                long remainingNanos = teardownDeadlineNanos - System.nanoTime();
                Assertions.assertTrue(
                        remainingNanos > 0,
                        "Jobs did not reach terminal statuses within the shared teardown deadline");
                jobResult = entry.getValue().get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                Assertions.fail(
                        "Job "
                                + entry.getKey()
                                + " did not reach a terminal status within the shared teardown "
                                + "deadline",
                        e);
                return;
            }
            JobStatus actual = jobResult.getStatus();
            terminalStatusCounts.merge(actual, 1, Integer::sum);
            // Accept every end state here (JobStatus#isEndState(): FAILED, CANCELED, FINISHED,
            // SAVEPOINT_DONE or UNKNOWABLE), not only CANCELED/FINISHED: the executor-isolation
            // assertions above are the actual property under test, and all this teardown has to
            // prove is that no job is left non-terminal (nothing leaks past the test). A job
            // cancelled during this mass-failover storm can legitimately end FAILED instead of
            // CANCELED; the exact chain was confirmed against a real CI failure and this engine's
            // current source:
            //   1. This module's seatunnel.yaml sets checkpoint timeout=100000ms. Under the
            //      storm, a job's first checkpoint can go unacknowledged for that whole window, so
            //      its pre-scheduled expiry check (the scheduled task started inside
            //      CheckpointCoordinator#triggerCheckpoint, CheckpointCoordinator.java:986-996)
            //      fires, aborts the checkpoint as expired, and marks the CheckpointCoordinator
            //      FAILED -- which cancels every task in the pipeline.
            //   2. A task whose BlockingWorker thread was, at that same moment, mid-RPC
            //      (ReportMetricsOperation) to the master this test kills independently has its
            //      connection close immediately, but the pending Hazelcast invocation does not
            //      fail fast -- it only surfaces once the operation-heartbeat-timeout gives up on
            //      it, up to ~120s later (TaskExecutionService#updateMetricsContextInImap,
            //      TaskExecutionService.java:880 and 893-901). That task still completes CANCELED
            //      (it was already flagged for cancellation), just ~120s after its siblings.
            //   3. Only once every task in the pipeline has completed does
            //      SubPlan#getPipelineEndState() (SubPlan.java:250-259) make the final call: it
            //      finds them all CANCELED, but also re-checks the CheckpointCoordinator's own
            //      status: seeing FAILED from step 1, it overrides the pipeline's terminal state
            //      to FAILED, and the job then follows CANCELING -> FAILING -> FAILED.
            Assertions.assertTrue(
                    actual.isEndState(),
                    "Job "
                            + entry.getKey()
                            + " should have reached a terminal status, was "
                            + actual);
        }
        log.info(
                "CoordinatorExecutorMassFailoverStormIT teardown: {} jobs reached a terminal "
                        + "status, by status: {}",
                completeFutures.size(),
                terminalStatusCounts);
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        final HazelcastInstanceImpl[] activeMasterRef = new HazelcastInstanceImpl[1];
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            activeMasterRef[0] = findActiveMaster(masterNode1, masterNode2);
                            Assertions.assertNotNull(
                                    activeMasterRef[0],
                                    "Should find active master after coordinator initialization");
                        });
        return activeMasterRef[0];
    }

    private static HazelcastInstanceImpl findActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        if (isCoordinatorActive(masterNode1)) {
            return masterNode1;
        }
        if (isCoordinatorActive(masterNode2)) {
            return masterNode2;
        }
        return null;
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return getCoordinatorService(masterNode).isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }
}
