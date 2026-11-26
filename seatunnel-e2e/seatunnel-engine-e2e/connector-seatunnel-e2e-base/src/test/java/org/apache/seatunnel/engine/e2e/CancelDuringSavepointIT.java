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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.e2e.savepointcancel.SavepointCancelGate;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end regression coverage for cancelling a Zeta job while a savepoint checkpoint is still
 * being snapshotted.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A savepoint is the only job-level
 * operation that moves a running job out of {@link JobStatus#RUNNING} without ending it: {@code
 * JobMaster#savePoint()} first drives the plan to {@link JobStatus#DOING_SAVEPOINT}, then blocks on
 * every pipeline's checkpoint coordinator. A cancel that arrives inside that window has to resolve
 * two independent state machines that both own the job state: {@code PhysicalPlan#cancelJob()}
 * moves the plan to {@code CANCELING} and cancels the very checkpoint coordinator the savepoint
 * thread is waiting on, while {@code JobMaster#savePoint()}'s failure path is allowed to move the
 * plan back to {@code RUNNING} (start-precondition failure) or into a savepoint-failure stop. Every
 * hop of that chain is silent when it regresses: a cancel that loses the race lets the job end
 * {@code SAVEPOINT_DONE}/{@code FINISHED} although the client cancelled it; a savepoint failure
 * path that overwrites {@code CANCELING} resurrects a job the user already cancelled; a cancel that
 * waits for the held snapshot ack hangs the client's cancel request; a savepoint whose aborted
 * checkpoint is still recorded leaves a recovery point that does not exist. None of these failures
 * surfaces as a task error, so only an end-to-end observation of the job status plus the checkpoint
 * history can catch them.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointPreconditionRecoveryIT}
 * covers a savepoint whose start preconditions are rejected and the job stays {@code RUNNING};
 * {@code SavepointBusySourceBarrierIT} covers a busy source that delays the barrier but no cancel;
 * {@code SavepointRestoreIT} and {@code CheckpointRestoreWithStopIT} only exercise the happy path
 * (savepoint completes, then job is restored); {@code PendingJobsRestIT} and {@code
 * SplitClusterPendingJobLifecycleFailoverIT} cover pending-job cancellation before any task runs.
 * No test cancels a job whose savepoint barrier is already deployed and unacknowledged, and no test
 * asserts that a late savepoint request against the resulting terminal job cannot reopen it.
 *
 * <p><b>How this test pins the window.</b> {@link
 * org.apache.seatunnel.engine.e2e.savepointcancel.SavepointCancelHoldSink} consumes every row but
 * parks the acknowledgement of its first checkpoint snapshot in {@link SavepointCancelGate}. The
 * job config uses {@code checkpoint.interval = 600000}, so the first snapshot of the job is the
 * savepoint the test requests, and {@code checkpoint.timeout = 120000}, so the parked savepoint
 * cannot expire while the test drives the cancel. The test only cancels after the master's
 * checkpoint overview reports that exact checkpoint id as an in-progress {@code SAVEPOINT_TYPE}
 * checkpoint and the job is already {@code DOING_SAVEPOINT}; it releases the gate only after the
 * cancel request has been acknowledged and the job has left {@code DOING_SAVEPOINT}. It then
 * requires the job to end {@code CANCELED} (never {@code SAVEPOINT_DONE}/{@code FINISHED}), the
 * in-flight savepoint request to fail instead of reporting success, no completed checkpoint for the
 * aborted id, no restore attempt, and a late savepoint request on the terminal job to fail fast
 * without reopening it. A second job submitted on the same cluster must still reach {@code RUNNING}
 * and cancel cleanly. That only proves continued scheduling, not slot or coordinator cleanup: the
 * fixture uses dynamic slots, so a leaked allocation could coexist with the follow-up job.
 */
@Slf4j
public class CancelDuringSavepointIT {

    /** Name of the test resource template rendered into the actual job config file. */
    private static final String TEST_TEMPLATE_FILE_NAME =
            "stream_fake_to_savepoint_cancel_hold_sink_template.conf";

    /** Placeholder key in the template that is substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Placeholder key in the template that is substituted with the held snapshot number. */
    private static final String DYNAMIC_BLOCK_ON_SNAPSHOT = "dynamic_block_on_snapshot";

    /** The savepoint is the first checkpoint of the job, so the first snapshot is held. */
    private static final int BLOCK_ON_SAVEPOINT_SNAPSHOT = 1;

    /** Used by the follow-up job, which must never park because no gate is armed for it. */
    private static final int NEVER_BLOCK_SNAPSHOT = 99;

    /** Upper bound to wait for the master/worker cluster to fully form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the submitted job to reach RUNNING. */
    private static final long JOB_RUNNING_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the savepoint request to move the job into DOING_SAVEPOINT. */
    private static final long SAVEPOINT_START_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the sink to park in the savepoint snapshot. */
    private static final long HOLD_ENTRY_TIMEOUT_SECONDS = 60L;

    /**
     * Upper bound to wait for the master to report the held checkpoint as an in-progress savepoint.
     */
    private static final long PENDING_SAVEPOINT_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the job to leave DOING_SAVEPOINT after the cancel request. */
    private static final long CANCEL_TRANSITION_TIMEOUT_SECONDS = 30L;

    /**
     * Upper bound for the cancel RPC acknowledgement while the savepoint barrier is still held. The
     * assertion under test is that the cancel does not wait for the parked snapshot, so this is
     * checked here instead of relying on an engine or workflow timeout.
     */
    private static final long CANCEL_ACK_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the cancelled job to reach its terminal state. */
    private static final long CANCELED_TERMINAL_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the in-flight savepoint request to resolve. */
    private static final long SAVEPOINT_RESULT_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for a late savepoint request on the terminal job to be rejected. */
    private static final long LATE_SAVEPOINT_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the follow-up job to reach RUNNING on the same cluster. */
    private static final long FOLLOW_UP_JOB_RUNNING_TIMEOUT_SECONDS = 60L;

    @Test
    public void testCancelDuringInFlightSavepoint() throws Exception {
        String testCaseName = "testCancelDuringInFlightSavepoint";
        String testClusterName = "CancelDuringSavepointIT_" + testCaseName;
        String holdKey = testCaseName + "_" + System.nanoTime();

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ExecutorService savepointExecutor = Executors.newSingleThreadExecutor();
        SavepointCancelGate.arm(holdKey);
        try {
            // Every instance needs its own config: the starter mutates the cluster role and the
            // lite-member flag of the config it is given, so sharing one instance would let the
            // worker role leak into the already running master.
            masterNode =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));
            // A member can join before its slot profiles are visible to the master's resource
            // manager. The default REJECT schedule strategy would then fail the submitted job
            // before it ever runs, which has nothing to do with the savepoint/cancel window
            // under test.
            awaitWorkerRegistered(finalMasterNode);

            Common.setDeployMode(DeployMode.CLUSTER);
            String confPath =
                    createTestResources(testCaseName, holdKey, BLOCK_ON_SAVEPOINT_SNAPSHOT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            confPath, jobConfig, getSeaTunnelConfig(testClusterName));
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            HazelcastInstanceImpl finalMaster = masterNode;
            JobMaster jobMaster = awaitJobMaster(finalMaster, jobId);
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(1, pipelines.size(), "The test job must have one pipeline");
            SubPlan pipeline = pipelines.get(0);
            int pipelineId = pipeline.getPipelineId();

            // A savepoint is requested but its barrier acknowledgement stays parked in the sink.
            AtomicReference<Throwable> savepointFailure = new AtomicReference<>();
            SeaTunnelClient finalEngineClient = engineClient;
            CompletableFuture<Void> savepointRequest =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    finalEngineClient.savePointJob(jobId);
                                } catch (Throwable t) {
                                    savepointFailure.set(t);
                                    throw new RuntimeException(t);
                                }
                            },
                            savepointExecutor);

            Awaitility.await()
                    .atMost(SAVEPOINT_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.DOING_SAVEPOINT,
                                            clientJobProxy.getJobStatus(),
                                            "The savepoint request did not move the job into"
                                                    + " DOING_SAVEPOINT"));

            SavepointCancelGate.awaitHeld(holdKey, HOLD_ENTRY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long heldCheckpointId = SavepointCancelGate.heldCheckpointId(holdKey);
            Assertions.assertTrue(
                    heldCheckpointId > 0, "The held snapshot must belong to a real checkpoint");
            Assertions.assertEquals(
                    BLOCK_ON_SAVEPOINT_SNAPSHOT,
                    SavepointCancelGate.snapshotCount(holdKey),
                    "The sink must hold exactly the configured snapshot");
            Assertions.assertFalse(
                    savepointRequest.isDone(),
                    "The savepoint must still be in flight while its barrier is unacknowledged");

            // The master must see exactly the parked checkpoint as an in-progress savepoint.
            Awaitility.await()
                    .atMost(PENDING_SAVEPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview overview =
                                        pipelineOverview(finalMaster, jobId, pipelineId);
                                Assertions.assertNotNull(
                                        overview,
                                        "The checkpoint overview is not available while the"
                                                + " savepoint is parked");
                                InProgressCheckpoint pending =
                                        overview.getInProgress().stream()
                                                .filter(
                                                        checkpoint ->
                                                                checkpoint.getCheckpointId()
                                                                        == heldCheckpointId)
                                                .findFirst()
                                                .orElse(null);
                                Assertions.assertNotNull(
                                        pending,
                                        "The held checkpoint is not reported as in progress");
                                Assertions.assertEquals(
                                        CheckpointType.SAVEPOINT_TYPE,
                                        pending.getCheckpointType(),
                                        "The held checkpoint must be the requested savepoint");
                                Assertions.assertTrue(
                                        overview.getLatestCompleted() == null
                                                || overview.getLatestCompleted().getCheckpointId()
                                                        != heldCheckpointId,
                                        "The held savepoint must not be recorded as completed"
                                                + " while the sink is parked");
                            });

            // Cancel while the savepoint barrier is still unacknowledged. The cancel request must
            // win: the job has to leave DOING_SAVEPOINT immediately and must never be reported as
            // SAVEPOINT_DONE/FINISHED afterwards. Run the call on a test-owned executor so a
            // regression that makes the acknowledgement wait for the parked snapshot fails through
            // this bounded get() instead of consuming the workflow timeout.
            awaitCancelAcknowledged(clientJobProxy);
            Awaitility.await()
                    .atMost(CANCEL_TRANSITION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                Assertions.assertTrue(
                                        status == JobStatus.CANCELING
                                                || status == JobStatus.CANCELED,
                                        "A cancel must move the job out of DOING_SAVEPOINT, but the"
                                                + " status is "
                                                + status);
                            });

            // Only now let the parked snapshot finish. Its acknowledgement must not complete the
            // aborted savepoint nor change the terminal state.
            SavepointCancelGate.release(holdKey);

            Awaitility.await()
                    .atMost(CANCELED_TERMINAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            clientJobProxy.getJobStatus(),
                                            "A cancelled job must end CANCELED"));

            // The savepoint request must resolve as a failure, never as a successful savepoint.
            try {
                savepointRequest.get(SAVEPOINT_RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                Assertions.fail(
                        "The in-flight savepoint request completed normally although the job was"
                                + " cancelled before the barrier was acknowledged");
            } catch (ExecutionException expected) {
                Assertions.assertNotNull(
                        expected.getCause(),
                        "The failed savepoint request must carry the engine failure");
                Assertions.assertNotNull(
                        savepointFailure.get(),
                        "The failed savepoint request must record its failure");
                log.warn(
                        "In-flight savepoint for job {} failed as expected after cancel: {}",
                        jobId,
                        savepointFailure.get().toString());
            } catch (TimeoutException e) {
                Assertions.fail(
                        "The in-flight savepoint request did not resolve within "
                                + SAVEPOINT_RESULT_TIMEOUT_SECONDS
                                + " s after the cancel");
            }

            // Cancelling must not turn the aborted savepoint into a recovery point, and must not
            // consume a restore attempt.
            PipelineCheckpointOverview overviewAfterCancel =
                    pipelineOverview(finalMaster, jobId, pipelineId);
            if (overviewAfterCancel != null) {
                Assertions.assertTrue(
                        overviewAfterCancel.getInProgress().stream()
                                .noneMatch(
                                        checkpoint ->
                                                checkpoint.getCheckpointId() == heldCheckpointId),
                        "The aborted savepoint must not stay in progress after the cancel");
                Assertions.assertTrue(
                        overviewAfterCancel.getLatestCompleted() == null
                                || overviewAfterCancel.getLatestCompleted().getCheckpointId()
                                        != heldCheckpointId,
                        "The aborted savepoint must never be reported as completed");
                Assertions.assertTrue(
                        overviewAfterCancel.getHistory().stream()
                                .filter(
                                        entry ->
                                                entry.getCheckpointInfo().getCheckpointId()
                                                        == heldCheckpointId)
                                .allMatch(
                                        entry ->
                                                !CheckpointStatus.COMPLETED.equals(
                                                        entry.getCheckpointInfo().getStatus())),
                        "The aborted savepoint must not be recorded as a completed checkpoint");
            }
            Assertions.assertEquals(
                    0,
                    pipeline.getPipelineRestoreNum(),
                    "Cancelling a job during a savepoint must not consume a restore attempt");

            // The terminal state must be stable, and a late savepoint request must fail fast
            // instead of reopening the job.
            for (int i = 0; i < 10; i++) {
                Assertions.assertEquals(
                        JobStatus.CANCELED,
                        clientJobProxy.getJobStatus(),
                        "The terminal state must not be replaced by a late savepoint");
                Thread.sleep(200L);
            }
            assertLateSavepointRejected(engineClient, jobId, testClusterName);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    clientJobProxy.getJobStatus(),
                    "A rejected late savepoint must not change the terminal state");

            // The follow-up job only proves that the cluster still schedules new jobs after the
            // cancelled savepoint. It does not prove that the first job's slots are gone: the test
            // config uses dynamic slots, so a leaked allocation could coexist with this job.
            String followUpCaseName = testCaseName + "FollowUp";
            String followUpConfPath =
                    createTestResources(
                            followUpCaseName, holdKey + "_followup", NEVER_BLOCK_SNAPSHOT);
            JobConfig followUpJobConfig = new JobConfig();
            followUpJobConfig.setName(followUpCaseName);
            ClientJobProxy followUpProxy =
                    engineClient
                            .createExecutionContext(
                                    followUpConfPath,
                                    followUpJobConfig,
                                    getSeaTunnelConfig(testClusterName))
                            .execute();
            Awaitility.await()
                    .atMost(FOLLOW_UP_JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING,
                                            followUpProxy.getJobStatus(),
                                            "A follow-up job must still be schedulable after the"
                                                    + " cancelled savepoint"));
            followUpProxy.cancelJob();
            Awaitility.await()
                    .atMost(CANCELED_TERMINAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            followUpProxy.getJobStatus(),
                                            "The follow-up job must cancel cleanly"));

            log.warn(
                    "Job {} was cancelled while savepoint checkpoint {} was in flight; the job"
                            + " ended CANCELED, the savepoint request failed, and the cluster"
                            + " scheduled job {} afterwards",
                    jobId,
                    heldCheckpointId,
                    followUpProxy.getJobId());
        } finally {
            // Release the parked snapshot first: a negative-path failure can leave the scripted
            // writer inside snapshotState(), and only the gate release lets it leave. Every later
            // step runs from a nested finally so an earlier teardown failure cannot skip it.
            SavepointCancelGate.release(holdKey);
            try {
                savepointExecutor.shutdownNow();
            } finally {
                try {
                    if (engineClient != null) {
                        engineClient.close();
                    }
                } finally {
                    try {
                        if (workerNode != null) {
                            workerNode.shutdown();
                        }
                    } finally {
                        if (masterNode != null) {
                            masterNode.shutdown();
                        }
                    }
                }
            }
        }
    }

    /**
     * Calls {@link ClientJobProxy#cancelJob()} on a test-owned daemon executor and requires the
     * acknowledgement within a bounded time.
     *
     * <p>The client proxy joins its remote future without a test-controlled timeout. Waiting for it
     * here, while the savepoint gate is still held, is what turns "a cancel must not wait for the
     * parked snapshot" into an assertion instead of a workflow timeout.
     *
     * @param clientJobProxy job whose cancellation is requested
     * @throws Exception when the cancel fails, or a bounded assertion when it does not acknowledge
     */
    private static void awaitCancelAcknowledged(ClientJobProxy clientJobProxy) throws Exception {
        ExecutorService cancelExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "cancel-during-savepoint-cancel");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> cancel = cancelExecutor.submit(clientJobProxy::cancelJob);
            try {
                cancel.get(CANCEL_ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException timeout) {
                cancel.cancel(true);
                throw new AssertionError(
                        "The cancel request for job "
                                + clientJobProxy.getJobId()
                                + " did not acknowledge within "
                                + CANCEL_ACK_TIMEOUT_SECONDS
                                + " s while the savepoint barrier was still held",
                        timeout);
            }
        } finally {
            // The remote join is not interrupted by Future.cancel alone, so the executor is
            // daemon-backed and abandoned after this bounded wait.
            cancelExecutor.shutdownNow();
        }
    }

    /**
     * A savepoint request against a job that already reached its terminal state must be rejected
     * with an engine failure, within a bounded time, and without changing the job's state.
     */
    private static void assertLateSavepointRejected(
            SeaTunnelClient engineClient, long jobId, String testClusterName) throws Exception {
        CompletableFuture<Void> lateRequest =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        try {
            lateRequest.get(LATE_SAVEPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.fail(
                    "A savepoint request on a CANCELED job must fail, but it completed normally");
        } catch (ExecutionException expected) {
            Assertions.assertNotNull(
                    expected.getCause(),
                    "The rejected savepoint request must carry the engine failure");
            log.warn(
                    "Late savepoint for job {} in cluster {} was rejected as expected: {}",
                    jobId,
                    testClusterName,
                    expected.getCause().toString());
        } catch (TimeoutException e) {
            Assertions.fail(
                    "A savepoint request on a CANCELED job did not fail within "
                            + LATE_SAVEPOINT_TIMEOUT_SECONDS
                            + " s");
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

    /**
     * Waits until the master's resource manager has registered at least one worker with slots.
     *
     * @param master the active master member
     */
    private static void awaitWorkerRegistered(HazelcastInstanceImpl master) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SeaTunnelServer server =
                                    master.node
                                            .getNodeEngine()
                                            .getService(SeaTunnelServer.SERVICE_NAME);
                            Assertions.assertNotNull(server, "The master server is not available");
                            Assertions.assertNotNull(
                                    server.getCoordinatorService(),
                                    "The coordinator service is not active yet");
                            ResourceManager resourceManager =
                                    server.getCoordinatorService().getInitializedResourceManager();
                            Assertions.assertNotNull(
                                    resourceManager, "The resource manager is not initialized yet");
                            Assertions.assertTrue(
                                    resourceManager.workerCount(java.util.Collections.emptyMap())
                                            > 0,
                                    "The worker has not registered its slots yet");
                        });
    }

    private static JobMaster awaitJobMaster(HazelcastInstanceImpl master, long jobId) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> getJobMaster(master, jobId) != null);
        return getJobMaster(master, jobId);
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl master, long jobId) {
        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService() == null
                ? null
                : server.getCoordinatorService().getJobMaster(jobId);
    }

    private static PipelineCheckpointOverview pipelineOverview(
            HazelcastInstanceImpl master, long jobId, int pipelineId) {
        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        CheckpointOverview overview =
                server.getEngineContext()
                        .getStateStores()
                        .auxiliary()
                        .checkpointOverviewStateStore()
                        .get(jobId);
        return overview == null ? null : overview.getPipelines().get(pipelineId);
    }

    private static String createTestResources(
            String testCaseName, String holdKey, int blockOnSnapshot) throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        valueMap.put(DYNAMIC_BLOCK_ON_SNAPSHOT, String.valueOf(blockOnSnapshot));

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                TEST_TEMPLATE_FILE_NAME, valueMap, targetConfigFilePath);
        return targetConfigFilePath;
    }
}
