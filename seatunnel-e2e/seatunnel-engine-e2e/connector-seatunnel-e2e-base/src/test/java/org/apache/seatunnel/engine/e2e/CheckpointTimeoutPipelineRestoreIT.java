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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.e2e.checkpointtimeout.CheckpointTimeoutHoldGate;
import org.apache.seatunnel.engine.e2e.checkpointtimeout.CheckpointTimeoutHoldSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * End-to-end regression coverage for what a Zeta pipeline does when one of its checkpoints is not
 * acknowledged before {@code checkpoint.timeout}.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A checkpoint timeout is the only
 * checkpoint failure that a healthy job reports without any task failing: the sink is simply slow
 * to snapshot (a large commit, an object-store upload, a lock held by the source). {@code
 * CheckpointCoordinator} arms a timeout task for every pending checkpoint and, when it fires,
 * routes {@link CheckpointCloseReason#CHECKPOINT_EXPIRED} through {@code handleCoordinatorError},
 * which must fail only that checkpoint, mark the barrier aborted, and let the pipeline restore from
 * the last completed checkpoint. Every hop of that chain is silent when it regresses: a timeout
 * that is not armed leaves the pipeline RUNNING with a barrier that can never complete (no further
 * checkpoints, no error, throughput still non-zero for a while); a timeout that completes the
 * aborted checkpoint poisons recovery with state that was never taken; a restore that does not
 * revive the coordinator leaves the restored pipeline running without checkpoints; an id counter
 * that is not advanced reuses the aborted id and makes the recovery point ambiguous. None of those
 * failures surfaces as a task error, so only an end-to-end assertion on the checkpoint lifecycle
 * can catch them.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code CheckpointCoordinatorFailoverIT}
 * injects a barrier-dispatch failure and expects the job to FAIL; {@code
 * CheckpointRestoreWithStopIT} and {@code RecoveryLifecycleRestApiIT} recover from a checkpoint
 * that completed normally; {@code RestApiIT#testCheckpointOverviewAndHistoryApi} fabricates a
 * {@code CHECKPOINT_EXPIRED} entry by calling the monitor service directly and therefore never
 * arms, expires, or restores anything; {@code SavepointBusySourceBarrierIT} races a savepoint
 * against a busy source but never lets a periodic checkpoint expire. No test makes a real
 * checkpoint time out and then asserts that the job survives, restores from the last completed
 * checkpoint, and resumes checkpointing.
 *
 * <p><b>How this test pins the window.</b> {@link CheckpointTimeoutHoldSink} consumes every row but
 * parks the acknowledgement of one configurable checkpoint snapshot on {@link
 * CheckpointTimeoutHoldGate}, with {@code checkpoint.timeout = 2000} and {@code checkpoint.interval
 * = 1000} in the job config. The test releases the gate only after the master's checkpoint overview
 * records that exact checkpoint as failed with the {@code CHECKPOINT_EXPIRED} reason while the sink
 * is still parked, which separates "the timeout fired" from "the sink finished". It then requires
 * the job to stay RUNNING, the pipeline to book exactly one restore, a later checkpoint to complete
 * with a higher id, and the restored writer to receive the state of the last completed checkpoint
 * (not the aborted one).
 */
@Slf4j
public class CheckpointTimeoutPipelineRestoreIT {

    /** Name of the test resource template rendered into the actual job config file. */
    private static final String TEST_TEMPLATE_FILE_NAME =
            "stream_fake_to_checkpoint_timeout_hold_sink_template.conf";

    /** Placeholder key in the template that is substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Placeholder key in the template that is substituted with the held snapshot number. */
    private static final String DYNAMIC_BLOCK_ON_SNAPSHOT = "dynamic_block_on_snapshot";

    /**
     * Number of checkpoints that must complete before the held one, so the restore has a real
     * completed checkpoint to fall back to.
     */
    private static final int COMPLETED_CHECKPOINTS_BEFORE_HOLD = 3;

    /** One-based number of the snapshot whose acknowledgement is held. */
    private static final int BLOCK_ON_SNAPSHOT = COMPLETED_CHECKPOINTS_BEFORE_HOLD + 1;

    /** Upper bound to wait for the master/worker cluster to fully form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the submitted job to reach RUNNING. */
    private static final long JOB_RUNNING_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the checkpoints that must complete before the hold. */
    private static final long INITIAL_CHECKPOINTS_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the sink to park in the held snapshot. */
    private static final long HOLD_ENTRY_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the checkpoint timeout to be reported by the master. */
    private static final long TIMEOUT_OBSERVATION_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the pipeline restore and the first post-restore checkpoint. */
    private static final long RESTORE_TIMEOUT_SECONDS = 180L;

    /** Upper bound to wait for the restored pipeline to prove it still processes rows. */
    private static final long ROW_PROGRESS_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for a clean cancellation of the recovered job. */
    private static final long CANCEL_TIMEOUT_SECONDS = 60L;

    @Test
    public void testPipelineRestoresFromLastCompletedCheckpointAfterTimeout() throws Exception {
        String testCaseName = "testPipelineRestoresFromLastCompletedCheckpointAfterTimeout";
        String testClusterName = "CheckpointTimeoutPipelineRestoreIT_" + testCaseName;
        String holdKey = testCaseName + "_" + System.nanoTime();

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        CheckpointTimeoutHoldGate.arm(holdKey);
        try {
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

            Common.setDeployMode(DeployMode.CLUSTER);
            String confPath = createTestResources(testCaseName, holdKey);
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

            HazelcastInstanceImpl master = masterNode;
            JobMaster jobMaster = awaitJobMaster(master, jobId);
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(1, pipelines.size(), "The test job must have one pipeline");
            SubPlan pipeline = pipelines.get(0);
            int pipelineId = pipeline.getPipelineId();

            // A checkpoint timeout must be tolerated by the job: the pipeline restores, the job
            // stays RUNNING. Wait until there is a completed checkpoint to fall back to before the
            // sink starts holding barriers.
            Awaitility.await()
                    .atMost(INITIAL_CHECKPOINTS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview overview =
                                        pipelineOverview(master, jobId, pipelineId);
                                Assertions.assertNotNull(
                                        overview, "The checkpoint overview is not available yet");
                                Assertions.assertTrue(
                                        overview.getCounts().getCompleted()
                                                >= COMPLETED_CHECKPOINTS_BEFORE_HOLD,
                                        "Waiting for the checkpoints that must complete before the"
                                                + " held one, completed="
                                                + overview.getCounts().getCompleted());
                            });

            CheckpointTimeoutHoldGate.awaitHeld(
                    holdKey, HOLD_ENTRY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long heldCheckpointId = CheckpointTimeoutHoldGate.heldCheckpointId(holdKey);
            Assertions.assertEquals(
                    BLOCK_ON_SNAPSHOT,
                    CheckpointTimeoutHoldGate.snapshotCount(holdKey),
                    "The sink must hold exactly the configured snapshot");
            Assertions.assertTrue(
                    heldCheckpointId > 0, "The held snapshot must belong to a real checkpoint");

            PipelineCheckpointOverview overviewWhileHeld =
                    pipelineOverview(master, jobId, pipelineId);
            Assertions.assertNotNull(
                    overviewWhileHeld, "The checkpoint overview disappeared while holding");
            Assertions.assertNotNull(
                    overviewWhileHeld.getLatestCompleted(),
                    "There must be a completed checkpoint to restore from");
            long lastCompletedBeforeHold = overviewWhileHeld.getLatestCompleted().getCheckpointId();
            Assertions.assertTrue(
                    lastCompletedBeforeHold < heldCheckpointId,
                    String.format(
                            "The held checkpoint must still be pending, but latestCompleted=%d and"
                                    + " heldCheckpointId=%d",
                            lastCompletedBeforeHold, heldCheckpointId));
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    clientJobProxy.getJobStatus(),
                    "A checkpoint that is still pending must not fail the job");

            // The timeout must be armed and must abort only the held checkpoint while the sink is
            // still parked, i.e. the abort cannot be the consequence of the sink finally acking.
            Awaitility.await()
                    .atMost(TIMEOUT_OBSERVATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        CheckpointTimeoutHoldGate.isHolding(holdKey),
                                        "The sink writer left the held snapshot before the"
                                                + " checkpoint timeout was observed");
                                PipelineCheckpointOverview overview =
                                        pipelineOverview(master, jobId, pipelineId);
                                Assertions.assertNotNull(
                                        overview, "The checkpoint overview is not available");
                                CheckpointInfo latestFailed = overview.getLatestFailed();
                                Assertions.assertNotNull(
                                        latestFailed,
                                        "The held checkpoint was not reported as failed;"
                                                + " checkpoint.timeout was not armed");
                                Assertions.assertEquals(
                                        heldCheckpointId,
                                        latestFailed.getCheckpointId(),
                                        "The reported failure must belong to the held checkpoint");
                                Assertions.assertEquals(
                                        CheckpointStatus.FAILED, latestFailed.getStatus());
                                Assertions.assertNotNull(latestFailed.getFailureReason());
                                Assertions.assertTrue(
                                        latestFailed
                                                .getFailureReason()
                                                .contains(
                                                        CheckpointCloseReason.CHECKPOINT_EXPIRED
                                                                .message()),
                                        "Expected the CHECKPOINT_EXPIRED reason, but got: "
                                                + latestFailed.getFailureReason());
                                Assertions.assertTrue(
                                        overview.getInProgress().stream()
                                                .noneMatch(
                                                        inProgress ->
                                                                inProgress.getCheckpointId()
                                                                        == heldCheckpointId),
                                        "The aborted checkpoint must not stay in progress");
                            });

            // Only now let the parked snapshot finish: from here on the pipeline can cancel the
            // held task and restore.
            CheckpointTimeoutHoldGate.release(holdKey);

            Awaitility.await()
                    .atMost(RESTORE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        clientJobProxy.getJobStatus(),
                                        "A streaming job must survive a checkpoint timeout");
                                Assertions.assertEquals(
                                        1,
                                        pipeline.getPipelineRestoreNum(),
                                        "The timed-out pipeline must restore exactly once,"
                                                + " restoreNum="
                                                + pipeline.getPipelineRestoreNum());
                                PipelineCheckpointOverview overview =
                                        pipelineOverview(master, jobId, pipelineId);
                                Assertions.assertNotNull(
                                        overview, "The checkpoint overview is not available");
                                Assertions.assertTrue(
                                        overview.getCounts().getRestored() >= 1,
                                        "The master did not book a restored pipeline");
                                Assertions.assertNotNull(
                                        overview.getLatestCompleted(),
                                        "The restored pipeline has not completed a checkpoint yet");
                                Assertions.assertTrue(
                                        overview.getLatestCompleted().getCheckpointId()
                                                > heldCheckpointId,
                                        String.format(
                                                "Checkpoint ids must move past the aborted one,"
                                                        + " latestCompleted=%d, aborted=%d",
                                                overview.getLatestCompleted().getCheckpointId(),
                                                heldCheckpointId));
                            });

            PipelineCheckpointOverview overviewAfterRecovery =
                    pipelineOverview(master, jobId, pipelineId);
            Assertions.assertNotNull(overviewAfterRecovery);
            List<CheckpointHistoryEntry> history =
                    overviewAfterRecovery.getHistory().stream()
                            .filter(
                                    entry ->
                                            entry.getCheckpointInfo().getCheckpointId()
                                                    == heldCheckpointId)
                            .collect(Collectors.toList());
            Assertions.assertFalse(
                    history.isEmpty(),
                    "The aborted checkpoint must stay in the checkpoint history");
            Assertions.assertTrue(
                    history.stream()
                            .allMatch(
                                    entry ->
                                            CheckpointStatus.FAILED.equals(
                                                    entry.getCheckpointInfo().getStatus())),
                    "The aborted checkpoint must never be recorded as completed");

            // Recovery must use the last completed checkpoint, not the aborted barrier: the sink
            // writer restored by the new pipeline is handed the state of that checkpoint.
            Awaitility.await()
                    .atMost(RESTORE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            CheckpointTimeoutHoldGate.restoreWriterCalls(holdKey)
                                                    >= 1,
                                            "The restored pipeline did not restore the sink writer"));
            Assertions.assertEquals(
                    CheckpointTimeoutHoldSink.STATE_PREFIX + lastCompletedBeforeHold,
                    CheckpointTimeoutHoldGate.restoredState(holdKey),
                    "The restored pipeline must resume from the last completed checkpoint");

            // The restored pipeline must keep consuming rows, not merely report RUNNING.
            long rowsAfterRecovery = CheckpointTimeoutHoldGate.rowsWritten(holdKey);
            Awaitility.await()
                    .atMost(ROW_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            CheckpointTimeoutHoldGate.rowsWritten(holdKey)
                                                    > rowsAfterRecovery,
                                            "The restored pipeline does not process rows"));

            // A recovered coordinator must also be able to end the job cleanly.
            clientJobProxy.cancelJob();
            Awaitility.await()
                    .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED, clientJobProxy.getJobStatus()));

            log.warn(
                    "Job {} restored pipeline {} after checkpoint {} expired; the sink state"
                            + " restored from checkpoint {} and the job canceled cleanly",
                    jobId,
                    pipelineId,
                    heldCheckpointId,
                    lastCompletedBeforeHold);
        } finally {
            CheckpointTimeoutHoldGate.clear(holdKey);
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
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

    private static String createTestResources(String testCaseName, String holdKey)
            throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        valueMap.put(DYNAMIC_BLOCK_ON_SNAPSHOT, String.valueOf(BLOCK_ON_SNAPSHOT));

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
