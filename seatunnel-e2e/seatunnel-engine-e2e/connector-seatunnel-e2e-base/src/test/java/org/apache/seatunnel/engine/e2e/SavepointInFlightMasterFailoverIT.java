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

package org.apache.seatunnel.engine.e2e.savepointfailover;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.PendingCheckpoint;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * E2E coverage for the boundary "the active master disappears while a stop-with-savepoint request
 * is still waiting for its barrier acknowledgements".
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A stop-with-savepoint request first
 * switches the job to {@link JobStatus#DOING_SAVEPOINT} and only then asks every pipeline
 * coordinator to inject a savepoint barrier. The job stays in that intermediate state until every
 * task acknowledges the barrier, the master stores the checkpoint, and the pipelines stop. If the
 * member that owns the job master dies inside that window, the new active master must rebuild the
 * job from the shared job metadata and let it continue; otherwise the job is stuck in {@code
 * DOING_SAVEPOINT} (a non-terminal state), keeps its slots, and the operator cannot savepoint,
 * cancel or resubmit it without a full cluster restart.
 *
 * <p><b>Why existing coverage does not reach it.</b>
 *
 * <ul>
 *   <li>{@code CheckpointCoordinatorFailoverIT#testStreamJobContinuesAfterMasterFailover} switches
 *       the master while the streaming job is simply RUNNING; no savepoint request is in flight and
 *       the job state machine is not in {@code DOING_SAVEPOINT}.
 *   <li>{@code CheckpointCoordinatorFailoverIT#testBatchJobCompletesAfterMasterFailover} covers a
 *       periodic checkpoint during a master switch, not a user savepoint.
 *   <li>{@code SavepointBusySourceBarrierIT} exercises a savepoint barrier against a busy source,
 *       but the cluster keeps its single master for the whole test.
 *   <li>{@code SplitClusterWorkerRestartAfterFinalCheckpointIT} kills a worker around the final
 *       checkpoint; no savepoint is requested and the active master survives.
 *   <li>{@code SavepointClusterRestartRestoreIT} restarts an entire cluster after a savepoint had
 *       already completed, so no savepoint is in flight either.
 * </ul>
 *
 * <p><b>How the window is pinned.</b> The job writes into {@link SavepointBarrierHoldSink}, whose
 * writer parks inside {@code snapshotState} after the barrier arrived and before the subtask
 * acknowledged it. Because {@code checkpoint.interval} in the job config is far larger than the
 * test duration, the only checkpoint that can reach the sink is the savepoint one. The test waits
 * until the coordinator exposes the pending {@code SAVEPOINT_TYPE} checkpoint with the same id the
 * gate is holding and the job status is {@code DOING_SAVEPOINT}, then shuts the active master down,
 * releases the writer and requires the cluster to settle:
 *
 * <ul>
 *   <li>the blocking client savepoint call must fail instead of hanging or being re-executed on the
 *       new master (a savepoint is not idempotent);
 *   <li>the new master must restore the job to {@code RUNNING} and keep it there, i.e. the failed
 *       request must not put the restored job back into {@code DOING_SAVEPOINT};
 *   <li>the restored job must still be cancellable.
 * </ul>
 *
 * <p>Without the fix the request is retried on the new master: the retry sets the restored job to
 * {@code DOING_SAVEPOINT}, its checkpoint never completes, expires after {@code
 * checkpoint.timeout}, and the job ends {@code FAILED} (after the terminal cleanup the status even
 * becomes {@code UNKNOWABLE}), so the operator loses a job that a master switch is required to
 * preserve.
 *
 * <p>Kept as its own in-JVM cluster class rather than a {@code SeaTunnelEngineContainer} subclass:
 * the contract is about master/savepoint bookkeeping and needs no containerized connector, so two
 * masters plus two workers keep the test fast and repeatable while still crossing real node
 * boundaries.
 */
@Slf4j
public class SavepointInFlightMasterFailoverIT {

    /** Job config template rendered with the per-run hold key. */
    private static final String TEST_TEMPLATE_FILE_NAME =
            "stream_fake_to_savepoint_barrier_hold_template.conf";

    /** Placeholder in the template that is substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Upper bound to wait for the 2-master/2-worker cluster to fully form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the submitted job to reach RUNNING. */
    private static final long JOB_RUNNING_TIMEOUT_SECONDS = 90L;

    /** Upper bound to wait for the savepoint barrier to be held in the sink. */
    private static final long SAVEPOINT_WINDOW_TIMEOUT_SECONDS = 120L;

    /**
     * Upper bound for the client savepoint call to settle after the active master was killed. A
     * hung call is the regression this test guards; the bound is well above the Hazelcast
     * invocation timeout so a slow retry is not mistaken for a hang.
     */
    private static final long SAVEPOINT_SETTLE_TIMEOUT_SECONDS = 180L;

    /** Upper bound to wait for the job to leave DOING_SAVEPOINT after the failover. */
    private static final long RECOVERY_TIMEOUT_SECONDS = 240L;

    /** How long the restored job must stay RUNNING without a duplicate savepoint. */
    private static final long JOB_STABILITY_WINDOW_SECONDS = 30L;

    @Test
    public void testJobRecoversWhenActiveMasterDiesDuringInFlightSavepoint() throws Exception {
        String testCaseName = "testJobRecoversWhenActiveMasterDiesDuringInFlightSavepoint";
        String testClusterName = "SavepointInFlightMasterFailoverIT_" + testCaseName;
        String holdKey = testCaseName + "_" + System.nanoTime();

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SavepointBarrierHoldGate.arm(holdKey);
        try {
            masterNode1 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            masterNode2 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode1 =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode2 =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            awaitClusterSize(masterNode1, 4);

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = createTestResources(testCaseName, holdKey);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            final SeaTunnelClient client = engineClient;

            ClientJobProxy clientJobProxy =
                    engineClient
                            .createExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName))
                            .execute();
            long jobId = clientJobProxy.getJobId();
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);

            HazelcastInstanceImpl activeMaster = awaitActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl survivorMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;
            JobMaster jobMaster = awaitRunningJobMaster(activeMaster, jobId);
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(1, pipelines.size(), "The test job must have one pipeline");
            CheckpointCoordinator coordinator =
                    jobMaster
                            .getCheckpointManager()
                            .getCheckpointCoordinator(pipelines.get(0).getPipelineId());

            // The savepoint must be requested while the job is actually processing data. The gate
            // is armed from the start, but checkpoint.interval is far larger than the test
            // duration, so no checkpoint exists before this request and nothing is held yet.
            Awaitility.await()
                    .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            SavepointBarrierHoldGate.writtenRows(holdKey) > 0,
                                            "The sink has not received any row before the savepoint"));

            // Ask for the savepoint from another thread: SeaTunnelClient#savePointJob blocks until
            // the master's future settles, and the test must keep control of the bound.
            CompletableFuture<Void> savepointRequest =
                    CompletableFuture.runAsync(() -> client.savePointJob(jobId));

            // Window open: the job accepted DOING_SAVEPOINT, the pending checkpoint is the
            // savepoint (not a periodic one), and the sink writer is parked before acknowledging
            // its barrier.
            Awaitility.await()
                    .atMost(SAVEPOINT_WINDOW_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PendingCheckpoint pending =
                                        coordinator.getSavepointPendingCheckpoint();
                                Assertions.assertNotNull(
                                        pending, "The savepoint checkpoint has not been created");
                                Assertions.assertTrue(
                                        pending.getCheckpointType().isSavepoint(),
                                        "The in-flight checkpoint must be the savepoint, but was "
                                                + pending.getCheckpointType());
                                Assertions.assertEquals(
                                        pending.getCheckpointId(),
                                        SavepointBarrierHoldGate.heldCheckpointId(holdKey),
                                        "The held barrier must belong to the pending savepoint");
                                Assertions.assertTrue(
                                        SavepointBarrierHoldGate.holdingWriters(holdKey) >= 1,
                                        "The sink writer is not parked in snapshotState yet");
                                Assertions.assertEquals(
                                        JobStatus.DOING_SAVEPOINT,
                                        clientJobProxy.getJobStatus(),
                                        "The job must be DOING_SAVEPOINT while the savepoint "
                                                + "barrier is unacknowledged");
                            });

            log.warn(
                    "=====================shutdown active master {} while job {} is DOING_SAVEPOINT on checkpoint {}=====================",
                    activeMaster.getCluster().getLocalMember().getAddress(),
                    jobId,
                    SavepointBarrierHoldGate.heldCheckpointId(holdKey));
            activeMaster.shutdown();

            // The survivor must take over the coordinator role while the held task keeps the
            // savepoint checkpoint pending on the old, now dead master.
            awaitActiveMaster(survivorMaster);
            awaitClusterSize(survivorMaster, 3);

            // Let the parked writer go: the new master must be able to restore/cancel the task
            // group without waiting for a barrier acknowledgement that can never arrive.
            SavepointBarrierHoldGate.release(holdKey);

            Throwable savepointFailure = awaitSavepointSettled(savepointRequest);
            // A savepoint request is not idempotent. The client must observe the failure of the
            // request that was owned by the dead master instead of having it silently re-executed
            // on the new master, where it would push the just-restored job back into
            // DOING_SAVEPOINT and eventually fail it on checkpoint timeout.
            Assertions.assertNotNull(
                    savepointFailure,
                    "The savepoint request must fail when the master that owned it dies instead of "
                            + "being retried on the new master");
            log.warn(
                    "The savepoint request of job {} failed after the master switch as expected: {}",
                    jobId,
                    ExceptionUtils.getMessage(savepointFailure));

            // Contract: the new master must restore the job and leave the intermediate
            // DOING_SAVEPOINT state.
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);

            // The retried savepoint request would arrive on the new master within about one
            // second and push the restored job back into DOING_SAVEPOINT, so a stability window
            // is enough to catch it. The window is longer than the time the restore itself needs
            // on a slow CI runner.
            long stableUntil = System.currentTimeMillis() + JOB_STABILITY_WINDOW_SECONDS * 1000L;
            while (System.currentTimeMillis() < stableUntil) {
                Assertions.assertEquals(
                        JobStatus.RUNNING,
                        clientJobProxy.getJobStatus(),
                        "The restored job must keep RUNNING; a request that is silently retried on "
                                + "the new master would put it back into DOING_SAVEPOINT");
                Thread.sleep(500L);
            }

            // The job must still be controllable after the failover.
            client.cancelJob(jobId);
            awaitJobStatus(clientJobProxy, JobStatus.CANCELED);
        } finally {
            SavepointBarrierHoldGate.clear(holdKey);
            if (engineClient != null) {
                engineClient.close();
            }
            shutdownQuietly(masterNode1);
            shutdownQuietly(masterNode2);
            shutdownQuietly(workerNode1);
            shutdownQuietly(workerNode2);
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
     * Renders the job config template with the per-run hold key.
     *
     * @return path of the rendered job config file
     */
    private static String createTestResources(String testCaseName, String holdKey)
            throws Exception {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
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

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedMembers) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedMembers, node.getCluster().getMembers().size()));
    }

    private static SeaTunnelServer getSeaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    /**
     * Returns the master node whose coordinator service is active, i.e. the one running the job.
     */
    private static HazelcastInstanceImpl awaitActiveMaster(HazelcastInstanceImpl... masters) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        Stream.of(masters)
                                                .filter(master -> master != null)
                                                .anyMatch(
                                                        master ->
                                                                getSeaTunnelServer(master)
                                                                        .getCoordinatorService()
                                                                        .isCoordinatorActive()),
                                        "No master node has an active coordinator service"));
        return Stream.of(masters)
                .filter(master -> master != null)
                .filter(
                        master ->
                                getSeaTunnelServer(master)
                                        .getCoordinatorService()
                                        .isCoordinatorActive())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No active master found"));
    }

    /** Waits until the running job master of the job is visible on the active master. */
    private static JobMaster awaitRunningJobMaster(HazelcastInstanceImpl activeMaster, long jobId) {
        Awaitility.await()
                .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster =
                                    getSeaTunnelServer(activeMaster)
                                            .getCoordinatorService()
                                            .getJobMaster(jobId);
                            Assertions.assertNotNull(jobMaster, "JobMaster not registered yet");
                            Assertions.assertNotNull(
                                    jobMaster.getPhysicalPlan(), "Physical plan not created yet");
                            Assertions.assertNotNull(
                                    jobMaster.getCheckpointManager(),
                                    "Checkpoint manager not created yet");
                        });
        return getSeaTunnelServer(activeMaster).getCoordinatorService().getJobMaster(jobId);
    }

    /**
     * Waits until the client savepoint call settles.
     *
     * @return the failure the call ended with, or {@code null} when the savepoint completed
     */
    private static Throwable awaitSavepointSettled(CompletableFuture<Void> savepointRequest) {
        return awaitSavepointSettled(savepointRequest, SAVEPOINT_SETTLE_TIMEOUT_SECONDS);
    }

    /**
     * Waits until the client savepoint call settles within the given bound.
     *
     * @return the failure the call ended with, or {@code null} when the savepoint completed
     */
    private static Throwable awaitSavepointSettled(
            CompletableFuture<Void> savepointRequest, long timeoutSeconds) {
        try {
            savepointRequest.get(timeoutSeconds, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (TimeoutException e) {
            Assertions.fail(
                    "The stop-with-savepoint call did not settle within "
                            + timeoutSeconds
                            + "s after the active master switch; the savepoint request hangs");
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for the savepoint to settle", e);
        }
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(RECOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> Assertions.assertEquals(expected, jobProxy.getJobStatus()));
    }

    private static void shutdownQuietly(HazelcastInstanceImpl node) {
        if (node == null) {
            return;
        }
        try {
            node.shutdown();
        } catch (Exception e) {
            log.warn("Failed to shut down test node: {}", ExceptionUtils.getMessage(e));
        }
    }
}
