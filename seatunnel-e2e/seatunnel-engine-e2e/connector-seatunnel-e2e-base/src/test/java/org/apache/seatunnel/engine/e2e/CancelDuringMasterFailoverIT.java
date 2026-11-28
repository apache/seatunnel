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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.workerrestart.FinalCheckpointCloseHoldGate;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Regression guard for the lifecycle contract of a cancellation that has already been accepted when
 * the Zeta master dies: the cancellation must survive the master handoff.
 *
 * <p>Scenario: a streaming job is RUNNING on a split master/worker cluster. The client asks the
 * active master to cancel it; the job persists {@link JobStatus#CANCELING} and starts cancelling
 * its tasks. One task is deliberately frozen inside {@code SinkWriter#close()} by {@link
 * FinalCheckpointCloseHoldGate}, so the old attempt cannot finish quietly on the worker while the
 * test changes master ownership. The test kills the active master (Hazelcast terminate, i.e. a
 * crash, not a graceful shutdown) as soon as it observes the persisted CANCELING state, then lets
 * the standby master take over.
 *
 * <p>The contract under test: once a cancel request has been accepted, a master failover must not
 * lose it. The new master must take over the persisted job and drive the cancellation to its
 * terminal state: the job must end CANCELED, stay CANCELED, and never be left in CANCELING or
 * UNKNOWABLE, because that would silently undo the operator's stop request. The test deliberately
 * does not pin the intermediate states the restore path passes through before that convergence;
 * only the terminal outcome and its stability are guarded here.
 *
 * <p>Existing coverage does not exercise this window. {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck} kills
 * the worker (not the master) after a task vertex enters CANCELING; {@code
 * SplitClusterPendingJobLifecycleFailoverIT} and {@code CoordinatorExecutorMassFailoverStormIT}
 * cancel jobs only after the failover has already completed; {@code
 * SplitClusterFaultToleranceIT#testStreamJobRestoreInMasterDown} fails over a job that is simply
 * RUNNING with no cancel in flight. No existing engine E2E changes master ownership while the job
 * is already CANCELING, which is exactly the state the restore path must treat specially: {@code
 * CoordinatorService#restoreJobFromMasterActiveSwitch} recreates a JobMaster for every persisted
 * non-terminal job and re-enqueues it as PENDING/RESTORE, so a persisted CANCELING job that is not
 * handled explicitly would be revived instead of finishing its cancellation.
 *
 * <p>The CANCELING window is short (milliseconds to a couple of seconds), so this test tight-polls
 * the active master's own in-JVM job status (a plain in-memory read, no network round trip) and
 * terminates the master the instant CANCELING is observed. Because the state transition is gated by
 * real IMap writes and cancellation RPCs while the poll loop is not, the sample rate is orders of
 * magnitude higher than the window it races against. If an attempt still loses the race (the cancel
 * completed before the kill), the test retries the whole scenario on the same cluster instead of
 * failing on timing; only an attempt that provably crashed the master mid-cancel is used for the
 * assertions.
 */
@Slf4j
public class CancelDuringMasterFailoverIT {

    private static final String JOB_TEMPLATE_FILE =
            "cancel-master-failover/stream_fake_to_close_hold_sink.conf";
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";
    private static final String DYNAMIC_OUTPUT_PATH = "dynamic_output_path";

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30;
    private static final long JOB_STATE_TIMEOUT_SECONDS = 180;
    private static final int MAX_CANCEL_RACE_ATTEMPTS = 5;

    @Test
    public void testCancelRequestedBeforeMasterFailoverIsNotLost() throws Exception {
        String testCaseName = "testCancelRequestedBeforeMasterFailoverIsNotLost";
        String testClusterName = "CancelDuringMasterFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        String outputPath = createSinkOutputPath(testCaseName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalWorker = workerNode;
            Awaitility.await()
                    .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorker.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = awaitActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            CancelledAttempt cancelledAttempt =
                    cancelWhileCrashingMaster(
                            engineClient,
                            activeMaster,
                            masterNode1Config,
                            testCaseName,
                            outputPath);
            long jobId = cancelledAttempt.jobId;

            Awaitility.await()
                    .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertTrue(
                                        isCoordinatorActive(standbyMaster),
                                        "Standby master should become active after failover");
                            });

            CoordinatorService standbyCoordinatorService = getCoordinatorService(standbyMaster);

            // Wait until the promoted master has taken over the persisted job (it owns a restored
            // JobMaster or the job is already terminal). Releasing the held writer before that
            // would let the old task's last status report race the restore, which is a different
            // scenario than the one under test.
            long takeoverDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
            while (System.currentTimeMillis() < takeoverDeadline) {
                JobStatus status = standbyCoordinatorService.getJobStatus(jobId);
                if (status != null && status.isEndState()) {
                    break;
                }
                if (getJobMaster(standbyMaster, jobId) != null) {
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(10);
            }

            // Let the worker finish closing its writer; the new master must then drive the still
            // persisted cancellation to its terminal state.
            FinalCheckpointCloseHoldGate.release(cancelledAttempt.holdKey);

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            standbyCoordinatorService.getJobStatus(jobId),
                                            "The cancellation must complete on the new master"));

            // Stability window: once terminal, the job must stay CANCELED (no late re-dispatch
            // from the restore path) and the client-visible status must agree.
            ClientJobProxy jobProxyAfterFailover =
                    engineClient.createJobClient().getJobProxy(jobId);
            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        standbyCoordinatorService.getJobStatus(jobId));
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, jobProxyAfterFailover.getJobStatus());
                            });
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null && workerNode.getLifecycleService().isRunning()) {
                workerNode.shutdown();
            }
            if (masterNode2 != null && masterNode2.getLifecycleService().isRunning()) {
                masterNode2.shutdown();
            }
            if (masterNode1 != null && masterNode1.getLifecycleService().isRunning()) {
                masterNode1.shutdown();
            }
            FileUtils.deleteFile(outputPath);
        }
    }

    /**
     * Submits a streaming job, starts a real client cancel, and terminates the active master the
     * instant the job's own persisted state is observed as CANCELING. If the cancel wins the race
     * and completes first, the attempt is thrown away (gate released, job terminal) and retried,
     * because only a kill that provably landed inside the cancellation window is meaningful here.
     *
     * @return the job id and hold key of the attempt whose master was killed mid-cancel
     */
    private static CancelledAttempt cancelWhileCrashingMaster(
            SeaTunnelClient engineClient,
            HazelcastInstanceImpl activeMaster,
            SeaTunnelConfig jobConfigSource,
            String testCaseName,
            String outputPath)
            throws Exception {
        for (int attempt = 1; attempt <= MAX_CANCEL_RACE_ATTEMPTS; attempt++) {
            String holdKey = testCaseName + "_attempt_" + attempt + "_" + System.nanoTime();
            String jobConfigFile = createJobConfigFile(testCaseName, attempt, holdKey, outputPath);
            FinalCheckpointCloseHoldGate.arm(holdKey);
            ClientJobProxy jobProxy = null;
            try {
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName(testCaseName + "_attempt_" + attempt);
                ClientJobExecutionEnvironment jobExecutionEnv =
                        engineClient.createExecutionContext(
                                jobConfigFile, jobConfig, jobConfigSource);
                jobProxy = jobExecutionEnv.execute();
                long jobId = jobProxy.getJobId();

                ClientJobProxy finalJobProxy = jobProxy;
                Awaitility.await()
                        .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                JobStatus.RUNNING, finalJobProxy.getJobStatus()));
                // Make sure the CANCELING observation below belongs to the cancel request, not to
                // an in-flight DEPLOYING -> RUNNING transition.
                assertAllVerticesRunning(activeMaster, jobId, 60);

                ClientJobProxy finalCancelProxy = jobProxy;
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                finalCancelProxy.cancelJob();
                            } catch (Throwable t) {
                                log.info(
                                        "cancelJob() did not return cleanly after the active "
                                                + "master was killed",
                                        t);
                            }
                        });

                CoordinatorService activeCoordinatorService = getCoordinatorService(activeMaster);
                boolean observedCanceling = false;
                long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
                while (System.currentTimeMillis() < deadline) {
                    JobStatus status = activeCoordinatorService.getJobStatus(jobId);
                    if (JobStatus.CANCELING.equals(status)) {
                        observedCanceling = true;
                        break;
                    }
                    if (status != null && status.isEndState()) {
                        break;
                    }
                    Thread.yield();
                }

                if (!observedCanceling) {
                    log.info(
                            "Cancel attempt {} lost the race before the master could be killed, "
                                    + "retrying",
                            attempt);
                    // Release the held writer so the attempt can reach its terminal state and
                    // free the worker slots before the next attempt submits a new job.
                    FinalCheckpointCloseHoldGate.release(holdKey);
                    CoordinatorService coordinatorService = getCoordinatorService(activeMaster);
                    Awaitility.await()
                            .atMost(60, TimeUnit.SECONDS)
                            .untilAsserted(
                                    () ->
                                            Assertions.assertTrue(
                                                    coordinatorService
                                                            .getJobStatus(jobId)
                                                            .isEndState()));
                    FinalCheckpointCloseHoldGate.clear(holdKey);
                    continue;
                }

                // Crash the master inside the cancellation window: terminate() does not give the
                // coordinator a chance to finish the in-flight cancel before the node goes away.
                activeMaster.getLifecycleService().terminate();
                return new CancelledAttempt(jobId, holdKey);
            } catch (Exception e) {
                FinalCheckpointCloseHoldGate.clear(holdKey);
                if (jobProxy != null) {
                    try {
                        jobProxy.cancelJob();
                    } catch (Exception ignored) {
                        // Best effort cleanup between attempts.
                    }
                }
                throw e;
            }
        }
        throw new AssertionError(
                "Could not construct the race: the cancel completed on the active master before "
                        + "it could be killed in all "
                        + MAX_CANCEL_RACE_ATTEMPTS
                        + " attempts");
    }

    private static void assertAllVerticesRunning(
            HazelcastInstanceImpl masterNode, long jobId, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = getJobMaster(masterNode, jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "Job master should exist before checking task vertex states");
                            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                            Assertions.assertEquals(JobStatus.RUNNING, physicalPlan.getJobStatus());
                            for (SubPlan subPlan : physicalPlan.getPipelineList()) {
                                subPlan.getCoordinatorVertexList()
                                        .forEach(
                                                vertex ->
                                                        Assertions.assertEquals(
                                                                ExecutionState.RUNNING,
                                                                vertex.getExecutionState()));
                                subPlan.getPhysicalVertexList()
                                        .forEach(
                                                vertex ->
                                                        Assertions.assertEquals(
                                                                ExecutionState.RUNNING,
                                                                vertex.getExecutionState()));
                            }
                        });
    }

    private static String createSinkOutputPath(String testCaseName) {
        String outputPath = "/tmp/hive/warehouse/" + testCaseName;
        outputPath = outputPath.replace("/", File.separator);
        FileUtils.createNewDir(outputPath);
        return outputPath;
    }

    private static String createJobConfigFile(
            String testCaseName, int attempt, String holdKey, String outputPath) throws Exception {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        valueMap.put(DYNAMIC_OUTPUT_PATH, outputPath);
        String jobConfigFile =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + "_attempt_"
                        + attempt
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(JOB_TEMPLATE_FILE, valueMap, jobConfigFile);
        return jobConfigFile;
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static HazelcastInstanceImpl awaitActiveMaster(HazelcastInstanceImpl... masters) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        Stream.of(masters)
                                                .anyMatch(
                                                        CancelDuringMasterFailoverIT
                                                                ::isCoordinatorActive),
                                        "No master node has an active coordinator service"));
        return Stream.of(masters)
                .filter(CancelDuringMasterFailoverIT::isCoordinatorActive)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No active master found"));
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return getCoordinatorService(masterNode).isCoordinatorActive();
        } catch (SeaTunnelEngineException | IllegalStateException e) {
            return false;
        }
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl node, long jobId) {
        return getCoordinatorService(node).getJobMaster(jobId);
    }

    /** Job id and gate key of the attempt whose active master was killed mid-cancel. */
    private static final class CancelledAttempt {
        private final long jobId;
        private final String holdKey;

        private CancelledAttempt(long jobId, String holdKey) {
            this.jobId = jobId;
            this.holdKey = holdKey;
        }
    }
}
