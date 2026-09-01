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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression test for the stop-with-savepoint hang fixed by <a
 * href="https://github.com/apache/seatunnel/pull/11489">#11489</a> ("[Fix][Zeta] Fix
 * stop-with-savepoint hang in DOING_SAVEPOINT"). Before that fix, {@code JobMaster#savePoint()}
 * moved the job into {@link JobStatus#DOING_SAVEPOINT} and then, if every pipeline's {@link
 * CheckpointCoordinator#startSavepoint()} rejected the request's precondition (not all tasks ready,
 * or the coordinator mid-shutdown), never left that state: nothing rolled the job back to {@link
 * JobStatus#RUNNING} or drove it to a terminal state, so it hung in {@code DOING_SAVEPOINT}
 * forever. The fix classifies these precondition rejections in {@code
 * JobMaster#waitSavepointCompleted}/{@code isSavepointStartPreconditionFailure} and, when every
 * pipeline failed for a precondition reason, restores the job to RUNNING via {@code
 * JobMaster#restoreRunningAfterSavepointStartFailure()}; any other failure instead drives the job
 * to a terminal state through the new {@link
 * org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan#savepointFailed()} fallback.
 *
 * <p>The fix's own accompanying tests ({@code JobMasterTest}, {@code SavePointBusySourceTest}) are
 * unit-level: they invoke the classification methods directly via reflection against fabricated,
 * already-failed {@code CompletableFuture}s, never submitting a real job or driving a real {@link
 * CheckpointCoordinator} through its actual precondition check. {@code SavepointRestoreIT} and
 * {@code CheckpointRestoreWithStopIT} in this package only exercise the happy path (savepoint
 * succeeds, then restore from it). None of the existing coverage proves that a real,
 * client-triggered savepoint request landing on a real {@link CheckpointCoordinator} while it
 * genuinely rejects the not-ready precondition actually recovers instead of hanging.
 *
 * <p>This test closes that gap by racing a real {@code SeaTunnelClient} savepoint request against
 * real task deployment on a real (split master/worker) cluster, using white-box polling of the
 * master's in-process state to land reliably inside the rejection window instead of guessing a
 * sleep duration -- see {@link #awaitCheckpointCoordinatorNotAllTaskReady}.
 */
public class SavepointPreconditionRecoveryIT {

    private static final String TEMPLATE_CONF =
            "stream_fake_to_localfile_savepoint_precondition_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    /** The test job has exactly one FakeSource -> LocalFile pipeline, so its id is always 1. */
    private static final int PIPELINE_ID = 1;

    @Test
    public void testSavepointRecoversFromNotAllTaskReadyPrecondition() throws Exception {
        String testCaseName = "testSavepointRecoversFromNotAllTaskReadyPrecondition";
        String testClusterName = "SavepointPreconditionRecoveryIT_" + testCaseName;

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerConfig = getSeaTunnelConfig(testClusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources = createTestResources(testCaseName);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, masterConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            // Race the savepoint request against task deployment: spin-poll the master's
            // in-process CheckpointCoordinator (the exact object startSavepoint() reads from)
            // until it exists but has not yet observed every subtask report READY_START, then
            // fire the real client savepoint request in the very next statement so it arrives
            // on the master while the precondition is still false.
            awaitCheckpointCoordinatorNotAllTaskReady(
                    masterNode, jobId, PIPELINE_ID, TimeUnit.SECONDS.toMillis(20));
            SeaTunnelClient finalEngineClient = engineClient;
            Exception rejection =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> finalEngineClient.savePointJob(jobId),
                            "A savepoint request fired while isAllTaskReady is still false must"
                                    + " be rejected by CheckpointCoordinator#startSavepoint(),"
                                    + " not silently accepted");
            String rejectionTrace = ExceptionUtils.getMessage(rejection);
            Assertions.assertTrue(
                    rejectionTrace.contains(
                            CheckpointCloseReason.TASK_NOT_ALL_READY_WHEN_SAVEPOINT.message()),
                    "Expected the savepoint rejection to be the not-all-tasks-ready precondition"
                            + " (proving the intended race was actually hit), but got: "
                            + rejectionTrace);

            // Core regression assertion. Pre-fix, the rejection above left the job stuck in
            // DOING_SAVEPOINT forever: nothing ever moved it back to RUNNING or to a terminal
            // state. Post-fix, because every pipeline failed for a precondition reason and none
            // reached SUSPEND, JobMaster#restoreRunningAfterSavepointStartFailure() runs in the
            // finally block of JobMaster#savePoint() and restores the job to RUNNING so it can be
            // retried, instead of leaving it wedged.
            ClientJobProxy finalClientJobProxy = clientJobProxy;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING,
                                            finalClientJobProxy.getJobStatus(),
                                            "Job must recover to RUNNING after a rejected"
                                                    + " savepoint precondition, not hang in"
                                                    + " DOING_SAVEPOINT"));

            // Prove the recovery is functionally real, not a cosmetic status flip: the pipeline
            // must still be moving data after coming back from the rejected savepoint attempt.
            long rowsAtRecovery = FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            FileUtils.getFileLineNumberFromDir(
                                                            testResources.getLeft())
                                                    > rowsAtRecovery,
                                            "Job must keep producing data after recovering from"
                                                    + " the rejected savepoint"));

            // Prove the CheckpointCoordinator itself recovered, not just the job status label: a
            // second savepoint request, issued long after isAllTaskReady has settled true, must
            // now succeed and drive the job through its normal savepoint-completed terminal
            // state.
            Assertions.assertDoesNotThrow(
                    () -> finalEngineClient.savePointJob(jobId),
                    "A savepoint issued after the job is confirmed RUNNING and producing data"
                            + " should succeed normally");
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            finalClientJobProxy.getJobStatus()));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static ImmutablePair<String, String> createTestResources(String testCaseName)
            throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);

        String targetDir = ("/tmp/hive/warehouse/" + testCaseName).replace("/", File.separator);
        FileUtils.createNewDir(targetDir);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(TEMPLATE_CONF, valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    /**
     * Spin-polls the master's in-process {@link CheckpointCoordinator} for {@code pipelineId} of
     * {@code jobId} until it exists but has not yet observed every subtask of the pipeline report
     * READY_START.
     *
     * <p>{@link CheckpointCoordinator#startSavepoint()} rejects a savepoint request with {@link
     * CheckpointCloseReason#TASK_NOT_ALL_READY_WHEN_SAVEPOINT} exactly while its private {@code
     * isAllTaskReady} flag is still false. That flag only flips true once every subtask in the
     * pipeline has been deployed to a worker, started, and reported back over the network -- a
     * multi-step handshake that takes real wall-clock time. The {@link CheckpointCoordinator}
     * instance itself, by contrast, is created synchronously inside {@code JobMaster#init()} before
     * the job first enters the pending queue. The public {@code getJobMaster()} accessor therefore
     * observes the coordinator before {@code CoordinatorService#savePoint(long)} can accept the
     * request, because the latter only accepts entries in {@code runningJobMasterMap}. This helper
     * intentionally waits for that same map before inspecting {@code isAllTaskReady}, so the next
     * client request reaches the real precondition check rather than the unrelated "job not
     * running" rejection. Spinning on the exact field the precondition check reads, once that
     * shared gate is open, lands inside the not-ready window with a wide safety margin instead of
     * guessing a sleep duration.
     *
     * @return the CheckpointCoordinator observed with isAllTaskReady still false, so the caller can
     *     immediately fire a real savepoint request against it
     */
    private static CheckpointCoordinator awaitCheckpointCoordinatorNotAllTaskReady(
            HazelcastInstanceImpl masterNode, long jobId, int pipelineId, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            JobMaster jobMaster = getRunningJobMaster(masterNode, jobId);
            if (jobMaster != null) {
                CheckpointManager checkpointManager = jobMaster.getCheckpointManager();
                if (checkpointManager != null) {
                    try {
                        CheckpointCoordinator coordinator =
                                checkpointManager.getCheckpointCoordinator(pipelineId);
                        if (coordinator != null && !isAllTaskReady(coordinator)) {
                            return coordinator;
                        }
                    } catch (RuntimeException notYetRegistered) {
                        // getCheckpointCoordinator(pipelineId) throws if the pipeline id is not
                        // (yet) present; keep spinning until the coordinator map is visible.
                    }
                }
            }
        }
        return Assertions.fail(
                "Timed out waiting to observe pipeline "
                        + pipelineId
                        + " of job "
                        + jobId
                        + " with a CheckpointCoordinator whose isAllTaskReady flag was still"
                        + " false; either the not-ready window closed before this test could"
                        + " observe it, or the job failed to schedule");
    }

    /**
     * Reads the private {@code isAllTaskReady} flag via reflection -- the exact precondition {@link
     * CheckpointCoordinator#startSavepoint()} checks -- since the class exposes no public accessor
     * for it.
     */
    private static boolean isAllTaskReady(CheckpointCoordinator coordinator) {
        Optional<Object> field = ReflectionUtils.getField(coordinator, "isAllTaskReady");
        Assertions.assertTrue(
                field.isPresent() && field.get() instanceof AtomicBoolean,
                "CheckpointCoordinator.isAllTaskReady field not found via reflection; it may"
                        + " have been renamed");
        return ((AtomicBoolean) field.get()).get();
    }

    /**
     * Reads the job master from the same running-job map checked by the savepoint RPC endpoint.
     *
     * <p>{@code CoordinatorService#getJobMaster(long)} is intentionally not used here because it
     * returns an entry from the pending queue before the savepoint endpoint can process the job.
     */
    private static JobMaster getRunningJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Optional<Object> runningJobMasterMap =
                ReflectionUtils.getField(server.getCoordinatorService(), "runningJobMasterMap");
        Assertions.assertTrue(
                runningJobMasterMap.isPresent() && runningJobMasterMap.get() instanceof Map<?, ?>,
                "CoordinatorService.runningJobMasterMap field not found via reflection; it may"
                        + " have been renamed");
        Object jobMaster = ((Map<?, ?>) runningJobMasterMap.get()).get(jobId);
        return jobMaster instanceof JobMaster ? (JobMaster) jobMaster : null;
    }
}
