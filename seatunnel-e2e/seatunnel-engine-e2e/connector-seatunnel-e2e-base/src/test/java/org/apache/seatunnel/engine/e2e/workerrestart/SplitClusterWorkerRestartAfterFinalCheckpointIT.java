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

package org.apache.seatunnel.engine.e2e.workerrestart;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Regression coverage for apache/seatunnel#11563 (fixed by apache/seatunnel#11565): a Worker that
 * disappears after a bounded BATCH pipeline already completed its final checkpoint, but before
 * every task of that pipeline reported FINISHED, must not fail the job with a {@code
 * NullPointerException} in {@code CheckpointCoordinator#isNoErrorCompleted()} while the pipeline is
 * restored.
 *
 * <p><b>The window.</b> When the last source subtask reports ready-to-close, the coordinator
 * triggers the {@code COMPLETED_POINT_TYPE} checkpoint. {@code
 * CheckpointCoordinator#completePendingCheckpoint} stores it as {@code latestCompletedCheckpoint}
 * and only then notifies the tasks through {@code CheckpointFinishedOperation}; the tasks close and
 * report FINISHED afterwards, and the pipeline turns FINISHED once the last of them did. A worker
 * that is lost inside that gap has RUNNING tasks on the master's books, so {@code
 * CoordinatorService#failedTaskOnMemberRemoved} marks them FAILED, the pipeline turns FAILED and
 * {@code SubPlan#restorePipeline} asks {@code CheckpointManager#isCompletedPipeline}, which reads
 * the coordinator status entry {@code checkpoint_state_<jobId>_<pipelineId>} from the running job
 * state map. On current {@code dev} nothing seeds that entry for a freshly submitted job and {@code
 * CheckpointCoordinator#updateStatus} skips persisting when the entry is missing
 * (apache/seatunnel#10687), so the read yields {@code null}; before apache/seatunnel#11565 the
 * method dereferenced it and the job failed once the retries were exhausted, now the missing state
 * counts as "not completed" and the normal restore path continues.
 *
 * <p><b>Why the existing worker-down tests never reach it.</b> {@code
 * ClusterFaultToleranceIT#testBatchJobRestoreIn2NodeWorkerDown}, {@code
 * SplitClusterFaultToleranceIT#testBatchJobRestoreInWorkerDown} and {@code
 * ClusterFaultToleranceTwoPipelineIT#testTwoPipelineBatchJobRestoreIn2NodeWorkerDown} all shut the
 * worker down while the sink output is still growing, i.e. while the latest completed checkpoint is
 * at most a periodic {@code CHECKPOINT_TYPE}. {@code isNoErrorCompleted()} short-circuits on {@code
 * isFinalCheckpoint()} before it touches the status entry, so those tests cannot observe the null
 * dereference; {@code ClusterFailureNoRestoreIT} disables restore altogether. The gap between the
 * final checkpoint and the last FINISHED report is normally milliseconds long, which is why the
 * issue reporter only hit it with one out of several short jobs.
 *
 * <p><b>How this test pins the window.</b> The job writes into {@link
 * FinalCheckpointCloseHoldSink}, whose writer parks inside {@code close()} on {@link
 * FinalCheckpointCloseHoldGate}. Because a sink task closes its writer only after the final
 * checkpoint was completed and notified, the held task keeps the pipeline RUNNING exactly inside
 * the window. The test polls the master's own {@link CheckpointCoordinator#isCompleted()} plus the
 * gate until both confirm the window is open, shuts down the worker that hosts the held task, waits
 * until the master booked a pipeline restore, and only then releases the writer and starts a
 * replacement worker (the "service manager restarts the worker" step of the issue). The job must
 * still finish with every row exactly once, the pipeline must have gone through a restore, and no
 * {@code NullPointerException} from {@code isNoErrorCompleted()} may be logged by the engine.
 */
@Slf4j
public class SplitClusterWorkerRestartAfterFinalCheckpointIT {

    /** Name of the test resource template rendered into the actual job config file. */
    private static final String TEST_TEMPLATE_FILE_NAME =
            "cluster_batch_fake_to_close_hold_sink_template.conf";

    /** Placeholder key in the template that is substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Placeholder key in the template that is substituted with the sink output directory. */
    private static final String DYNAMIC_OUTPUT_PATH = "dynamic_output_path";

    /** Placeholder key in the template that is substituted with the per-parallelism row count. */
    private static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    /** Placeholder key in the template that is substituted with the source/sink parallelism. */
    private static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    /** Enough rows to make an accidental replay visible, small enough to finish in seconds. */
    private static final long TEST_ROW_NUMBER_PER_PARALLELISM = 2_000L;

    /**
     * A single chained source-and-sink task keeps the "exactly one task is still RUNNING after the
     * final checkpoint" precondition simple to verify and mirrors the parallelism of the issue.
     */
    private static final int TEST_PARALLELISM = 1;

    /** Upper bound to wait for the 2-master/2-worker cluster to fully form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30L;

    /** Upper bound to wait for the submitted job to reach RUNNING. */
    private static final long JOB_RUNNING_TIMEOUT_SECONDS = 60L;

    /**
     * Upper bound to wait for the final-checkpoint-completed-but-writer-still-held window to open;
     * generous because the hold sink must first consume every row.
     */
    private static final long WINDOW_TIMEOUT_MINUTES = 3L;

    /** Upper bound to wait for the master to book a pipeline restore after the worker is killed. */
    private static final long RESTORE_BOOKED_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the restored pipeline to reach a terminal job status. */
    private static final long JOB_COMPLETE_TIMEOUT_MINUTES = 5L;

    /** Fragment identifying a captured NullPointerException log event. */
    private static final String NPE_SIGNATURE = "NullPointerException";

    /**
     * Fragment identifying that a captured NullPointerException originated from {@code
     * CheckpointCoordinator#isNoErrorCompleted()}, i.e. the exact regression this test guards.
     */
    private static final String RESTORE_CHECK_SIGNATURE = "isNoErrorCompleted";

    @Test
    public void testBatchJobRestoresAfterWorkerRestartWithMissingCoordinatorState()
            throws Exception {
        String testCaseName = "testBatchJobRestoresAfterWorkerRestartWithMissingCoordinatorState";
        String testClusterName = "SplitClusterWorkerRestartAfterFinalCheckpointIT_" + testCaseName;
        String holdKey = testCaseName + "_" + System.nanoTime();
        long expectedRows = TEST_ROW_NUMBER_PER_PARALLELISM * TEST_PARALLELISM;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        HazelcastInstanceImpl replacementWorker = null;
        SeaTunnelClient engineClient = null;
        CapturedEngineLogs engineLogs = CapturedEngineLogs.attach(testCaseName);

        FinalCheckpointCloseHoldGate.arm(holdKey);
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
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName,
                            holdKey,
                            TEST_ROW_NUMBER_PER_PARALLELISM,
                            TEST_PARALLELISM);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(),
                            jobConfig,
                            getSeaTunnelConfig(testClusterName));
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(JOB_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            HazelcastInstanceImpl activeMaster = awaitActiveMaster(masterNode1, masterNode2);
            JobMaster jobMaster = awaitRunningJobMaster(activeMaster, jobId);
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(1, pipelines.size(), "The test job must have one pipeline");
            SubPlan pipeline = pipelines.get(0);
            CheckpointCoordinator coordinator =
                    jobMaster
                            .getCheckpointManager()
                            .getCheckpointCoordinator(pipeline.getPipelineId());

            // Window open: the master already stored the final checkpoint, the writer is parked in
            // close(), and the held task is the only task of the pipeline that is still RUNNING.
            Awaitility.await()
                    .atMost(WINDOW_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        coordinator.isCompleted(),
                                        "The final checkpoint has not been completed on the master yet");
                                Assertions.assertEquals(
                                        1,
                                        FinalCheckpointCloseHoldGate.holdingWriters(holdKey),
                                        "The sink writer is not parked inside close() yet");
                                Assertions.assertEquals(
                                        1,
                                        runningVertices(pipeline).size(),
                                        "Exactly one task must still be RUNNING after the final checkpoint");
                            });
            PhysicalVertex heldVertex = runningVertices(pipeline).get(0);
            Assertions.assertTrue(
                    pipeline.getPhysicalVertexList().contains(heldVertex),
                    "The task still RUNNING after the final checkpoint must be the held sink task");
            Assertions.assertEquals(
                    PipelineStatus.RUNNING,
                    pipeline.getPipelineState(),
                    "The held task must keep the pipeline in RUNNING");
            Object coordinatorStateEntry =
                    activeMaster
                            .<Object, Object>getMap(Constant.IMAP_RUNNING_JOB_STATE)
                            .get(coordinator.getCheckpointStateImapKey());
            // WARN on purpose: the module's test logging keeps the root logger at WARN.
            log.warn(
                    "Window open for job {}: final checkpoint completed, coordinator state entry {} is {}, held task {} on {}",
                    jobId,
                    coordinator.getCheckpointStateImapKey(),
                    coordinatorStateEntry,
                    heldVertex.getTaskFullName(),
                    heldVertex.getCurrentExecutionAddress());

            Address heldAddress = heldVertex.getCurrentExecutionAddress();
            HazelcastInstanceImpl workerToKill = findWorker(heldAddress, workerNode1, workerNode2);

            CompletableFuture<JobResult> completeFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);

            log.warn(
                    "=====================shutdown worker {} while the sink task is held in close()=====================",
                    heldAddress);
            workerToKill.shutdown();

            // The master must book the restore while the task is still un-finished on its side:
            // SubPlan#prepareRestorePipeline increments the restore counter right after the
            // pipeline turned FAILED and before it evaluates isNoErrorCompleted().
            Awaitility.await()
                    .atMost(RESTORE_BOOKED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertNotEquals(
                                        ExecutionState.RUNNING,
                                        heldVertex.getExecutionState(),
                                        "The master has not noticed the lost worker yet");
                                Assertions.assertTrue(
                                        pipeline.getPipelineRestoreNum() >= 1,
                                        "The pipeline has not entered the restore path yet");
                            });

            // Only now let the parked writer go. Its worker is already gone, so it cannot report
            // anything to the master; the writer of the restored pipeline must not hold at all.
            FinalCheckpointCloseHoldGate.release(holdKey);

            replacementWorker =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            awaitClusterSize(activeMaster, 4);

            Awaitility.await()
                    .atMost(JOB_COMPLETE_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(completeFuture.isDone()));
            JobResult jobResult = completeFuture.get();
            Assertions.assertEquals(
                    JobStatus.FINISHED,
                    jobResult.getStatus(),
                    "The job must finish after the worker restart, error: " + jobResult.getError());
            Assertions.assertNull(
                    jobResult.getError(), "A finished job must not carry an error message");
            Assertions.assertTrue(
                    pipeline.getPipelineRestoreNum() >= 1,
                    "The pipeline must have been restored after the worker loss");
            Assertions.assertEquals(
                    expectedRows,
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft()),
                    "Every source row must be written exactly once");

            List<String> restoreNpeEvents =
                    engineLogs.eventsContaining(NPE_SIGNATURE, RESTORE_CHECK_SIGNATURE);
            Assertions.assertTrue(
                    restoreNpeEvents.isEmpty(),
                    "The engine logged a NullPointerException from isNoErrorCompleted() during restore: "
                            + restoreNpeEvents);
            List<String> anyNpeEvents = engineLogs.eventsContaining(NPE_SIGNATURE);
            if (!anyNpeEvents.isEmpty()) {
                log.warn(
                        "Engine logged NullPointerException events during the test: {}",
                        anyNpeEvents);
            }
        } finally {
            FinalCheckpointCloseHoldGate.clear(holdKey);
            engineLogs.detach();

            if (engineClient != null) {
                engineClient.close();
            }
            shutdownQuietly(masterNode1);
            shutdownQuietly(masterNode2);
            shutdownQuietly(workerNode1);
            shutdownQuietly(workerNode2);
            shutdownQuietly(replacementWorker);
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
     * Creates the job config from the template and clears the sink output directory.
     *
     * @return pair of sink output directory and job config file path
     */
    private static ImmutablePair<String, String> createTestResources(
            @NonNull String testCaseName, @NonNull String holdKey, long rowNumber, int parallelism)
            throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(parallelism > 0, "parallelism must greater than 0");

        String targetDir = "/tmp/hive/warehouse/" + testCaseName;
        targetDir = targetDir.replace("/", File.separator);
        FileUtils.createNewDir(targetDir);

        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        valueMap.put(DYNAMIC_OUTPUT_PATH, targetDir);
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));
        valueMap.put(DYNAMIC_TEST_PARALLELISM, String.valueOf(parallelism));

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
        return new ImmutablePair<>(targetDir, targetConfigFilePath);
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
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        Stream.of(masters)
                                                .anyMatch(
                                                        master ->
                                                                getSeaTunnelServer(master)
                                                                        .getCoordinatorService()
                                                                        .isCoordinatorActive()),
                                        "No master node has an active coordinator service"));
        return Stream.of(masters)
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

    private static List<PhysicalVertex> runningVertices(SubPlan pipeline) {
        return Stream.concat(
                        pipeline.getCoordinatorVertexList().stream(),
                        pipeline.getPhysicalVertexList().stream())
                .filter(vertex -> ExecutionState.RUNNING.equals(vertex.getExecutionState()))
                .collect(Collectors.toList());
    }

    private static HazelcastInstanceImpl findWorker(
            Address address, HazelcastInstanceImpl... workers) {
        return Stream.of(workers)
                .filter(worker -> worker.getCluster().getLocalMember().getAddress().equals(address))
                .findFirst()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No worker node owns the address of the held task: "
                                                + address));
    }

    private static void shutdownQuietly(HazelcastInstanceImpl node) {
        if (node != null) {
            node.shutdown();
        }
    }

    /**
     * Log4j2 appender attached to the engine server loggers for the duration of the test, so the
     * assertion can prove that the restore did not log the NullPointerException of
     * apache/seatunnel#11563 (the master logs it from {@code SubPlan#restorePipeline} as "Restore
     * pipeline ... error with exception" together with the stack trace).
     */
    private static final class CapturedEngineLogs extends AbstractAppender {

        /** Logger name every relevant engine-server class logs under, used as the attach point. */
        private static final String ENGINE_SERVER_LOGGER = "org.apache.seatunnel.engine.server";

        /** Formatted log lines captured while this appender is attached. */
        private final List<String> events = new CopyOnWriteArrayList<>();

        /** The logger config this appender was registered on, needed again to detach. */
        private LoggerConfig loggerConfig;

        /**
         * Whether {@link #attach(String)} had to create {@link #loggerConfig} itself, so {@link
         * #detach()} knows whether to remove it again instead of only unregistering the appender.
         */
        private boolean loggerConfigCreated;

        private CapturedEngineLogs(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY);
        }

        static CapturedEngineLogs attach(String name) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = context.getConfiguration();
            CapturedEngineLogs appender = new CapturedEngineLogs("captured-engine-logs-" + name);
            appender.start();
            configuration.addAppender(appender);
            LoggerConfig existing = configuration.getLoggerConfig(ENGINE_SERVER_LOGGER);
            if (ENGINE_SERVER_LOGGER.equals(existing.getName())) {
                appender.loggerConfig = existing;
            } else {
                appender.loggerConfig = new LoggerConfig(ENGINE_SERVER_LOGGER, Level.WARN, true);
                configuration.addLogger(ENGINE_SERVER_LOGGER, appender.loggerConfig);
                appender.loggerConfigCreated = true;
            }
            appender.loggerConfig.addAppender(appender, Level.WARN, null);
            context.updateLoggers();
            return appender;
        }

        void detach() {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            loggerConfig.removeAppender(getName());
            if (loggerConfigCreated) {
                context.getConfiguration().removeLogger(ENGINE_SERVER_LOGGER);
            }
            context.updateLoggers();
            stop();
        }

        @Override
        public void append(LogEvent event) {
            StringBuilder text =
                    new StringBuilder(event.getLoggerName())
                            .append(" - ")
                            .append(event.getMessage().getFormattedMessage());
            if (event.getThrown() != null) {
                text.append(System.lineSeparator())
                        .append(ExceptionUtils.getMessage(event.getThrown()));
            }
            events.add(text.toString());
        }

        /** Returns the captured events that contain every one of the given fragments. */
        List<String> eventsContaining(String... fragments) {
            return events.stream()
                    .filter(event -> Stream.of(fragments).allMatch(event::contains))
                    .collect(Collectors.toList());
        }
    }
}
