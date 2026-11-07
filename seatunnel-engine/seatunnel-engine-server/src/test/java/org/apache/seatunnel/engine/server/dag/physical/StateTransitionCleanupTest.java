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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.engine.common.config.server.QueueType.BLOCKINGQUEUE;
import static org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService.SKIP_CHECK_JAR;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SetEnvironmentVariable(key = SKIP_CHECK_JAR, value = "true")
class StateTransitionCleanupTest extends AbstractSeaTunnelServerTest {

    /**
     * A terminal callback can arrive after reset while the vertex is stopped. Resuming pipeline
     * cancellation must complete the new future even though no further task transition is needed.
     */
    @ParameterizedTest
    @CsvSource({
        "FAILED, true",
        "CANCELED, true",
        "FINISHED, true",
        "FAILED, false",
        "CANCELED, false",
        "FINISHED, false"
    })
    void testRestoredCancellationCompletesLateTerminalState(
            ExecutionState terminalState, boolean callbackBeforeInit) throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);
        PhysicalVertex vertex =
                planWithStateMaps
                        .physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0);

        vertex.startPhysicalVertex();
        vertex.updateTaskState(ExecutionState.CANCELED);
        vertex.reset();
        String error =
                terminalState == ExecutionState.FAILED ? "Paimon SELECT permission denied" : null;
        TaskExecutionState lateResult =
                new TaskExecutionState(vertex.getTaskGroupLocation(), terminalState, error);
        if (callbackBeforeInit) {
            vertex.updateStateByExecutionService(lateResult);
        }
        PassiveCompletableFuture<TaskExecutionState> restoredFuture = vertex.initStateFuture();

        if (!callbackBeforeInit) {
            vertex.updateStateByExecutionService(lateResult);
        }
        Assertions.assertFalse(restoredFuture.isDone());

        vertex.startPhysicalVertex();
        vertex.cancel();

        Assertions.assertTrue(
                restoredFuture.isDone(),
                "Resumed cancellation must publish the terminal task result");
        TaskExecutionState result = restoredFuture.get();
        Assertions.assertEquals(terminalState, result.getExecutionState());
        Assertions.assertEquals(error, result.getThrowableMsg());
        Assertions.assertEquals(
                terminalState,
                planWithStateMaps.runningJobState.get(vertex.getTaskGroupLocation()));
    }

    @Test
    void testPhysicalVertexIgnoresLateTransitionWhenTaskStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        PhysicalVertex physicalVertex =
                planWithStateMaps
                        .physicalPlan
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0);

        planWithStateMaps.runningJobState.put(
                physicalVertex.getTaskGroupLocation(), ExecutionState.FAILED);

        physicalVertex.updateTaskState(ExecutionState.CANCELING);

        Assertions.assertEquals(
                ExecutionState.FAILED,
                planWithStateMaps.runningJobState.get(physicalVertex.getTaskGroupLocation()));
    }

    @Test
    void testSubPlanIgnoresLateTransitionWhenPipelineStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        PipelineLocation pipelineLocation = subPlan.getPipelineLocation();

        planWithStateMaps.runningJobState.put(pipelineLocation, PipelineStatus.FAILED);

        subPlan.updatePipelineState(PipelineStatus.CANCELING);

        Assertions.assertEquals(
                PipelineStatus.FAILED, planWithStateMaps.runningJobState.get(pipelineLocation));
    }

    @Test
    void testPhysicalPlanIgnoresLateTransitionWhenJobStateAlreadyTerminal() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        planWithStateMaps.runningJobState.put(jobId, JobStatus.FAILED);

        planWithStateMaps.physicalPlan.updateJobState(JobStatus.CANCELING);

        Assertions.assertEquals(JobStatus.FAILED, planWithStateMaps.runningJobState.get(jobId));
    }

    @Test
    void testPipelineRestoreClearsMetricsFromTheFailedAttempt() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);
        JobConfig jobConfig = mock(JobConfig.class);
        Map<String, Object> envOptions = new HashMap<>();
        envOptions.put("job.retry.times", "1");
        envOptions.put("job.retry.interval.seconds", "0");
        when(jobImmutableInformation.getJobId()).thenReturn(jobId);
        when(jobImmutableInformation.getJobConfig()).thenReturn(jobConfig);
        when(jobConfig.getEnvOptions()).thenReturn(envOptions);

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("restoreMetricsState-" + jobId);
        IMap<Object, Long[]> runningJobStateTimestamps =
                nodeEngine.getHazelcastInstance().getMap("restoreMetricsStateTimestamps-" + jobId);
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        runningJobState.put(pipelineLocation, PipelineStatus.FAILED);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            SubPlan subPlan =
                    new SubPlan(
                            1,
                            1,
                            System.currentTimeMillis(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            jobImmutableInformation,
                            executorService,
                            runningJobState,
                            runningJobStateTimestamps,
                            Collections.emptyMap());
            JobMaster jobMaster = mock(JobMaster.class);
            CheckpointManager checkpointManager = mock(CheckpointManager.class);
            PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
            when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);
            when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);
            subPlan.setJobMaster(jobMaster);

            Method restore = SubPlan.class.getDeclaredMethod("prepareRestorePipeline");
            restore.setAccessible(true);
            restore.invoke(subPlan);

            verify(jobMaster).clearPipelineMetricsForRestore(pipelineLocation);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * The restore clear must clean the failed attempt's task-group contexts before it removes the
     * pipeline's metrics: a context that survives the wipe re-publishes the old counters during the
     * next worker report and defeats the reset.
     */
    @Test
    void testRestoreClearRunsTaskGroupCleanupBeforeMetricsRemoval() {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        Map<TaskGroupLocation, SlotProfile> slotProfiles = newOwnedSlotProfiles(jobId);
        @SuppressWarnings("unchecked")
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                mock(IMap.class);
        when(ownedSlotProfilesIMap.get(pipelineLocation)).thenReturn(slotProfiles);
        SeaTunnelServer seaTunnelServer = mock(SeaTunnelServer.class);

        JobMaster jobMaster = newJobMaster(jobId, ownedSlotProfilesIMap, seaTunnelServer);
        jobMaster.clearPipelineMetricsForRestore(pipelineLocation);

        InOrder inOrder = inOrder(ownedSlotProfilesIMap, seaTunnelServer);
        inOrder.verify(ownedSlotProfilesIMap).get(pipelineLocation);
        inOrder.verify(seaTunnelServer).removeMetrics(pipelineLocation);
    }

    /** The restore clear must remove the failed attempt's snapshots from the real state store. */
    @Test
    void testRestoreClearRemovesMetricsFromTheStateStore() {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineLocation otherPipelineLocation = new PipelineLocation(jobId, 2);
        upsertMetricsForPipeline(pipelineLocation);
        upsertMetricsForPipeline(otherPipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));
        Assertions.assertTrue(hasMetricsForPipeline(otherPipelineLocation));

        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap("restoreOwnedSlotProfiles-" + jobId);
        ownedSlotProfilesIMap.put(pipelineLocation, newOwnedSlotProfiles(jobId));

        JobMaster jobMaster = newJobMaster(jobId, ownedSlotProfilesIMap, server);
        jobMaster.clearPipelineMetricsForRestore(pipelineLocation);

        Assertions.assertFalse(
                hasMetricsForPipeline(pipelineLocation),
                "Restoring a pipeline must not keep the failed attempt's metrics");
        Assertions.assertTrue(
                hasMetricsForPipeline(otherPipelineLocation),
                "The clear must not touch other pipelines of the same job");
    }

    @Test
    void testMetricsCleanupFailureDoesNotPreventRestore() {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PipelineLocation location = new PipelineLocation(jobId, 1);
        SeaTunnelServer failingServer = mock(SeaTunnelServer.class);
        doThrow(new IllegalStateException("metrics map unavailable"))
                .when(failingServer)
                .removeMetrics(location);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> emptyOwnedSlotProfiles =
                nodeEngine.getHazelcastInstance().getMap("restoreOwnedSlotProfilesEmpty-" + jobId);
        JobMaster jobMaster = newJobMaster(jobId, emptyOwnedSlotProfiles, failingServer);

        Assertions.assertDoesNotThrow(() -> jobMaster.clearPipelineMetricsForRestore(location));
        verify(failingServer).removeMetrics(location);
    }

    private Map<TaskGroupLocation, SlotProfile> newOwnedSlotProfiles(long jobId) {
        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        Map<TaskGroupLocation, SlotProfile> slotProfiles = new HashMap<>();
        slotProfiles.put(
                new TaskGroupLocation(jobId, 1, 1L),
                new SlotProfile(localAddress, 0, null, "restore-clear-test"));
        return slotProfiles;
    }

    /**
     * Creates a JobMaster with only the pieces {@code clearPipelineMetricsForRestore} touches: a
     * real node engine backed by the test cluster and the given owned-slot-profiles map and server.
     * The remaining constructor arguments are never used by this code path.
     */
    private JobMaster newJobMaster(
            long jobId,
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap,
            SeaTunnelServer seaTunnelServer) {
        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("restoreJobMasterState-" + jobId);
        IMap<Object, Object> runningJobStateTimestamps =
                nodeEngine.getHazelcastInstance().getMap("restoreJobMasterStateTs-" + jobId);
        IMap<Long, JobInfo> runningJobInfo =
                nodeEngine.getHazelcastInstance().getMap("restoreJobMasterInfo-" + jobId);
        return new JobMaster(
                jobId,
                nodeEngine.getSerializationService().toData("unused"),
                nodeEngine,
                mock(ExecutorService.class),
                mock(ResourceManager.class),
                mock(JobHistoryService.class),
                runningJobState,
                runningJobStateTimestamps,
                ownedSlotProfilesIMap,
                runningJobInfo,
                new EngineConfig(),
                seaTunnelServer);
    }

    private void upsertMetricsForPipeline(PipelineLocation pipelineLocation) {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(
                        pipelineLocation.getJobId(), pipelineLocation.getPipelineId(), 1L);
        TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);

        Map<TaskLocation, SeaTunnelMetricsContext> local = new HashMap<>();
        local.put(taskLocation, new SeaTunnelMetricsContext());
        server.updateMetrics(local);
    }

    private boolean hasMetricsForPipeline(PipelineLocation pipelineLocation) {
        MetricsSnapshotStateStore metricsStore =
                server.getEngineContext().getStateStores().metricsSnapshotStore();
        return metricsStore.containsPipeline(pipelineLocation);
    }

    private PlanWithStateMaps createPhysicalPlan(long jobId) throws MalformedURLException {
        JobContext jobContext = new JobContext(jobId);
        jobContext.setJobMode(JobMode.BATCH);
        JobConfig config = new JobConfig();
        config.setName("cleanup-test");
        config.setJobContext(jobContext);
        LogicalDag logicalDag = TestUtils.getTestLogicalDag(jobContext, config);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "CleanupTest",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        IMap<Object, Object> runningJobState =
                nodeEngine.getHazelcastInstance().getMap("cleanupRunningJobState-" + jobId);
        IMap<Object, Long[]> runningJobStateTimestamps =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("cleanupRunningJobStateTimestamps-" + jobId);

        PhysicalPlan physicalPlan =
                PlanUtils.fromLogicalDAG(
                                logicalDag,
                                nodeEngine,
                                jobImmutableInformation,
                                System.currentTimeMillis(),
                                Executors.newCachedThreadPool(),
                                server.getClassLoaderService(),
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME),
                                runningJobState,
                                runningJobStateTimestamps,
                                BLOCKINGQUEUE,
                                new EngineConfig())
                        .f0();

        return new PlanWithStateMaps(physicalPlan, runningJobState, runningJobStateTimestamps);
    }

    private static final class PlanWithStateMaps {
        private final PhysicalPlan physicalPlan;
        private final IMap<Object, Object> runningJobState;
        private final IMap<Object, Long[]> runningJobStateTimestamps;

        private PlanWithStateMaps(
                PhysicalPlan physicalPlan,
                IMap<Object, Object> runningJobState,
                IMap<Object, Long[]> runningJobStateTimestamps) {
            this.physicalPlan = physicalPlan;
            this.runningJobState = runningJobState;
            this.runningJobStateTimestamps = runningJobStateTimestamps;
        }
    }

    @Test
    void testWorkerFailureDuringCancelKeepsPipelineCanceling() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        subPlan.initStateFuture();
        subPlan.updatePipelineState(PipelineStatus.CANCELING);

        // A vertex the cancel loop has not reached yet (still CREATED) is reported FAILED
        // by CoordinatorService.makeTasksFailed() once the worker is lost. This drives the
        // real callback path in SubPlan.addPhysicalVertexCallBack().
        PhysicalVertex workerLostVertex = subPlan.getPhysicalVertexList().get(0);
        workerLostVertex.startPhysicalVertex();
        workerLostVertex.updateStateByExecutionService(
                new TaskExecutionState(
                        workerLostVertex.getTaskGroupLocation(),
                        ExecutionState.FAILED,
                        "worker lost"));

        await().atMost(Duration.ofSeconds(10)).until(() -> subPlan.getFinishedTaskNum().get() == 1);
        // Moving the pipeline to FAILING here is exactly the race that made a
        // user-requested cancel end up FAILED (#12353).
        Assertions.assertEquals(PipelineStatus.CANCELING, subPlan.getPipelineState());
    }

    @Test
    void testWorkerFailureDuringCancelResolvesToCanceled() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        subPlan.setJobMaster(mockJobMaster());

        subPlan.initStateFuture();
        subPlan.updatePipelineState(PipelineStatus.CANCELING);

        // One task fails with the lost worker; every other task (including the
        // coordinator vertices) is canceled.
        List<PhysicalVertex> allVertices = new ArrayList<>(subPlan.getPhysicalVertexList());
        allVertices.addAll(subPlan.getCoordinatorVertexList());
        for (PhysicalVertex vertex : allVertices) {
            vertex.startPhysicalVertex();
        }
        allVertices
                .get(0)
                .updateStateByExecutionService(
                        new TaskExecutionState(
                                allVertices.get(0).getTaskGroupLocation(),
                                ExecutionState.FAILED,
                                "worker lost"));
        for (int i = 1; i < allVertices.size(); i++) {
            allVertices
                    .get(i)
                    .updateStateByExecutionService(
                            new TaskExecutionState(
                                    allVertices.get(i).getTaskGroupLocation(),
                                    ExecutionState.CANCELED));
        }

        await().atMost(Duration.ofSeconds(10))
                .until(() -> PipelineStatus.CANCELED.equals(subPlan.getPipelineState()));
    }

    @Test
    void testAllTasksFinishedNaturallyDuringCancelResolvesToFinished() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);

        SubPlan subPlan = planWithStateMaps.physicalPlan.getPipelineList().get(0);
        subPlan.setJobMaster(mockJobMaster());

        subPlan.initStateFuture();
        subPlan.updatePipelineState(PipelineStatus.CANCELING);

        // Every task (including the coordinator vertices) completes FINISHED while the
        // pipeline state has not left CANCELING yet: the pipeline must still end FINISHED,
        // not be reclassified as CANCELED.
        List<PhysicalVertex> allVertices = new ArrayList<>(subPlan.getPhysicalVertexList());
        allVertices.addAll(subPlan.getCoordinatorVertexList());
        for (PhysicalVertex vertex : allVertices) {
            vertex.startPhysicalVertex();
            vertex.updateStateByExecutionService(
                    new TaskExecutionState(vertex.getTaskGroupLocation(), ExecutionState.FINISHED));
        }

        await().atMost(Duration.ofSeconds(10))
                .until(() -> PipelineStatus.FINISHED.equals(subPlan.getPipelineState()));
    }

    /**
     * Sibling-vertex ordering (#12353): the job-level cancel loop processes pipelines one by one
     * and is still cancelling the checkpoint coordinator of the first pipeline when the worker
     * hosting every vertex of a later pipeline is removed. {@code
     * CoordinatorService.makeTasksFailed()} completes the vertices of that pipeline — including its
     * coordinator vertex, also deployed on the crashed member — as {@code FAILED} before the cancel
     * loop has ever reached the pipeline, so the pipeline state has not become {@code CANCELING}
     * yet. The pipeline must still end {@code CANCELED}: the user-requested cancel must not be
     * overwritten to {@code FAILED}.
     */
    @Test
    void testSiblingWorkerFailureWhileCancelInFlightResolvesToCanceled() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        PlanWithStateMaps planWithStateMaps = createPhysicalPlan(jobId);
        PhysicalPlan physicalPlan = planWithStateMaps.physicalPlan;

        // A second pipeline the job-level cancel loop has not reached while it is still stuck
        // cancelling the first pipeline's checkpoint coordinator.
        SubPlan notYetReachedPipeline =
                createPhysicalPlan(
                                instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                                        .newId())
                        .physicalPlan
                        .getPipelineList()
                        .get(0);
        physicalPlan.getPipelineList().add(notYetReachedPipeline);

        // The first pipeline's checkpoint cancellation blocks the job-level cancel loop.
        CompletableFuture<CheckpointCoordinatorState> firstPipelineCancelFuture =
                new CompletableFuture<>();
        CountDownLatch cancelLoopBlockedOnFirstPipeline = new CountDownLatch(1);
        physicalPlan.setJobMaster(
                mockJobMaster(
                        invocation -> {
                            cancelLoopBlockedOnFirstPipeline.countDown();
                            return new PassiveCompletableFuture<>(firstPipelineCancelFuture);
                        }));
        // The not-yet-reached pipeline must not block while computing its end state.
        notYetReachedPipeline.setJobMaster(mockJobMaster());

        physicalPlan.initStateFuture();
        // Move every pipeline to RUNNING before starting the job state machine, so the
        // SCHEDULED branch of PhysicalPlan.stateProcess() skips them (no resource
        // application) and cancelJob() walks the CANCELING branch.
        for (SubPlan subPlan : physicalPlan.getPipelineList()) {
            subPlan.updatePipelineState(PipelineStatus.RUNNING);
        }
        physicalPlan.startJob();

        Thread cancelJobThread = new Thread(physicalPlan::cancelJob);
        cancelJobThread.start();
        Assertions.assertTrue(
                cancelLoopBlockedOnFirstPipeline.await(10, TimeUnit.SECONDS),
                "the job-level cancel loop must reach the first pipeline");

        // The crashed worker hosted every vertex of the not-yet-reached pipeline (task
        // vertices and coordinator vertex, all still RUNNING when the member is removed).
        List<PhysicalVertex> crashedVertices =
                new ArrayList<>(notYetReachedPipeline.getPhysicalVertexList());
        crashedVertices.addAll(notYetReachedPipeline.getCoordinatorVertexList());
        Assertions.assertTrue(
                crashedVertices.size() >= 2,
                "the pipeline must host at least two vertices on the crashed member");
        for (PhysicalVertex vertex : crashedVertices) {
            vertex.startPhysicalVertex();
            vertex.updateStateByExecutionService(
                    new TaskExecutionState(
                            vertex.getTaskGroupLocation(), ExecutionState.FAILED, "worker lost"));
        }

        await().atMost(Duration.ofSeconds(10))
                .until(() -> notYetReachedPipeline.getPipelineState().isEndState());
        Assertions.assertEquals(PipelineStatus.CANCELED, notYetReachedPipeline.getPipelineState());

        // Let the blocked job-level cancel loop finish.
        firstPipelineCancelFuture.complete(
                new CheckpointCoordinatorState(CheckpointCoordinatorStatus.FINISHED, null));
        cancelJobThread.join(5000);
    }

    private JobMaster mockJobMaster() {
        return mockJobMaster(
                invocation ->
                        new PassiveCompletableFuture<>(
                                CompletableFuture.completedFuture(
                                        new CheckpointCoordinatorState(
                                                CheckpointCoordinatorStatus.FINISHED, null))));
    }

    private JobMaster mockJobMaster(
            Answer<PassiveCompletableFuture<CheckpointCoordinatorState>> cancelCheckpointAnswer) {
        CheckpointManager checkpointManager = Mockito.mock(CheckpointManager.class);
        Mockito.when(checkpointManager.cancelCheckpoint(Mockito.anyInt()))
                .thenAnswer(cancelCheckpointAnswer);
        Mockito.when(checkpointManager.waitCheckpointCoordinatorComplete(Mockito.anyInt()))
                .thenReturn(
                        new PassiveCompletableFuture<>(
                                CompletableFuture.completedFuture(
                                        new CheckpointCoordinatorState(
                                                CheckpointCoordinatorStatus.FINISHED, null))));
        JobMaster jobMaster = Mockito.mock(JobMaster.class);
        Mockito.when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);
        Mockito.when(jobMaster.getExecutorService()).thenReturn(Executors.newCachedThreadPool());
        return jobMaster;
    }
}
