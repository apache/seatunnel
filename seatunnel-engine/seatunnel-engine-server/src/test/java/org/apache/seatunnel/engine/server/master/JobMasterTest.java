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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.dag.physical.UnknownPhysicalPlanException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.task.CoordinatorTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** JobMaster Tester. */
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JobMasterTest extends AbstractSeaTunnelServerTest {
    /**
     * IMap key is jobId and value is a Tuple2 Tuple2 key is JobMaster init timestamp and value is
     * the jobImmutableInformation which is sent by client when submit job
     *
     * <p>This IMap is used to recovery runningJobInfoIMap in JobMaster when a new master node
     * active
     */
    private IMap<Long, JobInfo> runningJobInfoIMap;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node
     * active
     */
    IMap<Object, Object> runningJobStateIMap;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.SubPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex} stateTimestamps
     *
     * <p>This IMap is used to recovery runningJobStateTimestampsIMap in JobMaster when a new master
     * node active
     */
    IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * IMap key is {@link PipelineLocation}
     *
     * <p>The value of IMap is map of {@link TaskGroupLocation} and the {@link SlotProfile} it used.
     *
     * <p>This IMap is used to recovery ownedSlotProfilesIMap in JobMaster when a new master node
     * active
     */
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private final ExecutorService jobMasterTestExecutor = Executors.newCachedThreadPool();

    @BeforeAll
    public void before() {
        super.before();
    }

    @Test
    public void testHandleCheckpointTimeout() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster = newJobInstanceWithRunningState(jobId);

        jobMaster.neverNeedRestore();
        // call checkpoint timeout
        jobMaster.handleCheckpointError(1, false);

        PassiveCompletableFuture<JobResult> jobMasterCompleteFuture =
                jobMaster.getJobMasterCompleteFuture();

        // test job turn to complete
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                // Why equals CANCELED or FAILED? because handleCheckpointError
                                // should call by CheckpointCoordinator,
                                // before do this, CheckpointCoordinator should be failed. Anyway,
                                // use handleCheckpointError not good to test checkpoint timeout.
                                Assertions.assertTrue(
                                        jobMasterCompleteFuture.isDone()
                                                && (JobStatus.CANCELED.equals(
                                                                jobMasterCompleteFuture
                                                                        .get()
                                                                        .getStatus())
                                                        || JobStatus.FAILED.equals(
                                                                jobMasterCompleteFuture
                                                                        .get()
                                                                        .getStatus()))));

        testIMapRemovedAfterJobComplete(jobId, jobMaster);
    }

    private void testIMapRemovedAfterJobComplete(long jobId, JobMaster jobMaster) {
        runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap("runningJobInfo");
        runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap("runningJobState");
        runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap("stateTimestamps");
        ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(runningJobInfoIMap.get(jobId));
                            Assertions.assertNull(runningJobStateIMap.get(jobId));
                            Assertions.assertNull(runningJobStateTimestampsIMap.get(jobId));
                            Assertions.assertNull(ownedSlotProfilesIMap.get(jobId));

                            jobMaster
                                    .getPhysicalPlan()
                                    .getPipelineList()
                                    .forEach(
                                            pipeline -> {
                                                Assertions.assertNull(
                                                        runningJobStateIMap.get(
                                                                pipeline.getPipelineLocation()));

                                                Assertions.assertNull(
                                                        runningJobStateTimestampsIMap.get(
                                                                pipeline.getPipelineLocation()));
                                            });
                            jobMaster
                                    .getPhysicalPlan()
                                    .getPipelineList()
                                    .forEach(
                                            pipeline -> {
                                                pipeline.getCoordinatorVertexList()
                                                        .forEach(
                                                                coordinator -> {
                                                                    Assertions.assertNull(
                                                                            runningJobStateIMap.get(
                                                                                    coordinator
                                                                                            .getTaskGroupLocation()));

                                                                    Assertions.assertNull(
                                                                            runningJobStateTimestampsIMap
                                                                                    .get(
                                                                                            coordinator
                                                                                                    .getTaskGroupLocation()));
                                                                });

                                                pipeline.getPhysicalVertexList()
                                                        .forEach(
                                                                task -> {
                                                                    Assertions.assertNull(
                                                                            runningJobStateIMap.get(
                                                                                    task
                                                                                            .getTaskGroupLocation()));

                                                                    Assertions.assertNull(
                                                                            runningJobStateTimestampsIMap
                                                                                    .get(
                                                                                            task
                                                                                                    .getTaskGroupLocation()));
                                                                });
                                            });
                        });
    }

    @Test
    public void testCommitFailedWillRestore() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster = newJobInstanceWithRunningState(jobId);

        // call checkpoint timeout
        jobMaster
                .getCheckpointManager()
                .getCheckpointCoordinator(1)
                .handleCoordinatorError(
                        "commit failed",
                        new RuntimeException(),
                        CheckpointCloseReason.AGGREGATE_COMMIT_ERROR);
        Assertions.assertTrue(jobMaster.isNeedRestore());
    }

    @Test
    public void testCloseIdleTask() throws InterruptedException {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster = newJobInstanceWithRunningState(jobId);
        Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus());

        assertCloseIdleTask(jobMaster);

        server.getCoordinatorService().savePoint(jobId);
        server.getCoordinatorService().getJobStatus(jobId);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus jobStatus =
                                    server.getCoordinatorService().getJobStatus(jobId);
                            Assertions.assertEquals(JobStatus.SAVEPOINT_DONE, jobStatus);
                        });
        jobMaster = newJobInstanceWithRunningState(jobId, true);
        Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus());

        assertCloseIdleTask(jobMaster);
    }

    @Test
    void testFilteringFinishedPipelinesInPhysicalPlanGenerator() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster = newJobInstanceWithRunningState(jobId);

        jobMaster
                .getRunningJobStateIMap()
                .put(new PipelineLocation(jobId, 1), PipelineStatus.FINISHED);
        Assertions.assertThrows(
                UnknownPhysicalPlanException.class,
                () -> jobMaster.init(System.currentTimeMillis(), false));
    }

    @Test
    void testFailedPipelineCleanupEnqueuesRecordAndRemovesMetrics() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster = newJobInstanceWithRunningState(jobId);
        PipelineLocation pipelineLocation = getRunningPipelineLocation(jobMaster);

        upsertMetricsForPipeline(pipelineLocation);
        Assertions.assertTrue(hasMetricsForPipeline(pipelineLocation));

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);

        try {
            jobMaster.enqueuePipelineCleanupIfNeeded(pipelineLocation, PipelineStatus.FAILED);

            PipelineCleanupRecord cleanupRecord = pendingCleanupIMap.get(pipelineLocation);
            Assertions.assertNotNull(cleanupRecord);
            Assertions.assertEquals(PipelineStatus.FAILED, cleanupRecord.getFinalStatus());
            Assertions.assertFalse(cleanupRecord.isSavepointEnd());

            jobMaster.removeMetricsContext(pipelineLocation, PipelineStatus.FAILED);

            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertFalse(hasMetricsForPipeline(pipelineLocation)));
        } finally {
            server.getCoordinatorService().cancelJob(jobId).join();
            testIMapRemovedAfterJobComplete(jobId, jobMaster);
        }
    }

    @Test
    void testFinalMetricsFailureDoesNotPersistPartialHistory() throws Exception {
        long jobId = 10001L;
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        Address workerAddress = new Address("127.0.0.2", 5801);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        ClusterService clusterService = mock(ClusterService.class);
        MemberImpl workerMember = mock(MemberImpl.class);
        FlakeIdGenerator flakeIdGenerator = mock(FlakeIdGenerator.class);
        ResourceManager resourceManager = mock(ResourceManager.class);
        JobHistoryService jobHistoryService = mock(JobHistoryService.class);
        IMap<Object, Object> runningJobStateIMap = mock(IMap.class);
        IMap<Object, Object> runningJobStateTimestampsIMap = mock(IMap.class);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                mock(IMap.class);
        IMap<Long, JobInfo> runningJobInfoIMap = mock(IMap.class);
        SlotProfile slotProfile = mock(SlotProfile.class);
        JobImmutableInformation jobInformation = mock(JobImmutableInformation.class);

        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        when(hazelcastInstance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME))
                .thenReturn(flakeIdGenerator);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getMember(workerAddress)).thenReturn(workerMember);
        when(slotProfile.getWorker()).thenReturn(workerAddress);
        when(jobInformation.getJobId()).thenReturn(jobId);
        doAnswer(
                        invocation -> {
                            BiConsumer<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
                                    consumer = invocation.getArgument(0);
                            consumer.accept(
                                    pipelineLocation,
                                    Collections.singletonMap(taskGroupLocation, slotProfile));
                            return null;
                        })
                .when(ownedSlotProfilesIMap)
                .forEach((BiConsumer<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>) any());

        JobMaster jobMaster =
                new JobMaster(
                        jobId,
                        mock(Data.class),
                        nodeEngine,
                        jobMasterTestExecutor,
                        resourceManager,
                        jobHistoryService,
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        ownedSlotProfilesIMap,
                        runningJobInfoIMap,
                        null,
                        null) {
                    @Override
                    protected RawJobMetrics fetchTaskGroupMetrics(
                            Address address, List<TaskGroupLocation> taskGroupLocations)
                            throws Exception {
                        throw new TimeoutException("worker metrics timed out");
                    }
                };
        Field jobInformationField = JobMaster.class.getDeclaredField("jobImmutableInformation");
        jobInformationField.setAccessible(true);
        jobInformationField.set(jobMaster, jobInformation);

        Assertions.assertThrows(
                SeaTunnelEngineException.class,
                () -> jobMaster.savePipelineMetricsToHistory(pipelineLocation));
        verifyNoInteractions(jobHistoryService);
    }

    @Test
    void testRealtimeMetricsStopsAfterInterrupt() throws Exception {
        long jobId = 10002L;
        TaskGroupLocation firstTaskGroup = new TaskGroupLocation(jobId, 1, 1L);
        TaskGroupLocation secondTaskGroup = new TaskGroupLocation(jobId, 1, 2L);
        Address firstWorker = new Address("127.0.0.2", 5801);
        Address secondWorker = new Address("127.0.0.3", 5801);
        AtomicInteger fetchCount = new AtomicInteger();
        NodeEngine nodeEngine = mock(NodeEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getMember(firstWorker)).thenReturn(mock(MemberImpl.class));
        when(clusterService.getMember(secondWorker)).thenReturn(mock(MemberImpl.class));

        JobMaster jobMaster =
                new JobMaster(
                        jobId,
                        mock(Data.class),
                        nodeEngine,
                        jobMasterTestExecutor,
                        mock(ResourceManager.class),
                        mock(JobHistoryService.class),
                        mock(IMap.class),
                        mock(IMap.class),
                        mock(IMap.class),
                        mock(IMap.class),
                        null,
                        null) {
                    @Override
                    protected RawJobMetrics fetchTaskGroupMetrics(
                            Address address, List<TaskGroupLocation> taskGroupLocations)
                            throws Exception {
                        fetchCount.incrementAndGet();
                        throw new InterruptedException("test interrupt");
                    }
                };

        try {
            Map<TaskGroupLocation, Address> workers = new HashMap<>();
            workers.put(firstTaskGroup, firstWorker);
            workers.put(secondTaskGroup, secondWorker);
            Assertions.assertTrue(jobMaster.getCurrJobMetrics(workers).isEmpty());
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            Assertions.assertEquals(1, fetchCount.get());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testRestoreStartWithSavePointKeepsFinishedPipelines() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        JobMaster jobMaster =
                newJobMaster(
                        jobId,
                        "batch_fake_to_console.conf",
                        "test_restore_savepoint_pipeline",
                        true);

        runningJobStateIMap.put(new PipelineLocation(jobId, 1), PipelineStatus.FINISHED);

        Assertions.assertDoesNotThrow(() -> jobMaster.init(System.currentTimeMillis(), true));
        Assertions.assertEquals(1, jobMaster.getPhysicalPlan().getPipelineList().size());
    }

    @Test
    void testJobCheckpointConfigUsesJobLevelRetainAfterCancelledOverride() throws Exception {
        long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        Map<String, Object> envOptions = new HashMap<>();
        envOptions.put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), 10000L);
        envOptions.put(EnvCommonOptions.CHECKPOINT_RETAIN_AFTER_JOB_CANCELLED.key(), true);

        JobMaster jobMaster =
                newJobMaster(
                        jobId,
                        "stream_fakesource_to_file.conf",
                        "test_job_checkpoint_config_retain_override",
                        false,
                        envOptions);

        jobMaster.init(System.currentTimeMillis(), false);

        CheckpointConfig jobCheckpointConfig =
                ReflectionUtils.getField(jobMaster, "jobCheckpointConfig")
                        .map(CheckpointConfig.class::cast)
                        .orElse(null);
        Assertions.assertNotNull(jobCheckpointConfig);
        Assertions.assertTrue(
                jobCheckpointConfig.isRetainAfterJobCancelled(),
                "job-level env option should override retain-after-job-cancelled");
    }

    private void assertCloseIdleTask(JobMaster jobMaster) {
        SlotService slotService = server.getSlotService();
        long jobId = jobMaster.getJobId();
        // Savepoint restore can overlap old slot release with new task scheduling for a short time.
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> getAssignedSlotCount(slotService, jobId) == 4);

        Assertions.assertEquals(1, jobMaster.getPhysicalPlan().getPipelineList().size());
        SubPlan subPlan = jobMaster.getPhysicalPlan().getPipelineList().get(0);
        try {
            PhysicalVertex coordinatorVertex1 = subPlan.getCoordinatorVertexList().get(0);
            CoordinatorTask coordinatorTask =
                    (CoordinatorTask)
                            coordinatorVertex1.getTaskGroup().getTasks().stream().findFirst().get();
            jobMaster
                    .getCheckpointManager()
                    .readyToCloseIdleTask(coordinatorTask.getTaskLocation());
            Assertions.fail("should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // ignore
        }

        Assertions.assertEquals(2, subPlan.getPhysicalVertexList().size());
        PhysicalVertex taskGroup1 = subPlan.getPhysicalVertexList().get(0);
        Assertions.assertEquals(3, taskGroup1.getTaskGroup().getTasks().size());
        SeaTunnelTask seaTunnelTask =
                (SeaTunnelTask) taskGroup1.getTaskGroup().getTasks().stream().findFirst().get();
        jobMaster.getCheckpointManager().readyToCloseIdleTask(seaTunnelTask.getTaskLocation());
        int expectedClosedIdleTaskSize = taskGroup1.getTaskGroup().getTasks().size();

        CheckpointCoordinator checkpointCoordinator =
                jobMaster
                        .getCheckpointManager()
                        .getCheckpointCoordinator(seaTunnelTask.getTaskLocation().getPipelineId());
        await().atMost(60, TimeUnit.SECONDS)
                .until(
                        () ->
                                checkpointCoordinator.getClosedIdleTask().size()
                                        >= expectedClosedIdleTaskSize);
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> getAssignedSlotCount(slotService, jobId) == 3);
    }

    private long getAssignedSlotCount(SlotService slotService, long jobId) {
        return Arrays.stream(slotService.getWorkerProfile().getAssignedSlots())
                .filter(slotProfile -> slotProfile.getOwnerJobID() == jobId)
                .count();
    }

    private JobMaster newJobInstanceWithRunningState(long jobId) throws InterruptedException {
        return newJobInstanceWithRunningState(jobId, false);
    }

    private JobMaster newJobInstanceWithRunningState(long jobId, boolean restore)
            throws InterruptedException {
        LogicalDag testLogicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fakesource_to_file.conf", "test_clear_coordinator_service", jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        restore,
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);

        // waiting for job status turn to running
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, jobMaster.getJobStatus()));

        // Because handleCheckpointTimeout is an async method, so we need sleep 5s to waiting job
        // status become running again
        Thread.sleep(5000);
        return jobMaster;
    }

    private JobMaster newJobMaster(long jobId, String configFile, String jobName, boolean restore) {
        return newJobMaster(jobId, configFile, jobName, restore, Collections.emptyMap());
    }

    private JobMaster newJobMaster(
            long jobId,
            String configFile,
            String jobName,
            boolean restore,
            Map<String, Object> envOptions) {
        runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap("runningJobInfo");
        runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap("runningJobState");
        runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap("stateTimestamps");
        ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");

        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(configFile, jobName, jobId);
        testLogicalDag.getJobConfig().getEnvOptions().putAll(envOptions);
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        restore,
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        return new JobMaster(
                jobId,
                data,
                nodeEngine,
                jobMasterTestExecutor,
                server.getCoordinatorService().getResourceManager(),
                server.getCoordinatorService().getJobHistoryService(),
                runningJobStateIMap,
                runningJobStateTimestampsIMap,
                ownedSlotProfilesIMap,
                runningJobInfoIMap,
                ConfigProvider.locateAndGetSeaTunnelConfig().getEngineConfig(),
                server);
    }

    private PipelineLocation getRunningPipelineLocation(JobMaster jobMaster) {
        return jobMaster.getPhysicalPlan().getPipelineList().get(0).getPipelineLocation();
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
        return server.getEngineContext()
                .getStateStores()
                .metricsSnapshotStore()
                .containsPipeline(pipelineLocation);
    }
}
