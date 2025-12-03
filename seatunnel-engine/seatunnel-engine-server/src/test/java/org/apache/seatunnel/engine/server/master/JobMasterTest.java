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

import org.apache.seatunnel.engine.common.Constant;
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
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.storage.DistributedMapManager;
import org.apache.seatunnel.engine.server.storage.MapStorage;
import org.apache.seatunnel.engine.server.task.CoordinatorTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.internal.serialization.Data;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** JobMaster Tester. */
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobMasterTest extends AbstractSeaTunnelServerTest {
    /**
     * MapStorage key is jobId and value is a Tuple2 Tuple2 key is JobMaster init timestamp and
     * value is the jobImmutableInformation which is sent by client when submit job
     *
     * <p>This MapStorage is used to recovery runningJobInfoMapStorage in JobMaster when a new
     * master node active
     */
    private MapStorage<Long, JobInfo> runningJobInfoMapStorage;

    /**
     * MapStorage key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of MapStorage is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This MapStorage is used to recovery runningJobStateMapStorage in JobMaster when a new
     * master node active
     */
    MapStorage<Object, Object> runningJobStateMapStorage;

    /**
     * MapStorage key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of MapStorage is one of {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.SubPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex} stateTimestamps
     *
     * <p>This MapStorage is used to recovery runningJobStateTimestampsMapStorage in JobMaster when
     * a new master node active
     */
    MapStorage<Object, Long[]> runningJobStateTimestampsMapStorage;

    /**
     * MapStorage key is {@link PipelineLocation}
     *
     * <p>The value of MapStorage is map of {@link TaskGroupLocation} and the {@link SlotProfile} it
     * used.
     *
     * <p>This MapStorage is used to recovery ownedSlotProfilesMapStorage in JobMaster when a new
     * master node active
     */
    private MapStorage<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesMapStorage;

    @BeforeAll
    public void before() {
        super.before();
    }

    @Test
    void testHandleCheckpointTimeout() throws Exception {
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

        testMapStorageRemovedAfterJobComplete(jobId, jobMaster);
    }

    private void testMapStorageRemovedAfterJobComplete(long jobId, JobMaster jobMaster) {
        runningJobInfoMapStorage = DistributedMapManager.getMap(Constant.IMAP_RUNNING_JOB_INFO);
        runningJobStateMapStorage = DistributedMapManager.getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsMapStorage =
                DistributedMapManager.getMap(Constant.IMAP_STATE_TIMESTAMPS);
        ownedSlotProfilesMapStorage =
                DistributedMapManager.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(runningJobInfoMapStorage.get(jobId));
                            Assertions.assertNull(runningJobStateMapStorage.get(jobId));
                            Assertions.assertNull(runningJobStateTimestampsMapStorage.get(jobId));
                            Assertions.assertNull(ownedSlotProfilesMapStorage.get(jobId));

                            jobMaster
                                    .getPhysicalPlan()
                                    .getPipelineList()
                                    .forEach(
                                            pipeline -> {
                                                Assertions.assertNull(
                                                        runningJobStateMapStorage.get(
                                                                pipeline.getPipelineLocation()));

                                                Assertions.assertNull(
                                                        runningJobStateTimestampsMapStorage.get(
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
                                                                            runningJobStateMapStorage
                                                                                    .get(
                                                                                            coordinator
                                                                                                    .getTaskGroupLocation()));

                                                                    Assertions.assertNull(
                                                                            runningJobStateTimestampsMapStorage
                                                                                    .get(
                                                                                            coordinator
                                                                                                    .getTaskGroupLocation()));
                                                                });

                                                pipeline.getPhysicalVertexList()
                                                        .forEach(
                                                                task -> {
                                                                    Assertions.assertNull(
                                                                            runningJobStateMapStorage
                                                                                    .get(
                                                                                            task
                                                                                                    .getTaskGroupLocation()));

                                                                    Assertions.assertNull(
                                                                            runningJobStateTimestampsMapStorage
                                                                                    .get(
                                                                                            task
                                                                                                    .getTaskGroupLocation()));
                                                                });
                                            });
                        });
    }

    @Test
    void testCommitFailedWillRestore() throws Exception {
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
    void testCloseIdleTask() throws InterruptedException {
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
                .getRunningJobStateMapStorage()
                .put(new PipelineLocation(jobId, 1), PipelineStatus.FINISHED);
        Assertions.assertThrows(
                UnknownPhysicalPlanException.class,
                () -> jobMaster.init(System.currentTimeMillis(), false));
    }

    private void assertCloseIdleTask(JobMaster jobMaster) {
        SlotService slotService = server.getSlotService();
        Assertions.assertEquals(4, slotService.getWorkerProfile().getAssignedSlots().length);

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
        SeaTunnelTask seaTunnelTask =
                (SeaTunnelTask) taskGroup1.getTaskGroup().getTasks().stream().findFirst().get();
        jobMaster.getCheckpointManager().readyToCloseIdleTask(seaTunnelTask.getTaskLocation());

        CheckpointCoordinator checkpointCoordinator =
                jobMaster
                        .getCheckpointManager()
                        .getCheckpointCoordinator(seaTunnelTask.getTaskLocation().getPipelineId());
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> checkpointCoordinator.getClosedIdleTask().size() == 3);
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> slotService.getWorkerProfile().getAssignedSlots().length == 3);
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
}
