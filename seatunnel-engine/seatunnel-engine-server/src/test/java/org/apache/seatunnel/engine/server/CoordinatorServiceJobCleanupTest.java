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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.master.cleanup.JobCleanupRecord;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

class CoordinatorServiceJobCleanupTest extends AbstractSeaTunnelServerTest {

    @AfterEach
    void clearPendingCleanupRecords() {
        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP).clear();
        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP).clear();
    }

    @Test
    void testCleanupRemovesStateWhenOwnerMatches() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        String checkpointStateKey = "checkpoint_state_" + jobId;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateIMap.put(checkpointStateKey, "checkpoint");
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);

        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipelineLocation, taskGroupLocation, checkpointStateKey),
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        System.currentTimeMillis()));

        coordinatorService.runPendingJobCleanupOnce();

        Assertions.assertNull(runningJobInfoIMap.get(jobId));
        Assertions.assertNull(runningJobStateIMap.get(jobId));
        Assertions.assertNull(runningJobStateIMap.get(pipelineLocation));
        Assertions.assertNull(runningJobStateIMap.get(taskGroupLocation));
        Assertions.assertNull(runningJobStateIMap.get(checkpointStateKey));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(jobId));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(pipelineLocation));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(taskGroupLocation));
        Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
    }

    @Test
    void testCleanupWaitsForInFlightStateWriterAndDeletesBothMaps() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipelineLocation),
                        stateKeys(jobId),
                        System.currentTimeMillis()));

        ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor();
        boolean pipelineUnlocked = false;
        runningJobStateIMap.lock(pipelineLocation);
        try {
            Future<?> cleanup =
                    cleanupExecutor.submit(coordinatorService::runPendingJobCleanupOnce);
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId)));

            runningJobStateTimestampsIMap.set(pipelineLocation, new Long[] {1L});
            runningJobStateIMap.set(pipelineLocation, "late-writer");
            runningJobStateIMap.unlock(pipelineLocation);
            pipelineUnlocked = true;
            cleanup.get(10, TimeUnit.SECONDS);

            Assertions.assertFalse(runningJobStateIMap.containsKey(pipelineLocation));
            Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(pipelineLocation));
            Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
        } finally {
            if (!pipelineUnlocked) {
                runningJobStateIMap.unlock(pipelineLocation);
            }
            cleanupExecutor.shutdownNow();
        }
    }

    @Test
    void testCleanupRemovesBothMapsForTimestampOnlySnapshotKey() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation timestampOnlyKey = new PipelineLocation(jobId, 7);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        // Model a state entry that appears after the state-key snapshot but before timestamp scan.
        runningJobStateIMap.put(timestampOnlyKey, "late-state");
        runningJobStateTimestampsIMap.put(timestampOnlyKey, new Long[] {1L});
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        100L,
                        JobStatus.FINISHED,
                        Collections.emptySet(),
                        stateKeys(timestampOnlyKey),
                        System.currentTimeMillis()));

        coordinatorService.runPendingJobCleanupOnce();

        Assertions.assertFalse(runningJobStateIMap.containsKey(timestampOnlyKey));
        Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(timestampOnlyKey));
        Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
    }

    @Test
    void testCleanupSkipsStateWhenOwnerChanged() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, new JobInfo(200L, null));
        runningJobStateIMap.put(jobId, JobStatus.RUNNING);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);

        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        100L,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        System.currentTimeMillis()));

        coordinatorService.runPendingJobCleanupOnce();

        Assertions.assertNotNull(runningJobInfoIMap.get(jobId));
        Assertions.assertEquals(JobStatus.RUNNING, runningJobStateIMap.get(jobId));
        Assertions.assertNotNull(runningJobStateIMap.get(pipelineLocation));
        Assertions.assertNotNull(runningJobStateIMap.get(taskGroupLocation));
        Assertions.assertNotNull(runningJobStateTimestampsIMap.get(jobId));
        Assertions.assertNotNull(runningJobStateTimestampsIMap.get(pipelineLocation));
        Assertions.assertNotNull(runningJobStateTimestampsIMap.get(taskGroupLocation));
        Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
    }

    @Test
    void testCleanupRemovesStateWhenOwnerMissing() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        String checkpointStateKey = "checkpoint_state_" + jobId;

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateIMap.put(checkpointStateKey, "checkpoint");
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);

        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        100L,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipelineLocation, taskGroupLocation, checkpointStateKey),
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        System.currentTimeMillis()));

        coordinatorService.runPendingJobCleanupOnce();

        Assertions.assertNull(runningJobStateIMap.get(jobId));
        Assertions.assertNull(runningJobStateIMap.get(pipelineLocation));
        Assertions.assertNull(runningJobStateIMap.get(taskGroupLocation));
        Assertions.assertNull(runningJobStateIMap.get(checkpointStateKey));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(jobId));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(pipelineLocation));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(taskGroupLocation));
        Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
    }

    @Test
    void testSubmitBlockedWhenCleanupStillPending() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId),
                        stateKeys(jobId),
                        System.currentTimeMillis()));

        CompletionException exception =
                Assertions.assertThrows(
                        CompletionException.class,
                        () -> coordinatorService.submitJob(jobId, null, false).join());
        Assertions.assertInstanceOf(JobException.class, exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains("waiting for terminal state cleanup"));
        Assertions.assertEquals(
                initializationTimestamp,
                runningJobInfoIMap.get(jobId).getInitializationTimestamp(),
                "failed submit must not delete the retained cleanup owner");
        Assertions.assertTrue(
                pendingJobCleanupIMap.containsKey(jobId),
                "failed submit must not consume the pending cleanup record");
    }

    @Test
    void testSubmitBlockedWhenCleanupStillPendingOnOperationThread() {
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId),
                        stateKeys(jobId),
                        System.currentTimeMillis()));

        CompletionException exception =
                Assertions.assertThrows(
                        CompletionException.class,
                        () ->
                                NodeEngineUtil.sendOperationToMasterNode(
                                                nodeEngine,
                                                new SubmitJobOperation(
                                                        jobId, createJobData(jobId, false), false))
                                        .join());
        Assertions.assertInstanceOf(JobException.class, exception.getCause());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains("waiting for terminal state cleanup"));
    }

    /**
     * A savepoint restart whose pending cleanup record is owned by the job's currently published
     * generation must consume that record inline and let the submission through, instead of failing
     * - see {@link CoordinatorService#validateJobSubmissionFence}'s Javadoc. This covers the
     * FINISHED terminal state; {@link
     * #testSubmitStartWithSavePointConsumesOwnedPendingCleanupFromSavepointDoneState()} covers the
     * SAVEPOINT_DONE terminal state with the same ownership shape.
     */
    @Test
    void testSubmitStartWithSavePointConsumesOwnedPendingCleanupAndSucceeds() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        IMap<PipelineLocation, PipelineCleanupRecord> pendingPipelineCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineCleanupRecord pipelineCleanupRecord =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId),
                        stateKeys(jobId),
                        System.currentTimeMillis()));
        pendingPipelineCleanupIMap.put(pipelineLocation, pipelineCleanupRecord);

        Assertions.assertDoesNotThrow(
                () -> coordinatorService.submitJob(jobId, createJobData(jobId, true), true).join());

        Assertions.assertNotEquals(JobStatus.FINISHED, runningJobStateIMap.get(jobId));
        Assertions.assertFalse(
                pendingJobCleanupIMap.containsKey(jobId),
                "consuming the owned cleanup record inline must remove it");
        Assertions.assertFalse(
                pendingPipelineCleanupIMap.containsKey(pipelineLocation),
                "successful restore must invalidate cleanup for the previous pipeline generation");
    }

    /**
     * Same ownership shape as {@link
     * #testSubmitStartWithSavePointConsumesOwnedPendingCleanupAndSucceeds()}, starting from
     * SAVEPOINT_DONE instead of FINISHED: a savepoint restart consumes its own previous
     * generation's pending cleanup record inline in the same submission, with no separate {@link
     * CoordinatorService#runPendingJobCleanupOnce()} pass required first.
     */
    @Test
    void testSubmitStartWithSavePointConsumesOwnedPendingCleanupFromSavepointDoneState() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        IMap<PipelineLocation, PipelineCleanupRecord> pendingPipelineCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        PipelineCleanupRecord pipelineCleanupRecord =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        PipelineStatus.FINISHED,
                        false,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        false,
                        System.currentTimeMillis(),
                        0L,
                        0);

        runningJobInfoIMap.put(jobId, new JobInfo(initializationTimestamp, null));
        runningJobStateIMap.put(jobId, JobStatus.SAVEPOINT_DONE);
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        initializationTimestamp,
                        JobStatus.SAVEPOINT_DONE,
                        stateKeys(jobId),
                        stateKeys(jobId),
                        System.currentTimeMillis()));
        pendingPipelineCleanupIMap.put(pipelineLocation, pipelineCleanupRecord);

        Assertions.assertDoesNotThrow(
                () ->
                        coordinatorService
                                .submitJob(
                                        jobId,
                                        createJobData(jobId, true, "stream_fake_to_console.conf"),
                                        true)
                                .join());

        Assertions.assertNotEquals(JobStatus.SAVEPOINT_DONE, runningJobStateIMap.get(jobId));
        Assertions.assertFalse(
                pendingJobCleanupIMap.containsKey(jobId),
                "consuming the owned cleanup record inline must remove it");
        Assertions.assertFalse(
                pendingPipelineCleanupIMap.containsKey(pipelineLocation),
                "successful restore must invalidate cleanup for the previous pipeline generation");
    }

    /**
     * A removed owner does not authorize reusing an id while failed cleanup retains terminal keys.
     * Both ordinary submission and savepoint submission must wait for the existing cleanup fence.
     */
    @Test
    void testOwnerAbsentPartialCleanupBlocksBothSubmissionModes() throws Exception {
        List<Runnable> attempts = new CopyOnWriteArrayList<>();
        List<Long> delays = new CopyOnWriteArrayList<>();
        CoordinatorService coordinator = capturedCleanupCoordinator(attempts, delays);
        long jobId = System.currentTimeMillis();
        IMap<Object, Object> states =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> timestamps =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobInfo> infos =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Long, JobCleanupRecord> pending =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        JobCleanupRecord record =
                new JobCleanupRecord(
                        100L, JobStatus.FINISHED, stateKeys(jobId), stateKeys(jobId), 0L);
        IMap<Object, Object> failingStates =
                Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(states));
        Mockito.doThrow(new RuntimeException("state removal unavailable"))
                .when(failingStates)
                .remove(jobId);
        ReflectionUtils.setField(
                coordinator, CoordinatorService.class, "runningJobStateIMap", failingStates);
        try {
            states.put(jobId, JobStatus.FINISHED);
            timestamps.put(jobId, new Long[] {1L});
            pending.put(jobId, record);
            coordinator.schedulePendingJobCleanup(jobId, record);
            attempts.get(0).run();
            Assertions.assertEquals(2, attempts.size());
            for (boolean savepoint : new boolean[] {false, true}) {
                CompletionException failure =
                        Assertions.assertThrows(
                                CompletionException.class,
                                () ->
                                        coordinator
                                                .submitJob(
                                                        jobId,
                                                        createJobData(jobId, savepoint),
                                                        savepoint)
                                                .join());
                Assertions.assertInstanceOf(JobException.class, failure.getCause());
                Assertions.assertTrue(
                        failure.getCause().getMessage().contains("terminal state cleanup"));
                Assertions.assertFalse(infos.containsKey(jobId));
                Assertions.assertEquals(JobStatus.FINISHED, states.get(jobId));
                Assertions.assertEquals(record, pending.get(jobId));
            }
        } finally {
            states.remove(jobId);
            timestamps.remove(jobId);
            pending.remove(jobId);
        }
    }

    /**
     * Savepoint admission waits for an in-flight writer and removes both maps for asymmetric keys.
     * The fence is invoked directly so no successor job can obscure the removed old-generation
     * data.
     */
    @Test
    void testSavepointCleanupUsesLockedPairedKeyUnion() throws Exception {
        CoordinatorService coordinator = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipeline = new PipelineLocation(jobId, 1);
        IMap<Object, Object> states =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> timestamps =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobInfo> infos =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Long, JobCleanupRecord> pending =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        infos.put(jobId, new JobInfo(100L, null));
        states.put(jobId, JobStatus.SAVEPOINT_DONE);
        states.put(pipeline, "old-pipeline");
        timestamps.put(jobId, new Long[] {1L});
        timestamps.put(pipeline, new Long[] {1L});
        pending.put(
                jobId,
                new JobCleanupRecord(
                        100L,
                        JobStatus.SAVEPOINT_DONE,
                        stateKeys(jobId),
                        stateKeys(jobId, pipeline),
                        System.currentTimeMillis()));
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        Future<?> admission = null;
        boolean unlocked = false;
        states.lock(pipeline);
        try {
            admission =
                    submitter.submit(
                            () -> {
                                checkSubmissionFence(coordinator, jobId, true);
                                return null;
                            });
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertFalse(states.containsKey(jobId)));
            Assertions.assertFalse(admission.isDone());
            Assertions.assertTrue(infos.containsKey(jobId));
            states.put(pipeline, "in-flight-writer");
            timestamps.put(pipeline, new Long[] {7L});
            states.unlock(pipeline);
            unlocked = true;
            admission.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(states.containsKey(pipeline));
            Assertions.assertFalse(timestamps.containsKey(pipeline));
            Assertions.assertFalse(timestamps.containsKey(jobId));
            Assertions.assertFalse(infos.containsKey(jobId));
            Assertions.assertFalse(pending.containsKey(jobId));
        } finally {
            try {
                if (!unlocked) {
                    states.unlock(pipeline);
                }
            } finally {
                try {
                    if (admission != null) {
                        admission.cancel(true);
                        try {
                            admission.get(10, TimeUnit.SECONDS);
                        } catch (CancellationException expected) {
                            // Executor termination below also joins the cancelled worker itself.
                        }
                    }
                } finally {
                    submitter.shutdownNow();
                    Assertions.assertTrue(
                            submitter.awaitTermination(10, TimeUnit.SECONDS),
                            "savepoint admission worker must terminate before map cleanup");
                    states.remove(jobId);
                    states.remove(pipeline);
                    timestamps.remove(jobId);
                    timestamps.remove(pipeline);
                    infos.remove(jobId);
                    pending.remove(jobId);
                }
            }
        }
    }

    /**
     * Partial inline cleanup keeps its owner and record even if the terminal job key was removed.
     * The captured retry must finish that same generation instead of discarding its durable fence.
     */
    @Test
    void testSavepointCleanupRetriesAfterRemovingOnlyTheJobKey() throws Exception {
        List<Runnable> attempts = new CopyOnWriteArrayList<>();
        List<Long> delays = new CopyOnWriteArrayList<>();
        CoordinatorService coordinator = capturedCleanupCoordinator(attempts, delays);
        long jobId = System.currentTimeMillis();
        PipelineLocation pipeline = new PipelineLocation(jobId, 1);
        IMap<Object, Object> states =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> timestamps =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobInfo> infos =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Long, JobCleanupRecord> pending =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        JobInfo owner = new JobInfo(100L, null);
        JobCleanupRecord record =
                new JobCleanupRecord(
                        100L,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipeline),
                        stateKeys(jobId, pipeline),
                        System.currentTimeMillis());
        IMap<Object, Object> failingStates =
                Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(states));
        Mockito.doThrow(new RuntimeException("pipeline removal unavailable"))
                .when(failingStates)
                .remove(pipeline);
        ReflectionUtils.setField(
                coordinator, CoordinatorService.class, "runningJobStateIMap", failingStates);
        try {
            infos.put(jobId, owner);
            states.put(jobId, JobStatus.FINISHED);
            states.put(pipeline, "old-pipeline");
            timestamps.put(jobId, new Long[] {1L});
            timestamps.put(pipeline, new Long[] {1L});
            pending.put(jobId, record);
            Assertions.assertThrows(
                    JobException.class, () -> checkSubmissionFence(coordinator, jobId, true));
            Assertions.assertFalse(states.containsKey(jobId));
            Assertions.assertEquals(owner, infos.get(jobId));
            Assertions.assertEquals(record, pending.get(jobId));
            Assertions.assertEquals("old-pipeline", states.get(pipeline));
            Assertions.assertEquals(1, attempts.size());
            Assertions.assertEquals(Long.valueOf(5000L), delays.get(0));
            ReflectionUtils.setField(
                    coordinator, CoordinatorService.class, "runningJobStateIMap", states);
            attempts.get(0).run();
            Assertions.assertFalse(infos.containsKey(jobId));
            Assertions.assertFalse(states.containsKey(pipeline));
            Assertions.assertFalse(timestamps.containsKey(pipeline));
            Assertions.assertFalse(pending.containsKey(jobId));
        } finally {
            states.remove(jobId);
            states.remove(pipeline);
            timestamps.remove(jobId);
            timestamps.remove(pipeline);
            infos.remove(jobId);
            pending.remove(jobId);
        }
    }

    /**
     * State-key and job-fence lock failures retain one bounded retry for the captured generation.
     * Repeated acquisition failure and simultaneous key/unlock failures must neither lose nor
     * multiply the scheduled continuation.
     */
    @Test
    void testCleanupRetriesStateAndJobLockFailures() throws Exception {
        long firstJobId = System.currentTimeMillis();
        int index = 0;
        for (String failurePoint :
                new String[] {"state-lock", "state-unlock", "job-lock", "job-unlock"}) {
            List<Runnable> attempts = new CopyOnWriteArrayList<>();
            List<Long> delays = new CopyOnWriteArrayList<>();
            CoordinatorService coordinator = capturedCleanupCoordinator(attempts, delays);
            long jobId = firstJobId + index++;
            PipelineLocation pipeline = new PipelineLocation(jobId, 1);
            IMap<Object, Object> states =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
            IMap<Object, Long[]> timestamps =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
            IMap<Long, JobCleanupRecord> pending =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
            JobCleanupRecord record =
                    new JobCleanupRecord(
                            100L,
                            JobStatus.FINISHED,
                            stateKeys(jobId, pipeline),
                            stateKeys(jobId, pipeline),
                            0L);
            IMap<Object, Object> failingStates =
                    Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(states));
            IMap<Long, JobCleanupRecord> failingPending =
                    Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(pending));
            if ("state-lock".equals(failurePoint)) {
                Mockito.doThrow(new RuntimeException("state lock unavailable"))
                        .when(failingStates)
                        .lock(jobId);
            } else if ("state-unlock".equals(failurePoint)) {
                Mockito.doAnswer(
                                call -> {
                                    states.unlock(jobId);
                                    throw new RuntimeException("state unlock acknowledgement lost");
                                })
                        .when(failingStates)
                        .unlock(jobId);
            } else if ("job-lock".equals(failurePoint)) {
                Mockito.doThrow(new RuntimeException("job fence unavailable"))
                        .when(failingPending)
                        .lock(jobId);
            } else {
                Mockito.doThrow(new RuntimeException("state removal unavailable"))
                        .when(failingStates)
                        .remove(jobId);
                Mockito.doAnswer(
                                call -> {
                                    pending.unlock(jobId);
                                    throw new RuntimeException("job unlock acknowledgement lost");
                                })
                        .when(failingPending)
                        .unlock(jobId);
            }
            ReflectionUtils.setField(
                    coordinator, CoordinatorService.class, "runningJobStateIMap", failingStates);
            ReflectionUtils.setField(
                    coordinator, CoordinatorService.class, "pendingJobCleanupIMap", failingPending);
            try {
                states.put(jobId, JobStatus.FINISHED);
                states.put(pipeline, "old-pipeline");
                timestamps.put(jobId, new Long[] {1L});
                timestamps.put(pipeline, new Long[] {1L});
                pending.put(jobId, record);
                coordinator.schedulePendingJobCleanup(jobId, record);
                attempts.get(0).run();
                Assertions.assertEquals(2, attempts.size(), failurePoint);
                Assertions.assertEquals(Long.valueOf(5000L), delays.get(1), failurePoint);
                Assertions.assertEquals(record, pending.get(jobId), failurePoint);
                if ("job-lock".equals(failurePoint)) {
                    attempts.get(1).run();
                    Assertions.assertEquals(3, attempts.size());
                    Assertions.assertEquals(Long.valueOf(5000L), delays.get(2));
                } else {
                    Assertions.assertFalse(states.containsKey(pipeline), failurePoint);
                    Assertions.assertFalse(timestamps.containsKey(pipeline), failurePoint);
                }
                ReflectionUtils.setField(
                        coordinator, CoordinatorService.class, "runningJobStateIMap", states);
                ReflectionUtils.setField(
                        coordinator, CoordinatorService.class, "pendingJobCleanupIMap", pending);
                attempts.get(attempts.size() - 1).run();
                Assertions.assertFalse(states.containsKey(jobId), failurePoint);
                Assertions.assertFalse(timestamps.containsKey(jobId), failurePoint);
                Assertions.assertFalse(states.containsKey(pipeline), failurePoint);
                Assertions.assertFalse(timestamps.containsKey(pipeline), failurePoint);
                Assertions.assertFalse(pending.containsKey(jobId), failurePoint);
            } finally {
                states.remove(jobId);
                states.remove(pipeline);
                timestamps.remove(jobId);
                timestamps.remove(pipeline);
                pending.remove(jobId);
            }
        }
    }

    @Test
    void testTerminalZombieCleanupRegistersFenceAfterSideEffects() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        JobInfo jobInfo =
                new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, jobInfo);
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);
        JobDAGInfo staleDAG = new JobDAGInfo();
        staleDAG.setJobId(-1L);
        coordinatorService.getJobHistoryService().storeJobInfo(jobId, staleDAG);
        coordinatorService
                .getJobHistoryService()
                .storeFinishedJobState(
                        new JobHistoryService.JobState(
                                jobId,
                                "old-generation",
                                JobStatus.FINISHED,
                                1L,
                                2L,
                                3L,
                                Collections.emptyMap(),
                                null));
        long originalCleanupDelay =
                server.getSeaTunnelConfig().getEngineConfig().getStateCleanupDelayMillis();
        server.getSeaTunnelConfig().getEngineConfig().setStateCleanupDelayMillis(60000L);
        IMap<Object, Object> failingStateMap =
                Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(runningJobStateIMap));
        Mockito.doThrow(new RuntimeException("state removal unavailable"))
                .when(failingStateMap)
                .remove(jobId);
        ReflectionUtils.setField(coordinatorService, "runningJobStateIMap", failingStateMap);

        try {
            coordinatorService.cleanupTerminalZombieJob(jobId, jobInfo, JobStatus.CANCELED);

            Assertions.assertEquals(JobStatus.CANCELED, runningJobStateIMap.get(jobId));
            Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId));
            Assertions.assertTrue(
                    pendingJobCleanupIMap.containsKey(jobId),
                    "Concurrent owner removal must not discard the durable cleanup fence");
            Assertions.assertEquals(
                    jobInfo.getInitializationTimestamp(),
                    pendingJobCleanupIMap.get(jobId).getOwnerInitializationTimestamp());
            Assertions.assertEquals(
                    Long.valueOf(jobId),
                    coordinatorService.getJobHistoryService().getJobDAGInfo(jobId).getJobId());
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    coordinatorService
                            .getJobHistoryService()
                            .getJobDetailState(jobId)
                            .getJobStatus());
            Assertions.assertEquals(
                    "Test",
                    coordinatorService
                            .getJobHistoryService()
                            .getJobDetailState(jobId)
                            .getJobName());

            coordinatorService.cleanupTerminalZombieJob(jobId, jobInfo, JobStatus.CANCELED);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    coordinatorService
                            .getJobHistoryService()
                            .getJobDetailState(jobId)
                            .getJobStatus());
        } finally {
            ReflectionUtils.setField(
                    coordinatorService, "runningJobStateIMap", runningJobStateIMap);
            server.getSeaTunnelConfig()
                    .getEngineConfig()
                    .setStateCleanupDelayMillis(originalCleanupDelay);
        }
    }

    @Test
    void testImmediateZombieRetryCannotCleanAReplacementRecord() throws Exception {
        for (boolean replaceOwner : new boolean[] {false, true}) {
            CoordinatorService coordinatorService = server.getCoordinatorService();
            long jobId = System.currentTimeMillis();
            JobInfo oldOwner =
                    new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
            IMap<Long, JobInfo> infos =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
            IMap<Object, Object> states =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
            IMap<Object, Long[]> timestamps =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
            IMap<Long, JobCleanupRecord> pending =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
            ScheduledExecutorService originalMonitor = server.getMonitorService();
            ScheduledExecutorService monitor = Mockito.mock(ScheduledExecutorService.class);
            AtomicReference<Runnable> retry = new AtomicReference<>();
            Mockito.doAnswer(
                            call -> {
                                retry.set(call.getArgument(0));
                                return Mockito.mock(ScheduledFuture.class);
                            })
                    .when(monitor)
                    .schedule(
                            Mockito.any(Runnable.class),
                            Mockito.anyLong(),
                            Mockito.eq(TimeUnit.MILLISECONDS));
            IMap<Object, Object> failingStates =
                    Mockito.mock(IMap.class, AdditionalAnswers.delegatesTo(states));
            Mockito.doThrow(new RuntimeException("first cleanup cannot remove state"))
                    .when(failingStates)
                    .remove(jobId);
            long originalDelay =
                    server.getSeaTunnelConfig().getEngineConfig().getStateCleanupDelayMillis();
            try {
                server.getSeaTunnelConfig().getEngineConfig().setStateCleanupDelayMillis(60000L);
                ReflectionUtils.setField(server, "monitorService", monitor);
                ReflectionUtils.setField(coordinatorService, "runningJobStateIMap", failingStates);
                infos.put(jobId, oldOwner);
                states.put(jobId, JobStatus.FAILED);
                coordinatorService.cleanupTerminalZombieJob(jobId, oldOwner, JobStatus.FAILED);
                Assertions.assertNotNull(retry.get());
                JobCleanupRecord oldRecord = pending.get(jobId);
                Assertions.assertNotNull(oldRecord);
                Assertions.assertFalse(infos.containsKey(jobId));

                // The old attempt has already released its fence. A later terminal record for
                // the same id, with either a new owner or new cleanup identity, owns retention.
                ReflectionUtils.setField(coordinatorService, "runningJobStateIMap", states);
                JobInfo successor = new JobInfo(replaceOwner ? 200L : 100L, null);
                JobCleanupRecord successorRecord =
                        new JobCleanupRecord(
                                successor.getInitializationTimestamp(),
                                JobStatus.FAILED,
                                stateKeys(jobId),
                                stateKeys(jobId),
                                oldRecord.getCreateTimeMillis() + 1L);
                infos.put(jobId, successor);
                states.put(jobId, JobStatus.FAILED);
                Long[] successorTimestamps = new Long[JobStatus.values().length];
                successorTimestamps[JobStatus.FAILED.ordinal()] = 7L;
                timestamps.put(jobId, successorTimestamps);
                pending.put(jobId, successorRecord);
                Runnable staleRetry = retry.get();
                staleRetry.run();

                Assertions.assertEquals(successor, infos.get(jobId));
                Assertions.assertEquals(JobStatus.FAILED, states.get(jobId));
                Assertions.assertArrayEquals(successorTimestamps, timestamps.get(jobId));
                Assertions.assertEquals(successorRecord, pending.get(jobId));
            } finally {
                ReflectionUtils.setField(coordinatorService, "runningJobStateIMap", states);
                ReflectionUtils.setField(server, "monitorService", originalMonitor);
                server.getSeaTunnelConfig()
                        .getEngineConfig()
                        .setStateCleanupDelayMillis(originalDelay);
                infos.remove(jobId);
                states.remove(jobId);
                timestamps.remove(jobId);
                pending.remove(jobId);
            }
        }
    }

    @Test
    void testTerminalZombieSideEffectFailureKeepsRecoverableOwner() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        JobInfo jobInfo =
                new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        JobHistoryService originalHistoryService = coordinatorService.getJobHistoryService();
        JobHistoryService failingHistoryService = Mockito.spy(originalHistoryService);
        Mockito.doThrow(new RuntimeException("history unavailable"))
                .when(failingHistoryService)
                .storeJobInfo(Mockito.eq(jobId), Mockito.any(JobDAGInfo.class));

        runningJobInfoIMap.put(jobId, jobInfo);
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);
        ReflectionUtils.setField(coordinatorService, "jobHistoryService", failingHistoryService);

        try {
            RuntimeException failure =
                    Assertions.assertThrows(
                            RuntimeException.class,
                            () ->
                                    coordinatorService.cleanupTerminalZombieJob(
                                            jobId, jobInfo, JobStatus.CANCELED));
            Assertions.assertEquals("history unavailable", failure.getMessage());
            Assertions.assertEquals(jobInfo, runningJobInfoIMap.get(jobId));
            Assertions.assertEquals(JobStatus.CANCELED, runningJobStateIMap.get(jobId));
            Assertions.assertFalse(
                    pendingJobCleanupIMap.containsKey(jobId),
                    "Destructive cleanup must not be registered before side effects complete");
        } finally {
            ReflectionUtils.setField(
                    coordinatorService, "jobHistoryService", originalHistoryService);
        }

        coordinatorService.cleanupTerminalZombieJob(jobId, jobInfo, JobStatus.CANCELED);
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId));
                            Assertions.assertFalse(runningJobStateIMap.containsKey(jobId));
                            Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
                        });
        Assertions.assertNotNull(originalHistoryService.getJobDAGInfo(jobId));
        Assertions.assertEquals(
                JobStatus.CANCELED, originalHistoryService.getJobDetailState(jobId).getJobStatus());
    }

    @Test
    void testDelayedOrphanCleanupIsScheduledWhenMonitorServiceUnavailable() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobStateIMap.put(jobId, JobStatus.FINISHED);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);

        JobCleanupRecord cleanupRecord =
                new JobCleanupRecord(
                        100L,
                        JobStatus.FINISHED,
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        stateKeys(jobId, pipelineLocation, taskGroupLocation),
                        System.currentTimeMillis());
        pendingJobCleanupIMap.put(jobId, cleanupRecord);

        ScheduledExecutorService monitorService = server.getMonitorService();
        server.getSeaTunnelConfig().getEngineConfig().setStateCleanupDelayMillis(50L);
        ReflectionUtils.setField(server, "monitorService", null);
        try {
            coordinatorService.schedulePendingJobCleanup(jobId, cleanupRecord);

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(pendingJobCleanupIMap.containsKey(jobId));
                                Assertions.assertFalse(runningJobStateIMap.containsKey(jobId));
                                Assertions.assertFalse(
                                        runningJobStateIMap.containsKey(pipelineLocation));
                                Assertions.assertFalse(
                                        runningJobStateIMap.containsKey(taskGroupLocation));
                                Assertions.assertFalse(
                                        runningJobStateTimestampsIMap.containsKey(jobId));
                                Assertions.assertFalse(
                                        runningJobStateTimestampsIMap.containsKey(
                                                pipelineLocation));
                                Assertions.assertFalse(
                                        runningJobStateTimestampsIMap.containsKey(
                                                taskGroupLocation));
                            });
        } finally {
            ReflectionUtils.setField(server, "monitorService", monitorService);
            server.getSeaTunnelConfig().getEngineConfig().setStateCleanupDelayMillis(0L);
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testTerminalZombieJobWithoutCleanupRecordPersistsHistoryAndRemovesState()
            throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        String checkpointStateKey = "checkpoint_state_" + jobId + "_1";

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);

        Data jobData = createJobData(jobId, false, "stream_fake_to_console.conf");
        runningJobInfoIMap.put(jobId, new JobInfo(100L, jobData));
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateIMap.put(checkpointStateKey, "checkpoint");

        Long[] jobStateTimestamps = new Long[JobStatus.values().length];
        jobStateTimestamps[JobStatus.SCHEDULED.ordinal()] = 10L;
        jobStateTimestamps[JobStatus.CANCELED.ordinal()] = 20L;
        runningJobStateTimestampsIMap.put(jobId, jobStateTimestamps);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);
        storeCheckpoint(jobId);
        server.getCheckpointMonitorService()
                .onCheckpointTriggered(
                        jobId,
                        1,
                        1,
                        CheckpointType.COMPLETED_POINT_TYPE,
                        System.currentTimeMillis(),
                        1);
        Assertions.assertFalse(
                server.getCheckpointService()
                        .getCheckpointStorage()
                        .getAllCheckpoints(String.valueOf(jobId))
                        .isEmpty());
        Assertions.assertTrue(server.getCheckpointMonitorService().getOverview(jobId).isPresent());

        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);

        method.invoke(coordinatorService, jobId, runningJobInfoIMap.get(jobId));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId));
                            Assertions.assertFalse(runningJobStateIMap.containsKey(jobId));
                            Assertions.assertFalse(
                                    runningJobStateIMap.containsKey(pipelineLocation));
                            Assertions.assertFalse(
                                    runningJobStateIMap.containsKey(taskGroupLocation));
                            Assertions.assertFalse(
                                    runningJobStateIMap.containsKey(checkpointStateKey));
                            Assertions.assertFalse(
                                    runningJobStateTimestampsIMap.containsKey(jobId));
                            Assertions.assertFalse(
                                    runningJobStateTimestampsIMap.containsKey(pipelineLocation));
                            Assertions.assertFalse(
                                    runningJobStateTimestampsIMap.containsKey(taskGroupLocation));
                        });
        Assertions.assertNotNull(coordinatorService.getJobHistoryService().getJobDAGInfo(jobId));
        Assertions.assertEquals(
                JobStatus.CANCELED,
                coordinatorService.getJobHistoryService().getJobDetailState(jobId).getJobStatus());
        Assertions.assertTrue(
                server.getCheckpointService()
                        .getCheckpointStorage()
                        .getAllCheckpoints(String.valueOf(jobId))
                        .isEmpty());
        Assertions.assertFalse(server.getCheckpointMonitorService().getOverview(jobId).isPresent());
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void testTerminalZombieCanceledJobRetainsCheckpointWhenJobEnvOverrideEnabled()
            throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        PipelineLocation pipelineLocation = new PipelineLocation(jobId, 1);
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        String checkpointStateKey = "checkpoint_state_" + jobId + "_retain";

        server.getSeaTunnelConfig()
                .getEngineConfig()
                .getCheckpointConfig()
                .setRetainAfterJobCancelled(false);

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);

        Map<String, Object> envOptions = new HashMap<>();
        envOptions.put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), 10000L);
        envOptions.put(EnvCommonOptions.CHECKPOINT_RETAIN_AFTER_JOB_CANCELLED.key(), true);
        Data jobData = createJobData(jobId, false, "stream_fake_to_console.conf", envOptions);
        runningJobInfoIMap.put(jobId, new JobInfo(100L, jobData));
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);
        runningJobStateIMap.put(pipelineLocation, "pipeline");
        runningJobStateIMap.put(taskGroupLocation, "task");
        runningJobStateIMap.put(checkpointStateKey, "checkpoint");

        Long[] jobStateTimestamps = new Long[JobStatus.values().length];
        jobStateTimestamps[JobStatus.SCHEDULED.ordinal()] = 10L;
        jobStateTimestamps[JobStatus.CANCELED.ordinal()] = 20L;
        runningJobStateTimestampsIMap.put(jobId, jobStateTimestamps);
        runningJobStateTimestampsIMap.put(pipelineLocation, new Long[1]);
        runningJobStateTimestampsIMap.put(taskGroupLocation, new Long[1]);
        storeCheckpoint(jobId);

        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);
        method.invoke(coordinatorService, jobId, runningJobInfoIMap.get(jobId));

        Assertions.assertTrue(
                server.getCheckpointService()
                                .getCheckpointStorage()
                                .getAllCheckpoints(String.valueOf(jobId))
                                .size()
                        > 0,
                "job-level env override should retain checkpoint data even when cluster default is false");
    }

    @Test
    void testRestoreUsesProvidedJobInfoInitializationTimestamp() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long initializationTimestamp = 100L;

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);

        JobInfo jobInfo =
                new JobInfo(
                        initializationTimestamp,
                        createJobData(jobId, false, "stream_fake_to_console.conf"));
        runningJobInfoIMap.put(jobId, jobInfo);
        runningJobStateIMap.put(jobId, JobStatus.RUNNING);

        invokeRestoreJobFromMasterActiveSwitch(coordinatorService, jobId, jobInfo);

        Long[] jobStateTimestamps = runningJobStateTimestampsIMap.get(jobId);
        Assertions.assertNotNull(jobStateTimestamps);
        Assertions.assertEquals(
                initializationTimestamp, jobStateTimestamps[JobStatus.INITIALIZING.ordinal()]);
        Assertions.assertTrue(
                coordinatorService.getPendingJobQueue().contains(jobId)
                        || getRunningJobMasterMap(coordinatorService).containsKey(jobId));
    }

    @Test
    void testRestoreOwnerRetryStopsWhenGenerationChanged() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        JobInfo staleJobInfo =
                new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        JobInfo currentJobInfo =
                new JobInfo(200L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        runningJobInfoIMap.put(jobId, currentJobInfo);
        runningJobStateIMap.put(jobId, JobStatus.RUNNING);

        IMap<Long, JobInfo> spiedRunningJobInfoIMap = Mockito.spy(runningJobInfoIMap);
        Mockito.doThrow(new RetryableHazelcastException("owner loading"))
                .doReturn(currentJobInfo)
                .when(spiedRunningJobInfoIMap)
                .get(jobId);
        ReflectionUtils.setField(coordinatorService, "runningJobInfoIMap", spiedRunningJobInfoIMap);
        try {
            invokeRestoreJobFromMasterActiveSwitch(coordinatorService, jobId, staleJobInfo);
        } finally {
            ReflectionUtils.setField(coordinatorService, "runningJobInfoIMap", runningJobInfoIMap);
        }

        Assertions.assertFalse(coordinatorService.getPendingJobQueue().contains(jobId));
        Assertions.assertFalse(getRunningJobMasterMap(coordinatorService).containsKey(jobId));
        Assertions.assertNull(runningJobStateTimestampsIMap.get(jobId));
        Assertions.assertEquals(JobStatus.RUNNING, runningJobStateIMap.get(jobId));
    }

    @Test
    void testMissingStateRepairKeepsTimestampWhenRetryObservesCreated() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        JobInfo jobInfo =
                new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Object, Object> spiedRunningJobStateIMap = Mockito.spy(runningJobStateIMap);
        Mockito.doReturn(null).when(spiedRunningJobStateIMap).get(jobId);
        Mockito.doReturn(JobStatus.CREATED)
                .when(spiedRunningJobStateIMap)
                .putIfAbsent(jobId, JobStatus.CREATED);
        runningJobInfoIMap.put(jobId, jobInfo);
        ReflectionUtils.setField(
                coordinatorService, "runningJobStateIMap", spiedRunningJobStateIMap);

        try {
            Assertions.assertEquals(
                    JobStatus.CREATED,
                    coordinatorService.repairMissingJobStateForRestore(jobId, jobInfo));
            assertCreatedRepairTimestamps(
                    runningJobStateTimestampsIMap.get(jobId), jobInfo.getInitializationTimestamp());
        } finally {
            ReflectionUtils.setField(
                    coordinatorService, "runningJobStateIMap", runningJobStateIMap);
            runningJobInfoIMap.remove(jobId);
            runningJobStateIMap.remove(jobId);
            runningJobStateTimestampsIMap.remove(jobId);
        }
    }

    @Test
    void testMissingStateRepairKeepsTimestampWhenIndeterminateWriteCommitted() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        JobInfo jobInfo =
                new JobInfo(100L, createJobData(jobId, false, "stream_fake_to_console.conf"));
        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Object, Object> spiedRunningJobStateIMap = Mockito.spy(runningJobStateIMap);
        AtomicReference<Object> persistedState = new AtomicReference<>();
        Mockito.doAnswer(invocation -> persistedState.get())
                .when(spiedRunningJobStateIMap)
                .get(jobId);
        Mockito.doAnswer(
                        invocation -> {
                            persistedState.set(JobStatus.CREATED);
                            throw new IndeterminateOperationStateException("reply lost");
                        })
                .when(spiedRunningJobStateIMap)
                .putIfAbsent(jobId, JobStatus.CREATED);
        runningJobInfoIMap.put(jobId, jobInfo);
        ReflectionUtils.setField(
                coordinatorService, "runningJobStateIMap", spiedRunningJobStateIMap);

        try {
            Assertions.assertEquals(
                    JobStatus.CREATED,
                    coordinatorService.repairMissingJobStateForRestore(jobId, jobInfo));
            assertCreatedRepairTimestamps(
                    runningJobStateTimestampsIMap.get(jobId), jobInfo.getInitializationTimestamp());
        } finally {
            ReflectionUtils.setField(
                    coordinatorService, "runningJobStateIMap", runningJobStateIMap);
            runningJobInfoIMap.remove(jobId);
            runningJobStateIMap.remove(jobId);
            runningJobStateTimestampsIMap.remove(jobId);
        }
    }

    /**
     * Regression test: a {@link JobCleanupRecord} left behind by an older, already-dead generation
     * of the same job id must not permanently block {@link
     * CoordinatorService#repairMissingJobStateForRestore} for the current generation.
     *
     * <p>Before this fix, the fence used a jobId-only {@code containsKey} check, so this stale
     * record -- owned by initialization timestamp 100, while the currently registered {@link
     * JobInfo} is at timestamp 200 -- would make repair return {@code null} forever, since nothing
     * else ever revisits or clears a cleanup record whose owning generation is gone. The fix reuses
     * {@link CoordinatorService#getOwnedPendingCleanup}, which both ignores a foreign-generation
     * record for the purpose of blocking repair and clears it as a side effect.
     */
    @Test
    void testMissingStateRepairIgnoresStaleCleanupRecordFromForeignGeneration() {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();
        long staleInitializationTimestamp = 100L;
        long currentInitializationTimestamp = 200L;
        JobInfo currentJobInfo =
                new JobInfo(
                        currentInitializationTimestamp,
                        createJobData(jobId, false, "stream_fake_to_console.conf"));

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);

        runningJobInfoIMap.put(jobId, currentJobInfo);
        pendingJobCleanupIMap.put(
                jobId,
                new JobCleanupRecord(
                        staleInitializationTimestamp,
                        JobStatus.FINISHED,
                        stateKeys(jobId),
                        stateKeys(jobId),
                        System.currentTimeMillis()));

        try {
            Assertions.assertEquals(
                    JobStatus.CREATED,
                    coordinatorService.repairMissingJobStateForRestore(jobId, currentJobInfo));
            assertCreatedRepairTimestamps(
                    runningJobStateTimestampsIMap.get(jobId), currentInitializationTimestamp);
            Assertions.assertFalse(
                    pendingJobCleanupIMap.containsKey(jobId),
                    "stale foreign-generation cleanup record must be cleared, not left behind"
                            + " blocking future repairs of this job id");
        } finally {
            runningJobInfoIMap.remove(jobId);
            runningJobStateIMap.remove(jobId);
            runningJobStateTimestampsIMap.remove(jobId);
            pendingJobCleanupIMap.remove(jobId);
        }
    }

    /**
     * Captures this coordinator's cleanup tasks without replacing the shared server's scheduler.
     * The spy retains real maps and production cleanup methods; only scheduling is recorded.
     */
    private CoordinatorService capturedCleanupCoordinator(
            List<Runnable> attempts, List<Long> delays) throws Exception {
        ScheduledExecutorService monitor = Mockito.mock(ScheduledExecutorService.class);
        Mockito.doAnswer(
                        call -> {
                            attempts.add(call.getArgument(0));
                            delays.add(call.getArgument(1));
                            return Mockito.mock(ScheduledFuture.class);
                        })
                .when(monitor)
                .schedule(
                        Mockito.any(Runnable.class),
                        Mockito.anyLong(),
                        Mockito.eq(TimeUnit.MILLISECONDS));
        SeaTunnelServer monitoredServer =
                Mockito.mock(SeaTunnelServer.class, AdditionalAnswers.delegatesTo(server));
        Mockito.doReturn(monitor).when(monitoredServer).getMonitorService();
        CoordinatorService coordinator = Mockito.spy(server.getCoordinatorService());
        ReflectionUtils.setField(
                coordinator, CoordinatorService.class, "seaTunnelServer", monitoredServer);
        return coordinator;
    }

    /**
     * Exercises the internal admission check under its required distributed job fence. The direct
     * test seam avoids starting a successor job that could obscure removed state.
     */
    private void checkSubmissionFence(CoordinatorService coordinator, long jobId, boolean savepoint)
            throws Exception {
        IMap<Long, JobCleanupRecord> pending =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        pending.lock(jobId);
        try {
            coordinator.validateJobSubmissionFence(jobId, savepoint);
        } finally {
            pending.unlock(jobId);
        }
    }

    private void assertCreatedRepairTimestamps(Long[] timestamps, long initializationTimestamp) {
        Assertions.assertNotNull(timestamps);
        Assertions.assertEquals(
                Long.valueOf(initializationTimestamp),
                timestamps[JobStatus.INITIALIZING.ordinal()]);
        Assertions.assertNotNull(timestamps[JobStatus.CREATED.ordinal()]);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, JobMaster> getRunningJobMasterMap(CoordinatorService coordinatorService)
            throws Exception {
        Field field = CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        field.setAccessible(true);
        return (Map<Long, JobMaster>) field.get(coordinatorService);
    }

    private Set<Object> stateKeys(Object... keys) {
        Set<Object> stateKeys = new LinkedHashSet<>();
        for (Object key : keys) {
            stateKeys.add(key);
        }
        return stateKeys;
    }

    private void invokeRestoreJobFromMasterActiveSwitch(
            CoordinatorService coordinatorService, long jobId, JobInfo jobInfo) throws Exception {
        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);
        method.invoke(coordinatorService, jobId, jobInfo);
    }

    private Data createJobData(long jobId, boolean isStartWithSavePoint) {
        return createJobData(jobId, isStartWithSavePoint, "batch_fake_to_console.conf");
    }

    private Data createJobData(long jobId, boolean isStartWithSavePoint, String configFile) {
        return createJobData(jobId, isStartWithSavePoint, configFile, Collections.emptyMap());
    }

    private Data createJobData(
            long jobId,
            boolean isStartWithSavePoint,
            String configFile,
            Map<String, Object> envOptions) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(configFile, "job-cleanup-submit-test", jobId);
        logicalDag.getJobConfig().getEnvOptions().putAll(envOptions);
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        isStartWithSavePoint,
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return nodeEngine.getSerializationService().toData(jobImmutableInformation);
    }

    private void storeCheckpoint(long jobId) throws CheckpointStorageException {
        long now = System.currentTimeMillis();
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        1,
                        1,
                        now,
                        CheckpointType.COMPLETED_POINT_TYPE,
                        now,
                        new HashMap<>(),
                        new HashMap<>());
        server.getCheckpointService()
                .getCheckpointStorage()
                .storeCheckPoint(
                        PipelineState.builder()
                                .jobId(String.valueOf(jobId))
                                .pipelineId(1)
                                .checkpointId(1)
                                .states(new ProtoStuffSerializer().serialize(completedCheckpoint))
                                .build());
    }
}
