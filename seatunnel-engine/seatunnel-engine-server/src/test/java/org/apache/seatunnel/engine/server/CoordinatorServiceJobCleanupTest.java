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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.master.cleanup.JobCleanupRecord;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;

class CoordinatorServiceJobCleanupTest extends AbstractSeaTunnelServerTest {

    @AfterEach
    void clearPendingCleanupRecords() {
        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP).clear();
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

    @Test
    void testSubmitStartWithSavePointAllowedWhenCleanupStillPending() {
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

        Assertions.assertDoesNotThrow(
                () -> coordinatorService.submitJob(jobId, createJobData(jobId, true), true).join());
    }

    @Test
    void testSubmitStartWithSavePointClearsStaleTerminalState() {
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

        Assertions.assertDoesNotThrow(
                () ->
                        coordinatorService
                                .submitJob(
                                        jobId,
                                        createJobData(jobId, true, "stream_fake_to_console.conf"),
                                        true)
                                .join());

        Assertions.assertFalse(
                pendingJobCleanupIMap.containsKey(jobId),
                "restore submit should consume stale pending cleanup record");
        Assertions.assertNotEquals(JobStatus.SAVEPOINT_DONE, runningJobStateIMap.get(jobId));
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

        Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId));
        Assertions.assertFalse(runningJobStateIMap.containsKey(jobId));
        Assertions.assertFalse(runningJobStateIMap.containsKey(pipelineLocation));
        Assertions.assertFalse(runningJobStateIMap.containsKey(taskGroupLocation));
        Assertions.assertFalse(runningJobStateIMap.containsKey(checkpointStateKey));
        Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(jobId));
        Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(pipelineLocation));
        Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(taskGroupLocation));
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

        IMap<Long, JobInfo> spiedRunningJobInfoIMap = Mockito.spy(runningJobInfoIMap);
        doThrow(new RetryableHazelcastException("loading"))
                .when(spiedRunningJobInfoIMap)
                .get(jobId);
        ReflectionUtils.setField(coordinatorService, "runningJobInfoIMap", spiedRunningJobInfoIMap);

        try {
            invokeRestoreJobFromMasterActiveSwitch(coordinatorService, jobId, jobInfo);
        } finally {
            ReflectionUtils.setField(coordinatorService, "runningJobInfoIMap", runningJobInfoIMap);
        }

        Long[] jobStateTimestamps = runningJobStateTimestampsIMap.get(jobId);
        Assertions.assertNotNull(jobStateTimestamps);
        Assertions.assertEquals(
                initializationTimestamp, jobStateTimestamps[JobStatus.INITIALIZING.ordinal()]);
        Assertions.assertTrue(
                coordinatorService.getPendingJobQueue().contains(jobId)
                        || getRunningJobMasterMap(coordinatorService).containsKey(jobId));
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
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(configFile, "job-cleanup-submit-test", jobId);
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
