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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.cleanup.JobCleanupRecord;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;

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
    void testTerminalZombieJobWithoutCleanupRecordRemovesRunningState() throws Exception {
        CoordinatorService coordinatorService = server.getCoordinatorService();
        long jobId = System.currentTimeMillis();

        IMap<Long, JobInfo> runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);

        runningJobInfoIMap.put(jobId, new JobInfo(100L, null));
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);
        runningJobStateTimestampsIMap.put(jobId, new Long[JobStatus.values().length]);

        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);

        method.invoke(coordinatorService, jobId, runningJobInfoIMap.get(jobId));

        Assertions.assertFalse(runningJobInfoIMap.containsKey(jobId));
        Assertions.assertFalse(runningJobStateIMap.containsKey(jobId));
        Assertions.assertFalse(runningJobStateTimestampsIMap.containsKey(jobId));
    }

    private Set<Object> stateKeys(Object... keys) {
        Set<Object> stateKeys = new LinkedHashSet<>();
        for (Object key : keys) {
            stateKeys.add(key);
        }
        return stateKeys;
    }

    private Data createJobData(long jobId, boolean isStartWithSavePoint) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(
                        "batch_fake_to_console.conf", "job-cleanup-submit-test", jobId);
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
}
