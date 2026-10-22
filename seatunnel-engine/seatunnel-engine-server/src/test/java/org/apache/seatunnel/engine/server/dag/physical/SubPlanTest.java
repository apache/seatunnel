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

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers checkpoint coordinator restore decisions for running pipelines with an intentionally
 * closed idle source reader.
 */
class SubPlanTest extends AbstractSeaTunnelServerTest<SubPlanTest> {

    @Test
    void shouldKeepCheckpointCoordinatorStartedWithFinishedIdlePhysicalTask() {
        CheckpointManager checkpointManager = mock(CheckpointManager.class);
        JobMaster jobMaster = mock(JobMaster.class);
        when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);

        PhysicalVertex coordinator = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex incrementalReader = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex idleReader = mockPhysicalVertex(ExecutionState.FINISHED);
        when(idleReader.isRecoverableFinishedStateForRunningPipelineRestore()).thenReturn(true);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            SubPlan subPlan =
                    newSubPlan(
                            executorService,
                            Arrays.asList(incrementalReader, idleReader),
                            Collections.singletonList(coordinator));

            subPlan.setJobMaster(jobMaster);
            subPlan.setCurrPipelineStatus(PipelineStatus.RUNNING);
            subPlan.restorePipelineState();

            verify(checkpointManager).reportedPipelineRunning(1, true);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldResetCheckpointCoordinatorWithFinishedNonIdlePhysicalTask() {
        CheckpointManager checkpointManager = mock(CheckpointManager.class);
        JobMaster jobMaster = mock(JobMaster.class);
        when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);

        PhysicalVertex coordinator = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex incrementalReader = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex finishedTransform = mockPhysicalVertex(ExecutionState.FINISHED);
        when(finishedTransform.isRecoverableFinishedStateForRunningPipelineRestore())
                .thenReturn(false);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            SubPlan subPlan =
                    newSubPlan(
                            executorService,
                            Arrays.asList(incrementalReader, finishedTransform),
                            Collections.singletonList(coordinator));

            subPlan.setJobMaster(jobMaster);
            subPlan.setCurrPipelineStatus(PipelineStatus.RUNNING);
            subPlan.restorePipelineState();

            verify(checkpointManager).reportedPipelineRunning(1, false);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldRejectFailedPhysicalTaskDuringRunningRestore() {
        CheckpointManager checkpointManager = mock(CheckpointManager.class);
        JobMaster jobMaster = mock(JobMaster.class);
        when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);

        PhysicalVertex coordinator = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex incrementalReader = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex failedReader = mockPhysicalVertex(ExecutionState.FAILED);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            SubPlan subPlan =
                    newSubPlan(
                            executorService,
                            Arrays.asList(incrementalReader, failedReader),
                            Collections.singletonList(coordinator));

            subPlan.setJobMaster(jobMaster);
            subPlan.setCurrPipelineStatus(PipelineStatus.RUNNING);
            subPlan.restorePipelineState();

            verify(checkpointManager).reportedPipelineRunning(1, false);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldTreatUnknownCoordinatorStateAsNotStarted() {
        PhysicalVertex coordinator = mockPhysicalVertex(null);
        PhysicalVertex runningReader = mockPhysicalVertex(ExecutionState.RUNNING);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            SubPlan subPlan =
                    newSubPlan(
                            executorService,
                            Collections.singletonList(runningReader),
                            Collections.singletonList(coordinator));

            Assertions.assertFalse(subPlan.isCheckpointCoordinatorAlreadyStarted());
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldOnlyTreatRunningOrRecoverableFinishedPhysicalVertexAsReady() {
        PhysicalVertex runningReader = mockPhysicalVertex(ExecutionState.RUNNING);
        PhysicalVertex idleReader = mockPhysicalVertex(ExecutionState.FINISHED);
        when(idleReader.isRecoverableFinishedStateForRunningPipelineRestore()).thenReturn(true);
        PhysicalVertex finishedTransform = mockPhysicalVertex(ExecutionState.FINISHED);
        when(finishedTransform.isRecoverableFinishedStateForRunningPipelineRestore())
                .thenReturn(false);
        PhysicalVertex failedReader = mockPhysicalVertex(ExecutionState.FAILED);

        Assertions.assertTrue(SubPlan.isRecoverableRunningState(runningReader));
        Assertions.assertTrue(SubPlan.isRecoverableRunningState(idleReader));
        Assertions.assertFalse(SubPlan.isRecoverableRunningState(finishedTransform));
        Assertions.assertFalse(SubPlan.isRecoverableRunningState(failedReader));
    }

    private SubPlan newSubPlan(
            ExecutorService executorService,
            List<PhysicalVertex> physicalVertexList,
            List<PhysicalVertex> coordinatorVertexList) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("test");

        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);
        when(jobImmutableInformation.getJobId()).thenReturn(1L);
        when(jobImmutableInformation.getJobConfig()).thenReturn(jobConfig);

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("sub-plan-test-running-state-" + UUID.randomUUID());
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("sub-plan-test-state-timestamps-" + UUID.randomUUID());

        return new SubPlan(
                1,
                1,
                System.currentTimeMillis(),
                physicalVertexList,
                coordinatorVertexList,
                jobImmutableInformation,
                executorService,
                runningJobStateIMap,
                runningJobStateTimestampsIMap,
                Collections.emptyMap());
    }

    private PhysicalVertex mockPhysicalVertex(ExecutionState executionState) {
        PhysicalVertex physicalVertex = mock(PhysicalVertex.class);
        when(physicalVertex.getExecutionState()).thenReturn(executionState);
        return physicalVertex;
    }
}
