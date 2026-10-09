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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class SubPlanRestoreStateTest {

    /**
     * After a master switch, a task group whose idle reader was already closed (FINISHED) must not
     * prevent the checkpoint coordinator from being restored as already started, otherwise no
     * checkpoint is ever triggered again because running tasks never re-report READY_START.
     */
    @Test
    void testRestoreRunningPipelineWithIdleClosedTaskGroupIsAlreadyStarted() {
        TaskGroupLocation enumeratorGroup = new TaskGroupLocation(1L, 1, 1);
        TaskGroupLocation runningReaderGroup = new TaskGroupLocation(1L, 1, 2);
        TaskGroupLocation idleClosedReaderGroup = new TaskGroupLocation(1L, 1, 3);

        CheckpointManager checkpointManager = Mockito.mock(CheckpointManager.class);
        JobMaster jobMaster = Mockito.mock(JobMaster.class);
        Mockito.when(jobMaster.getCheckpointManager()).thenReturn(checkpointManager);

        SubPlan subPlan = Mockito.mock(SubPlan.class);
        Mockito.doCallRealMethod().when(subPlan).restorePipelineState();
        ReflectionUtils.setField(subPlan, "jobMaster", jobMaster);
        Mockito.when(subPlan.getPipelineState()).thenReturn(PipelineStatus.RUNNING);
        Mockito.when(subPlan.getPipelineLocation()).thenReturn(new PipelineLocation(1L, 1));
        List<PhysicalVertex> coordinators =
                Collections.singletonList(vertex(enumeratorGroup, ExecutionState.RUNNING));
        List<PhysicalVertex> tasks =
                Arrays.asList(
                        vertex(runningReaderGroup, ExecutionState.RUNNING),
                        vertex(idleClosedReaderGroup, ExecutionState.FINISHED));
        Mockito.when(subPlan.getCoordinatorVertexList()).thenReturn(coordinators);
        Mockito.when(subPlan.getPhysicalVertexList()).thenReturn(tasks);

        subPlan.restorePipelineState();

        Mockito.verify(checkpointManager)
                .reportedPipelineRunning(1, true, Collections.singleton(idleClosedReaderGroup));
    }

    private static PhysicalVertex vertex(TaskGroupLocation location, ExecutionState state) {
        PhysicalVertex vertex = Mockito.mock(PhysicalVertex.class);
        Mockito.when(vertex.getTaskGroupLocation()).thenReturn(location);
        Mockito.when(vertex.getExecutionState()).thenReturn(state);
        return vertex;
    }
}
