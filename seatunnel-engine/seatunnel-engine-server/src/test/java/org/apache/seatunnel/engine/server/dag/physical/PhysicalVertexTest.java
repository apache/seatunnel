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
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.map.IMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that only intentionally closed unbounded source task groups are recoverable during a
 * running-pipeline master restore.
 */
class PhysicalVertexTest extends AbstractSeaTunnelServerTest<PhysicalVertexTest> {

    @Test
    void shouldTreatFinishedUnboundedSourceTaskGroupAsRecoverable() {
        SourceSeaTunnelTask<?, ?> sourceTask = mock(SourceSeaTunnelTask.class);
        when(sourceTask.getTaskID()).thenReturn(11L);
        when(sourceTask.isUnboundedSourceTask()).thenReturn(true);
        Task transformTask = mock(Task.class);
        when(transformTask.getTaskID()).thenReturn(12L);

        PhysicalVertex physicalVertex =
                newPhysicalVertex(
                        ExecutionState.FINISHED, "idle-source", sourceTask, transformTask);

        Assertions.assertTrue(physicalVertex.isRecoverableFinishedStateForRunningPipelineRestore());
    }

    @Test
    void shouldRejectFinishedBoundedSourceTaskGroupAsRecoverable() {
        SourceSeaTunnelTask<?, ?> sourceTask = mock(SourceSeaTunnelTask.class);
        when(sourceTask.getTaskID()).thenReturn(21L);
        when(sourceTask.isUnboundedSourceTask()).thenReturn(false);

        PhysicalVertex physicalVertex =
                newPhysicalVertex(ExecutionState.FINISHED, "bounded-source", sourceTask);

        Assertions.assertFalse(
                physicalVertex.isRecoverableFinishedStateForRunningPipelineRestore());
    }

    @Test
    void shouldRejectFinishedNonSourceTaskGroupAsRecoverable() {
        Task transformTask = mock(Task.class);
        when(transformTask.getTaskID()).thenReturn(31L);

        PhysicalVertex physicalVertex =
                newPhysicalVertex(ExecutionState.FINISHED, "transform", transformTask);

        Assertions.assertFalse(
                physicalVertex.isRecoverableFinishedStateForRunningPipelineRestore());
    }

    @Test
    void shouldRejectRunningSourceTaskGroupAsRecoverable() {
        SourceSeaTunnelTask<?, ?> sourceTask = mock(SourceSeaTunnelTask.class);
        when(sourceTask.getTaskID()).thenReturn(41L);
        when(sourceTask.isUnboundedSourceTask()).thenReturn(true);

        PhysicalVertex physicalVertex =
                newPhysicalVertex(ExecutionState.RUNNING, "running-source", sourceTask);

        Assertions.assertFalse(
                physicalVertex.isRecoverableFinishedStateForRunningPipelineRestore());
    }

    private PhysicalVertex newPhysicalVertex(
            ExecutionState executionState, String taskGroupName, Task... tasks) {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(1L, 1, taskGroupName.hashCode() & Integer.MAX_VALUE);
        TaskGroupDefaultImpl taskGroup =
                new TaskGroupDefaultImpl(taskGroupLocation, taskGroupName, Arrays.asList(tasks));

        IMap<Object, Object> runningJobStateIMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("physical-vertex-test-running-state-" + UUID.randomUUID());
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap("physical-vertex-test-state-timestamps-" + UUID.randomUUID());
        runningJobStateIMap.put(taskGroupLocation, executionState);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("test");
        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);
        when(jobImmutableInformation.getJobId()).thenReturn(1L);
        when(jobImmutableInformation.getJobConfig()).thenReturn(jobConfig);

        FlakeIdGenerator flakeIdGenerator =
                nodeEngine
                        .getHazelcastInstance()
                        .getFlakeIdGenerator("physical-vertex-test-" + UUID.randomUUID());

        return new PhysicalVertex(
                0,
                1,
                taskGroup,
                flakeIdGenerator,
                1,
                1,
                Collections.emptyList(),
                Collections.emptyList(),
                jobImmutableInformation,
                System.currentTimeMillis(),
                nodeEngine,
                runningJobStateIMap,
                runningJobStateTimestampsIMap);
    }
}
