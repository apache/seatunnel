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
import org.apache.seatunnel.engine.server.execution.ExecutionState;

import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SubPlanDeploymentTest {

    @Test
    void pipelineIsNotRunningWhenAnyTaskDeploymentFailed() {
        assertFalse(
                SubPlan.areAllVerticesRunningOrFinished(
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING)),
                        Collections.singletonList(vertexIn(ExecutionState.FAILED))));
    }

    @Test
    void pipelineIsNotRunningWhenCoordinatorDeploymentFailed() {
        assertFalse(
                SubPlan.areAllVerticesRunningOrFinished(
                        Collections.singletonList(vertexIn(ExecutionState.FAILED)),
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING))));
    }

    @Test
    void pipelineIsRunningOnlyAfterEveryVertexIsRunning() {
        assertTrue(
                SubPlan.areAllVerticesRunningOrFinished(
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING)),
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING))));
    }

    @Test
    void finishedVertexDuringDeploymentStillPromotesPipelineToRunning() {
        AtomicReference<PipelineStatus> state = new AtomicReference<>(PipelineStatus.DEPLOYING);
        SubPlan subPlan =
                subPlanIn(
                        state,
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING)),
                        Collections.singletonList(vertexIn(ExecutionState.FINISHED)));

        subPlan.startSubPlanStateProcess();

        assertTrue(PipelineStatus.RUNNING.equals(state.get()));
        assertTrue(PipelineStatus.RUNNING.equals(subPlan.getPipelineState()));
    }

    @Test
    void failedVertexDuringDeploymentDoesNotPromotePipelineToRunning() {
        AtomicReference<PipelineStatus> state = new AtomicReference<>(PipelineStatus.DEPLOYING);
        SubPlan subPlan =
                subPlanIn(
                        state,
                        Collections.singletonList(vertexIn(ExecutionState.FAILED)),
                        Collections.singletonList(vertexIn(ExecutionState.RUNNING)));

        subPlan.startSubPlanStateProcess();

        assertTrue(PipelineStatus.DEPLOYING.equals(state.get()));
        assertTrue(PipelineStatus.DEPLOYING.equals(subPlan.getPipelineState()));
    }

    @SuppressWarnings("unchecked")
    private static SubPlan subPlanIn(
            AtomicReference<PipelineStatus> state,
            List<PhysicalVertex> tasks,
            List<PhysicalVertex> coordinators) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("deployment-state");
        JobImmutableInformation jobInformation = mock(JobImmutableInformation.class);
        when(jobInformation.getJobId()).thenReturn(1L);
        when(jobInformation.getJobConfig()).thenReturn(jobConfig);
        IMap<Object, Object> states = mock(IMap.class);
        when(states.get(any())).thenAnswer(ignored -> state.get());
        doAnswer(
                        invocation -> {
                            state.set(invocation.getArgument(1));
                            return null;
                        })
                .when(states)
                .set(any(), any());
        IMap<Object, Long[]> timestamps = mock(IMap.class);
        when(timestamps.get(any())).thenReturn(new Long[PipelineStatus.values().length]);
        return new SubPlan(
                1,
                1,
                0L,
                tasks,
                coordinators,
                jobInformation,
                mock(ExecutorService.class),
                states,
                timestamps,
                Collections.emptyMap());
    }

    private static PhysicalVertex vertexIn(ExecutionState state) {
        PhysicalVertex vertex = mock(PhysicalVertex.class);
        when(vertex.getExecutionState()).thenReturn(state);
        return vertex;
    }
}
