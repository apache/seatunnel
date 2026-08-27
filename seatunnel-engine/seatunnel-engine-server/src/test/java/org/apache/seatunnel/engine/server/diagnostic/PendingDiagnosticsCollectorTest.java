/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.diagnostic;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.PendingSourceState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PendingDiagnosticsCollectorTest {

    @Test
    public void testCollectJobDiagnosticWithFailures() {
        JobMaster jobMaster = Mockito.mock(JobMaster.class);
        Mockito.when(jobMaster.getJobId()).thenReturn(1000L);
        JobImmutableInformation jobImmutableInformation =
                Mockito.mock(JobImmutableInformation.class);
        Mockito.when(jobImmutableInformation.getJobName()).thenReturn("test_job");
        Mockito.when(jobMaster.getJobImmutableInformation()).thenReturn(jobImmutableInformation);
        Mockito.when(jobMaster.getJobStatus()).thenReturn(JobStatus.PENDING);

        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        Mockito.when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);

        SubPlan subPlan = Mockito.mock(SubPlan.class);
        Mockito.when(subPlan.getPipelineId()).thenReturn(1);
        Mockito.when(subPlan.getPipelineFullName()).thenReturn("pipeline-1");

        PhysicalVertex vertexSuccess = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationSuccess = new TaskGroupLocation(1000L, 1, 1L);
        Mockito.when(vertexSuccess.getTaskGroupLocation()).thenReturn(locationSuccess);
        Mockito.when(vertexSuccess.getTaskFullName()).thenReturn("task-success");

        PhysicalVertex vertexFailA = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationFailA = new TaskGroupLocation(1000L, 1, 2L);
        Mockito.when(vertexFailA.getTaskGroupLocation()).thenReturn(locationFailA);
        Mockito.when(vertexFailA.getTaskFullName()).thenReturn("task-fail-a");

        PhysicalVertex vertexFailB = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationFailB = new TaskGroupLocation(1000L, 1, 3L);
        Mockito.when(vertexFailB.getTaskGroupLocation()).thenReturn(locationFailB);
        Mockito.when(vertexFailB.getTaskFullName()).thenReturn("task-fail-b");

        Mockito.when(subPlan.getCoordinatorVertexList()).thenReturn(Collections.emptyList());
        Mockito.when(subPlan.getPhysicalVertexList())
                .thenReturn(Arrays.asList(vertexSuccess, vertexFailA, vertexFailB));
        Mockito.when(physicalPlan.getPipelineList()).thenReturn(Collections.singletonList(subPlan));

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        CompletableFuture<SlotProfile> successFuture =
                CompletableFuture.completedFuture(Mockito.mock(SlotProfile.class));
        futures.put(locationSuccess, successFuture);

        CompletableFuture<SlotProfile> failFutureA = new CompletableFuture<>();
        failFutureA.completeExceptionally(new RuntimeException("no slot available"));
        futures.put(locationFailA, failFutureA);

        CompletableFuture<SlotProfile> failFutureB = new CompletableFuture<>();
        failFutureB.completeExceptionally(new RuntimeException("worker busy"));
        futures.put(locationFailB, failFutureB);

        Mockito.when(physicalPlan.getPreApplyResourceFutures()).thenReturn(futures);

        PendingJobInfo pendingJobInfo = new PendingJobInfo(PendingSourceState.SUBMIT, jobMaster);

        ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
        SlotProfile blockingSlot = Mockito.mock(SlotProfile.class);
        Mockito.when(blockingSlot.getOwnerJobID()).thenReturn(2000L);
        Mockito.when(resourceManager.getAssignedSlots(Mockito.anyMap()))
                .thenReturn(Collections.singletonList(blockingSlot));

        PendingJobDiagnostic diagnostic =
                PendingDiagnosticsCollector.collectJobDiagnostic(
                        pendingJobInfo, Collections.emptyMap(), resourceManager);

        Assertions.assertEquals(2, diagnostic.getLackingTaskGroups());
        Assertions.assertEquals("REQUEST_FAILED", diagnostic.getFailureReason());
        Assertions.assertEquals(1, diagnostic.getBlockingJobIds().size());
        Assertions.assertEquals(3, diagnostic.getPipelines().get(0).getTotalTaskGroups());
        Assertions.assertEquals(2, diagnostic.getPipelines().get(0).getLackingTaskGroups());
    }
}
