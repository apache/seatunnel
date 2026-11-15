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

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResourceUtilsTest {

    @Test
    void nullSlotIsRejectedEvenWhenEveryTaskHasAnEntry() {
        JobMaster jobMaster = mock(JobMaster.class);
        PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
        SubPlan subPlan = mock(SubPlan.class);
        PhysicalVertex vertex = mock(PhysicalVertex.class);
        TaskGroupLocation taskLocation = new TaskGroupLocation(1L, 2, 3L);
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplied = new HashMap<>();
        preApplied.put(taskLocation, null);

        configure(jobMaster, physicalPlan, subPlan, Collections.singletonList(vertex), preApplied);
        when(vertex.getTaskGroupLocation()).thenReturn(taskLocation);

        assertThrows(
                NoEnoughResourceException.class,
                () -> ResourceUtils.applyResourceForPipeline(jobMaster, subPlan));
    }

    @Test
    void exceptionalFutureRetainsSuccessfulSlotsForCleanup() {
        assertFailedFutureRetainsSuccessfulSlots(false);
    }

    @Test
    void canceledFutureRetainsSuccessfulSlotsForCleanup() {
        assertFailedFutureRetainsSuccessfulSlots(true);
    }

    private static void assertFailedFutureRetainsSuccessfulSlots(boolean cancelFuture) {
        JobMaster jobMaster = mock(JobMaster.class);
        PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
        SubPlan subPlan = mock(SubPlan.class);
        PhysicalVertex successfulVertex = mock(PhysicalVertex.class);
        PhysicalVertex failedVertex = mock(PhysicalVertex.class);
        TaskGroupLocation successfulLocation = new TaskGroupLocation(1L, 2, 3L);
        TaskGroupLocation failedLocation = new TaskGroupLocation(1L, 2, 4L);
        PipelineLocation pipelineLocation = new PipelineLocation(1L, 2);
        SlotProfile successfulSlot = mock(SlotProfile.class);
        CompletableFuture<SlotProfile> failedFuture = new CompletableFuture<>();
        if (cancelFuture) {
            failedFuture.cancel(false);
        } else {
            failedFuture.completeExceptionally(new RuntimeException("slot request failed"));
        }
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplied = new HashMap<>();
        preApplied.put(successfulLocation, CompletableFuture.completedFuture(successfulSlot));
        preApplied.put(failedLocation, failedFuture);

        configure(
                jobMaster,
                physicalPlan,
                subPlan,
                Arrays.asList(successfulVertex, failedVertex),
                preApplied);
        when(subPlan.getPipelineLocation()).thenReturn(pipelineLocation);
        when(successfulVertex.getTaskGroupLocation()).thenReturn(successfulLocation);
        when(failedVertex.getTaskGroupLocation()).thenReturn(failedLocation);

        assertThrows(
                NoEnoughResourceException.class,
                () -> ResourceUtils.applyResourceForPipeline(jobMaster, subPlan));

        verify(jobMaster)
                .setOwnedSlotProfiles(
                        eq(pipelineLocation),
                        eq(Collections.singletonMap(successfulLocation, successfulSlot)));
    }

    private static void configure(
            JobMaster jobMaster,
            PhysicalPlan physicalPlan,
            SubPlan subPlan,
            List<PhysicalVertex> vertices,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplied) {
        when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);
        when(physicalPlan.getPreApplyResourceFutures()).thenReturn(preApplied);
        when(subPlan.getCoordinatorVertexList()).thenReturn(Collections.emptyList());
        when(subPlan.getPhysicalVertexList()).thenReturn(vertices);
        when(subPlan.getJobMaster()).thenReturn(jobMaster);
        when(subPlan.getPipelineLocation()).thenReturn(new PipelineLocation(1L, 2));
    }
}
