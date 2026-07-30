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

package org.apache.seatunnel.engine.server.trace;

import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

class RunningJobSlotUsageBuilderTest {

    @Test
    void shouldAggregateSlotUsageForRunningJobsOnly() throws UnknownHostException {
        Address firstWorker = new Address("127.0.0.1", 5801);
        Address secondWorker = new Address("127.0.0.1", 5802);
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                new HashMap<>();
        ownedSlotProfiles.put(
                new PipelineLocation(1L, 1),
                taskGroupSlots(slot(1L, firstWorker, 1), slot(1L, secondWorker, 2)));
        ownedSlotProfiles.put(
                new PipelineLocation(1L, 2), taskGroupSlots(slot(1L, firstWorker, 3), null));
        ownedSlotProfiles.put(
                new PipelineLocation(3L, 1), taskGroupSlots(slot(3L, firstWorker, 4)));

        SlotProfile reusedByOtherJob = slot(2L, firstWorker, 3);
        List<SlotProfile> assignedSlots =
                new ArrayList<>(
                        Arrays.asList(
                                ownedSlotProfiles
                                        .get(new PipelineLocation(1L, 1))
                                        .values()
                                        .toArray(new SlotProfile[0])));
        assignedSlots.add(reusedByOtherJob);
        List<Map<String, Object>> result =
                RunningJobSlotUsageBuilder.build(
                        ownedSlotProfiles, new HashSet<>(Arrays.asList(1L, 2L)), assignedSlots);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("1", result.get(0).get("jobId"));
        Assertions.assertEquals(2, result.get(0).get("slotCount"));
        Map<Integer, Integer> firstPipelineSlotCounts = pipelineSlotCounts(result.get(0));
        Map<String, Integer> firstWorkerSlotCounts = workerSlotCounts(result.get(0));
        Assertions.assertEquals(2, firstPipelineSlotCounts.get(1));
        Assertions.assertFalse(firstPipelineSlotCounts.containsKey(2));
        Assertions.assertEquals(1, firstWorkerSlotCounts.get(firstWorker.toString()));
        Assertions.assertEquals(1, firstWorkerSlotCounts.get(secondWorker.toString()));
        Assertions.assertEquals("2", result.get(1).get("jobId"));
        Assertions.assertEquals(0, result.get(1).get("slotCount"));
        Assertions.assertTrue(pipelineSlotCounts(result.get(1)).isEmpty());
        Assertions.assertTrue(workerSlotCounts(result.get(1)).isEmpty());
    }

    private Map<TaskGroupLocation, SlotProfile> taskGroupSlots(SlotProfile... slots) {
        Map<TaskGroupLocation, SlotProfile> taskGroupSlots = new HashMap<>();
        for (int i = 0; i < slots.length; i++) {
            SlotProfile slot = slots[i];
            long jobId = slot == null ? 1L : slot.getOwnerJobID();
            taskGroupSlots.put(new TaskGroupLocation(jobId, 1, i), slot);
        }
        return taskGroupSlots;
    }

    private SlotProfile slot(long jobId, Address worker, int slotId) {
        SlotProfile slotProfile = new SlotProfile(worker, slotId, new ResourceProfile(), "test");
        slotProfile.assign(jobId);
        return slotProfile;
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, Integer> pipelineSlotCounts(Map<String, Object> usage) {
        return (Map<Integer, Integer>) usage.get("pipelineSlotCounts");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> workerSlotCounts(Map<String, Object> usage) {
        return (Map<String, Integer>) usage.get("workerSlotCounts");
    }
}
