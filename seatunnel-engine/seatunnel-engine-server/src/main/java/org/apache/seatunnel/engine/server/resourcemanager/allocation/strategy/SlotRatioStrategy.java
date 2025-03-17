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

package org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** SlotRatioStrategy is a strategy that selects the worker with the lowest slot usage rate. */
public class SlotRatioStrategy implements SlotAllocationStrategy {

    @Getter @Setter private Map<Address, SlotAssignedProfile> workerAssignedSlots;

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers) {

        Optional<WorkerProfile> workerProfile =
                availableWorkers.stream().min(Comparator.comparingDouble(this::calculateSlotUsage));
        workerProfile.ifPresent(
                profile -> {
                    workerAssignedSlots.merge(
                            profile.getAddress(),
                            new SlotAssignedProfile(0.0, 1, profile.getAssignedSlots().length),
                            (oldVal, newVal) ->
                                    new SlotAssignedProfile(
                                            0.0,
                                            oldVal.getCurrentTaskAssignedSlotsNum() + 1,
                                            oldVal.getAssignedSlotsNum()));
                });
        return workerProfile;
    }

    /**
     * Calculate the slot usage rate of the worker
     *
     * @param worker WorkerProfile
     * @return slot usage rate, range 0.0-1.0
     */
    private double calculateSlotUsage(WorkerProfile worker) {
        SlotAssignedProfile slotAssignedProfile = workerAssignedSlots.get(worker.getAddress());
        // If we manually record the number of assigned slots, we use that number, since
        // worker.getAssignedSlots is not updated in real time.
        int assignedSlots =
                (slotAssignedProfile != null)
                        ? slotAssignedProfile.getCurrentTaskAssignedSlotsNum()
                        : worker.getAssignedSlots().length;
        workerAssignedSlots.put(
                worker.getAddress(), new SlotAssignedProfile(0.0, assignedSlots, 0));

        int totalSlots = worker.getUnassignedSlots().length + worker.getAssignedSlots().length;
        if (totalSlots == 0) {
            // When using dynamic slots, the default usage rate is 50%
            return 0.5;
        }

        return (double) assignedSlots / totalSlots;
    }
}
