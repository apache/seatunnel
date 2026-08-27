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

import org.apache.seatunnel.shade.com.google.common.collect.EvictingQueue;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.utils.SystemLoadCalculate;

import com.hazelcast.cluster.Address;
import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** SystemLoadStrategy is a strategy that selects the worker with the lowest system load. */
public class SystemLoadStrategy implements SlotAllocationStrategy {
    private final Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap;

    @Getter @Setter private Map<Address, SlotAssignedProfile> workerAssignedSlots;

    public SystemLoadStrategy(Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap) {
        this.workerLoadMap = workerLoadMap;
    }

    public SystemLoadStrategy() {
        this.workerLoadMap = new ConcurrentHashMap<>();
    }

    public void updateWorkerLoad(Address address, SystemLoadInfo systemLoadInfo) {
        workerLoadMap.computeIfAbsent(address, k -> EvictingQueue.create(5)).add(systemLoadInfo);
    }

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers) {
        Optional<WorkerProfile> workerProfile =
                availableWorkers.stream()
                        .max(
                                Comparator.comparingDouble(
                                        w -> calculateWeight(w, workerAssignedSlots)));

        workerProfile.ifPresent(
                profile -> {
                    workerAssignedSlots.merge(
                            profile.getAddress(),
                            new SlotAssignedProfile(0.0, 1, profile.getAssignedSlots().length),
                            (oldVal, newVal) ->
                                    new SlotAssignedProfile(
                                            oldVal.getSingleSlotUseResource(),
                                            oldVal.getCurrentTaskAssignedSlotsNum() + 1,
                                            oldVal.getAssignedSlotsNum()));
                });
        return workerProfile;
    }

    public Double calculateWeight(
            WorkerProfile workerProfile, Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();
        return systemLoadCalculate.calculate(
                workerLoadMap.get(workerProfile.getAddress()), workerProfile, workerAssignedSlots);
    }
}
