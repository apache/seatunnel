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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.shade.com.google.common.collect.EvictingQueue;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;

import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

public class SystemLoadCalculate {
    // Maximum number of records supported
    private static final int MAX_TIME_WINDOW = 5;
    // Time weight ratio configuration, arranged from new to old. Any length can be configured, and
    // the actual use will take min(current number of records, length of the weight array)
    private static final double[] TIME_WEIGHT_RATIOS = {4.0, 2.0, 2.0, 1.0, 1.0};
    // Resource weight configuration
    private static final double CPU_WEIGHT = 0.5;
    private static final double MEMORY_WEIGHT = 0.5;

    final double RESOURCE_AVAILABILITY_WEIGHT = 0.7;
    final double SLOT_WEIGHT = 0.3;

    private static class UtilizationData {
        private final double cpuUtilization;
        private final double memoryUtilization;

        public UtilizationData(double cpuUtilization, double memoryUtilization) {
            this.cpuUtilization = cpuUtilization;
            this.memoryUtilization = memoryUtilization;
        }
    }

    private final LinkedList<UtilizationData> utilizationHistory;

    public SystemLoadCalculate() {
        this.utilizationHistory = new LinkedList<>();
    }

    /** Add new resource utilization data */
    public void addUtilizationData(double cpuUtilization, double memoryUtilization) {
        // Validate input data
        if (cpuUtilization < 0
                || cpuUtilization > 1
                || memoryUtilization < 0
                || memoryUtilization > 1) {
            throw new IllegalArgumentException("Utilization values must be between 0 and 1");
        }

        if (utilizationHistory.size() >= MAX_TIME_WINDOW) {
            utilizationHistory.removeLast(); // Remove the oldest record
        }
        utilizationHistory.addFirst(new UtilizationData(cpuUtilization, memoryUtilization));
    }

    /** Generate corresponding time weights based on the actual number of records */
    private double[] generateTimeWeights() {
        int size = utilizationHistory.size();
        if (size == 0) return new double[0];

        // Determine the actual number of weights to use
        int weightCount = Math.min(size, TIME_WEIGHT_RATIOS.length);
        double[] weights = new double[size];
        double totalWeight = 0;

        // Allocate weights according to the configured ratio
        for (int i = 0; i < size; i++) {
            weights[i] =
                    (i < weightCount) ? TIME_WEIGHT_RATIOS[i] : TIME_WEIGHT_RATIOS[weightCount - 1];
            totalWeight += weights[i];
        }

        // Normalize weights so that the sum is 1
        for (int i = 0; i < size; i++) {
            weights[i] /= totalWeight;
        }

        return weights;
    }

    /** Calculate scheduling priority */
    public double calculateSchedulingPriority() {
        if (utilizationHistory.isEmpty()) {
            return 1.0; // If there is no historical data, return the highest priority
        }

        double[] timeWeights = generateTimeWeights();
        double prioritySum = 0.0;
        int index = 0;

        for (UtilizationData data : utilizationHistory) {
            // Calculate resource availability at the current time point
            double resourceAvailability = calculateResourceAvailability(data);
            // Apply time weight
            prioritySum += resourceAvailability * timeWeights[index++];
        }

        return prioritySum;
    }

    public double calculate(
            EvictingQueue<SystemLoadInfo> systemLoads,
            WorkerProfile workerProfile,
            Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        if (Objects.isNull(systemLoads) || systemLoads.isEmpty()) {
            // If the node load is not obtained, zero is returned. This only happens when the
            // service is just started and the load status has not yet been obtained.
            return 0.0;
        }
        systemLoads.forEach(
                v -> {
                    Double cpuPercentage = v.getCpuPercentage();
                    Double memPercentage = v.getMemPercentage();
                    this.addUtilizationData(cpuPercentage, memPercentage);
                });
        // step3.The comprehensive resource idle rate calculated
        double comprehensiveResourceAvailability = this.calculateSchedulingPriority();
        // step4
        double resourceAvailabilityStep4 =
                this.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        // step5
        double slotWeight = this.balanceFactor(workerProfile, workerAssignedSlots);
        return this.calculateResourceAvailability(resourceAvailabilityStep4, slotWeight);
    }

    public double calculateResourceAvailability(
            double resourceAvailabilityStep4, double slotWeight) {
        return RESOURCE_AVAILABILITY_WEIGHT * resourceAvailabilityStep4 + SLOT_WEIGHT * slotWeight;
    }

    /** Calculate resource availability at a single point in time */
    private double calculateResourceAvailability(UtilizationData data) {
        double cpuAvailability = 1.0 - data.cpuUtilization;
        double memoryAvailability = 1.0 - data.memoryUtilization;

        return (cpuAvailability * CPU_WEIGHT + memoryAvailability * MEMORY_WEIGHT)
                / (CPU_WEIGHT + MEMORY_WEIGHT);
    }

    /** step4. The comprehensive resource idle rate calculated */
    public double calculateComprehensiveResourceAvailability(
            double comprehensiveResourceAvailability,
            WorkerProfile workerProfile,
            Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        // Start step 4
        // Number of assigned slots
        int assignedSlotsNum = workerProfile.getAssignedSlots().length;
        // Resource usage per slot, default is 0.1
        double singleSlotUseResource = 0.1;
        SlotAssignedProfile slotAssignedProfile;
        if (workerAssignedSlots.get(workerProfile.getAddress()) == null) {
            if (assignedSlotsNum != 0) {
                singleSlotUseResource =
                        Math.round(
                                        ((1.0 - comprehensiveResourceAvailability)
                                                        / assignedSlotsNum)
                                                * 100.0)
                                / 100.0;
            }
            slotAssignedProfile =
                    workerAssignedSlots.getOrDefault(
                            workerProfile.getAddress(),
                            new SlotAssignedProfile(singleSlotUseResource, 0, assignedSlotsNum));
        } else {
            slotAssignedProfile = workerAssignedSlots.get(workerProfile.getAddress());
            singleSlotUseResource = slotAssignedProfile.getSingleSlotUseResource();
        }

        Integer assignedTimesForTask = slotAssignedProfile.getCurrentTaskAssignedSlotsNum();
        // Calculate the weight of the current task on the Worker node, step 4 completed
        comprehensiveResourceAvailability =
                comprehensiveResourceAvailability - (assignedTimesForTask * singleSlotUseResource);
        return comprehensiveResourceAvailability;
    }

    public double balanceFactor(
            WorkerProfile workerProfile, Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        SlotAssignedProfile slotAssignedProfile =
                workerAssignedSlots.get(workerProfile.getAddress());
        if (slotAssignedProfile != null) {
            return balanceFactor(
                    workerProfile,
                    slotAssignedProfile.getCurrentTaskAssignedSlotsNum()
                            + slotAssignedProfile.getAssignedSlotsNum());
        } else {
            return balanceFactor(workerProfile, workerProfile.getAssignedSlots().length);
        }
    }

    public double balanceFactor(WorkerProfile workerProfile, Integer assignedSlots) {
        return 1.0
                - ((double) assignedSlots
                        / (workerProfile.getAssignedSlots().length
                                + workerProfile.getUnassignedSlots().length));
    }
}
