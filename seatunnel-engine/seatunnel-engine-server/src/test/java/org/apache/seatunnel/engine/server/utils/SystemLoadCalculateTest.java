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

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.resourcemanager.AbstractResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SystemLoadStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@Slf4j
public class SystemLoadCalculateTest {

    private SystemLoadCalculate systemLoadCalculate;

    @BeforeEach
    void setUp() {
        systemLoadCalculate = new SystemLoadCalculate();
    }

    @Test
    @DisplayName("Step0: A newly created LoadBalancer should return the highest priority of 1.0")
    void newLoadBalancerShouldReturnMaxPriority() {
        Assertions.assertEquals(1.0, systemLoadCalculate.calculateSchedulingPriority());
    }

    @Test
    @DisplayName("Step1-3: Adding invalid utilization data should throw an exception")
    void shouldThrowExceptionForInvalidUtilizationData() {
        Assertions.assertAll(
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(-0.1, 0.5)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(0.5, 1.1)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(1.1, 0.5)));
    }

    @Test
    @DisplayName("Step1-3: Test weight calculation for 3 records")
    void shouldCalculateCorrectPriorityForThreeRecords() {
        // Add 3 records
        // Oldest record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // CPU: 50%, Memory: 40%
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // CPU: 70%, Memory: 60%
        // Newest record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // CPU: 60%, Memory: 50%

        double priority = systemLoadCalculate.calculateSchedulingPriority();

        // Manually calculate the expected result
        // Weight distribution should be [4/8, 2/8, 2/8]
        double expectedPriority =
                // Newest record (1-0.6)*0.5 + (1-0.5)*0.5  * (4/8)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 8.0))
                        +
                        // Second record (1-0.7)*0.5 + (1-0.6)*0.5  * (2/8)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 8.0))
                        +
                        // Oldest record (1-0.5)*0.5 + (1-0.4)*0.5 * (2/8)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 8.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName("Step1-3: Test weight calculation for 5 records")
    void shouldCalculateCorrectPriorityForFiveRecords() {
        // Add 5 records, from oldest to newest
        systemLoadCalculate.addUtilizationData(0.3, 0.2);
        systemLoadCalculate.addUtilizationData(0.4, 0.3);
        systemLoadCalculate.addUtilizationData(0.5, 0.4);
        systemLoadCalculate.addUtilizationData(0.7, 0.6);
        systemLoadCalculate.addUtilizationData(0.6, 0.5);

        double priority = systemLoadCalculate.calculateSchedulingPriority();

        // Manually calculate the expected result
        // Weight distribution should be [4/10, 2/10, 2/10, 1/10, 1/10]
        double expectedPriority =
                // Newest record: (1-0.6)*0.5 + (1-0.5)*0.5 * (4/10)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 10.0))
                        +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 10.0))
                        +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 10.0))
                        +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0 - 0.4) * 0.5) + ((1.0 - 0.3) * 0.5)) * (1.0 / 10.0))
                        +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0 - 0.3) * 0.5) + ((1.0 - 0.2) * 0.5)) * (1.0 / 10.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName(
            "Step1-3: Detailed verification of adding 6 records (verifying the maximum window limit of 5)")
    void detailedCalculationForSixRecords() {
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        systemLoadCalculate.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        systemLoadCalculate.addUtilizationData(0.3, 0.2); // Now the oldest record
        systemLoadCalculate.addUtilizationData(0.4, 0.3); // Fourth record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // Third record
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // Second record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // Newest record

        double expectedPriority =
                // Newest record: (1-0.6)*0.5 + (1-0.5)*0.5 * (4/10)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 10.0))
                        +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 10.0))
                        +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 10.0))
                        +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0 - 0.4) * 0.5) + ((1.0 - 0.3) * 0.5)) * (1.0 / 10.0))
                        +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0 - 0.3) * 0.5) + ((1.0 - 0.2) * 0.5)) * (1.0 / 10.0));

        double actualPriority = systemLoadCalculate.calculateSchedulingPriority();

        Assertions.assertEquals(expectedPriority, actualPriority);
    }

    @Test
    @DisplayName("Step4: Test calculateComprehensiveResourceAvailability method")
    void testCalculateComprehensiveResourceAvailability() throws UnknownHostException {
        // Assume that the overall resource idle rate is 0.8, and the Worker node has been
        // continuously allocated 3 slots. This value is calculated based on the actual memory and
        // CPU.
        double comprehensiveResourceAvailability = 0.8;

        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        Address address = new Address("127.0.0.1", 5701);
        when(workerProfile.getAddress()).thenReturn(address);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, SlotAssignedProfile> workerAssignedSlots = new ConcurrentHashMap<>();

        // Each task has a fixed slot resource
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // When the worker has not been assigned, the overall resource idle rate remains unchanged
        double result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.8, result, 0.01);

        // The worker has been assigned 1 slot
        times = 1;
        workerAssignedSlots.put(address, new SlotAssignedProfile(singleSlotResource, 1, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.76, result, 0.01);

        // The worker has been assigned 2 slots
        times = 2;
        workerAssignedSlots.put(address, new SlotAssignedProfile(singleSlotResource, 2, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.72, result, 0.01);

        // If there is no unassigned slot, it will not be executed.

    }

    @Test
    @DisplayName("Step5: Test balanceFactor method")
    void testBalanceFactor() {
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[7]);
        double balanceFactor = systemLoadCalculate.balanceFactor(workerProfile, 3);
        Assertions.assertEquals(0.7, balanceFactor, 0.01);
    }

    @Test
    @DisplayName("All: Test the overall calculation logic")
    void testLoadBalancer() throws UnknownHostException {

        // Verification plan 1: Split each step and verify whether the settlement indicators of each
        // link are accurate
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        systemLoadCalculate.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        systemLoadCalculate.addUtilizationData(0.3, 0.2); // Now the oldest record
        systemLoadCalculate.addUtilizationData(0.4, 0.3); // Fourth record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // Third record
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // Second record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // Newest record
        double comprehensiveResourceAvailability =
                systemLoadCalculate.calculateSchedulingPriority();
        Address address = new Address("127.0.0.1", 5701);
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAddress()).thenReturn(address);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, SlotAssignedProfile> workerAssignedSlots = new ConcurrentHashMap<>();

        // Each task has a fixed Slot resource
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // When the worker has not been assigned, the overall resource idle rate remains unchanged
        double result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.5, result, 0.01);

        // The worker has been assigned 1 slot
        times = 1;
        workerAssignedSlots.put(address, new SlotAssignedProfile(singleSlotResource, 1, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.4, result, 0.01);

        workerAssignedSlots.put(address, new SlotAssignedProfile(singleSlotResource, 2, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double balanceFactor = systemLoadCalculate.balanceFactor(workerProfile, 7);
        Assertions.assertEquals(0.12, balanceFactor, 0.01);

        double finalResult = 0.7 * 0.3 + 0.125 * 0.3;
        Assertions.assertEquals(
                finalResult,
                systemLoadCalculate.calculateResourceAvailability(result, balanceFactor),
                0.01);

        // Verification plan 2: simulate the actual scenario and call the calculateWeight method to
        // verify the final result and whether it is consistent with the result of step 1
        Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap = new ConcurrentHashMap<>();
        workerLoadMap
                .computeIfAbsent(address, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.3, 0.2));
        workerLoadMap
                .computeIfAbsent(address, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.4, 0.3));
        workerLoadMap
                .computeIfAbsent(address, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.5, 0.4));
        workerLoadMap
                .computeIfAbsent(address, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.7, 0.6));
        workerLoadMap
                .computeIfAbsent(address, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.6, 0.5));

        // Mock current node resources
        WorkerProfile workerProfile2 = Mockito.mock(WorkerProfile.class);
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile2.getAddress()).thenReturn(address);

        Map<Address, SlotAssignedProfile> workerAssignedSlots2 = new ConcurrentHashMap<>();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        // Mock ResourceManager
        AbstractResourceManager rm = Mockito.mock(AbstractResourceManager.class);
        when(rm.getEngineConfig()).thenReturn(Mockito.mock(EngineConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig())
                .thenReturn(Mockito.mock(SlotServiceConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig().getAllocateStrategy())
                .thenReturn(ServerConfigOptions.SLOT_ALLOCATE_STRATEGY.defaultValue());
        // Simulate ResourceRequestHandler to call calculateWeight to calculate weight
        SystemLoadStrategy systemLoadStrategy = new SystemLoadStrategy(workerLoadMap);
        systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        // Mock Application Resources
        workerAssignedSlots2.put(address, new SlotAssignedProfile(singleSlotResource, 1, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);

        workerAssignedSlots2.put(address, new SlotAssignedProfile(singleSlotResource, 2, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        // Verity
        Assertions.assertEquals(
                systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2),
                finalResult);
    }

    /**
     * Test Multi-Node System Load Balancing:
     *
     * <p>This test simulates the load distribution between two nodes, gradually increasing each
     * node's load to verify the system's load balancing algorithm. The main steps include creating
     * nodes, adding load information, configuring resource management components, calculating node
     * weights, and finally allocating slots based on these weights.
     *
     * <p>Specific Process: <br>
     * - Initialize two nodes (address1 and address2), each with a pre-added 5 load entries. <br>
     * - Configure ResourceManager and ResourceRequestHandler for handling resource requests and
     * calculating weights. <br>
     * - Create workerProfile1 and workerProfile2, representing two worker nodes, and set their
     * allocated and unallocated slots. <br>
     * - Initially, it is expected that the first node has a higher weight (0.78 vs the second
     * node's 0.41), leading to the preference of the first node for allocation. <br>
     * - Gradually allocate slots to the first node (from 1 to 4), recalculating weights after each
     * allocation and noting changes: <br>
     * - After allocating 1 slot: the first node's weight drops to 0.68; <br>
     * - After allocating 2 slots: the first node's weight drops to 0.58; <br>
     * - After allocating 3 slots: the first node's weight drops to 0.48; <br>
     * - After allocating 4 slots: the first node's weight drops to 0.38, at which point the second
     * node has a higher weight (0.41), switching preference to the second node. <br>
     * - Finally, allocate one slot to the second node, updating its weight to 0.31, and again
     * choosing the first node for allocation. <br>
     * <br>
     * Each slot consumes a fixed amount of resources, set to 0.1 in this test case. This test
     * ensures that the load balancing algorithm can make reasonable resource allocation decisions
     * based on the current load situation of the nodes. <br>
     */
    @Test
    @DisplayName("All: Test multiple node system load")
    void testMultipleNodeSystemLoad() throws UnknownHostException {
        Address address1 = new Address("127.0.0.1", 5701);
        Address address2 = new Address("127.0.0.1", 5702);

        // Simulate the actual scenario and call the calculateWeight method to verify the final
        // result
        Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap = new ConcurrentHashMap<>();
        workerLoadMap
                .computeIfAbsent(address1, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.2, 0.1));
        workerLoadMap
                .computeIfAbsent(address1, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.3, 0.2));
        workerLoadMap
                .computeIfAbsent(address1, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.4, 0.3));
        workerLoadMap
                .computeIfAbsent(address1, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.4, 0.4));
        workerLoadMap
                .computeIfAbsent(address1, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.3, 0.3));

        workerLoadMap
                .computeIfAbsent(address2, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.8, 0.7));
        workerLoadMap
                .computeIfAbsent(address2, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.9, 0.8));
        workerLoadMap
                .computeIfAbsent(address2, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.85, 0.75));
        workerLoadMap
                .computeIfAbsent(address2, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.9, 0.85));
        workerLoadMap
                .computeIfAbsent(address2, v -> EvictingQueue.create(5))
                .offer(new SystemLoadInfo(0.88, 0.8));

        // Mock current node resources
        WorkerProfile workerProfile2 = Mockito.mock(WorkerProfile.class);
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[0]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[10]);
        when(workerProfile2.getAddress()).thenReturn(address2);

        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());

        // Mock ResourceManager
        AbstractResourceManager rm = Mockito.mock(AbstractResourceManager.class);
        when(rm.getEngineConfig()).thenReturn(Mockito.mock(EngineConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig())
                .thenReturn(Mockito.mock(SlotServiceConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig().getAllocateStrategy())
                .thenReturn(ServerConfigOptions.SLOT_ALLOCATE_STRATEGY.defaultValue());

        WorkerProfile workerProfile1 = Mockito.mock(WorkerProfile.class);
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[0]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[10]);
        when(workerProfile1.getAddress()).thenReturn(address1);
        // Simulate ResourceRequestHandler to call calculateWeight to calculate weight
        SystemLoadStrategy systemLoadStrategy = new SystemLoadStrategy(workerLoadMap);
        Map<Address, SlotAssignedProfile> workerAssignedSlots1 = new ConcurrentHashMap<>();
        Double calculateWeight1 =
                systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        log.info("Node1 initialization weight: {}", calculateWeight1);

        Map<Address, SlotAssignedProfile> workerAssignedSlots2 = new ConcurrentHashMap<>();
        Double calculateWeight2 =
                systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info("Node2 initialization weight: {}", calculateWeight2);

        // First node load is low, second node load is high, first node weight should be greater
        // than second node
        Assertions.assertTrue(calculateWeight1 > calculateWeight2);

        // Tip: Here, we default to singleSlotUseResource=0.1 for easy verification of the accuracy
        // of the results. The singleSlotUseResource for the load can refer to the class:
        // org.apache.setannel.engine.E2e.allocatestgy SystemLoadAllocateStrategyIT
        double singleSlotUseResource = 0.1;

        // First node is assigned a slot
        workerAssignedSlots1.put(address1, new SlotAssignedProfile(singleSlotUseResource, 1, 0));
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[1]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[9]);
        calculateWeight1 = systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        calculateWeight2 = systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info(
                "First allocation weight: Node 1: {}, Node 2: {}",
                calculateWeight1,
                calculateWeight2);
        Assertions.assertTrue(calculateWeight1 > calculateWeight2);

        // First node is assigned two slots
        workerAssignedSlots1.put(address1, new SlotAssignedProfile(singleSlotUseResource, 2, 0));
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[2]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[8]);
        calculateWeight1 = systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        calculateWeight2 = systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info(
                "Second allocation weight: Node 1: {}, Node 2: {}",
                calculateWeight1,
                calculateWeight2);
        Assertions.assertTrue(calculateWeight1 > calculateWeight2);

        // First node is assigned three slots
        workerAssignedSlots1.put(address1, new SlotAssignedProfile(singleSlotUseResource, 3, 0));
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[7]);
        calculateWeight1 = systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        calculateWeight2 = systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info(
                "Third allocation weight: Node 1: {}, Node 2: {}",
                calculateWeight1,
                calculateWeight2);
        Assertions.assertTrue(calculateWeight1 > calculateWeight2);

        // First node is assigned four slots
        workerAssignedSlots1.put(address1, new SlotAssignedProfile(singleSlotUseResource, 4, 0));
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[4]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[6]);
        calculateWeight1 = systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        calculateWeight2 = systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info(
                "Fourth allocation weight: Node 1: {}, Node 2: {}",
                calculateWeight1,
                calculateWeight2);

        // After applying for resources five times, the weight of the first node should be less than
        // the second node because the estimated resource usage rate of a single slot is 0.1
        Assertions.assertTrue(calculateWeight1 < calculateWeight2);

        // Second node is assigned one slot
        workerAssignedSlots2.put(address2, new SlotAssignedProfile(singleSlotUseResource, 1, 0));
        when(workerProfile1.getAssignedSlots()).thenReturn(new SlotProfile[1]);
        when(workerProfile1.getUnassignedSlots()).thenReturn(new SlotProfile[9]);
        calculateWeight1 = systemLoadStrategy.calculateWeight(workerProfile1, workerAssignedSlots1);
        calculateWeight2 = systemLoadStrategy.calculateWeight(workerProfile2, workerAssignedSlots2);
        log.info(
                "Fifth allocation weight: Node 1: {}, Node 2: {}",
                calculateWeight1,
                calculateWeight2);

        // After applying for resources five times, the weight of the first node should be less than
        // the second node because the estimated resource usage rate of a single slot is 0.1
        Assertions.assertTrue(calculateWeight1 > calculateWeight2);
    }
}
