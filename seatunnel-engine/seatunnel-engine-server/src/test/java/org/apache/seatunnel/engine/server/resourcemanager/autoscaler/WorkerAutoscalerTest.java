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

package org.apache.seatunnel.engine.server.resourcemanager.autoscaler;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link WorkerAutoscaler} decision logic and sliding window. */
class WorkerAutoscalerTest {

    private AutoscalerConfig config;
    private ResourceManager resourceManager;

    @BeforeEach
    void setUp() {
        config = new AutoscalerConfig();
        config.setEnabled(true);
        config.setMinWorkers(1);
        config.setMaxWorkers(5);
        config.setStabilizationWindowSeconds(60);
        config.setEvaluationIntervalSeconds(10);
        config.setCpuScaleOutThreshold(0.75);
        config.setCpuScaleInThreshold(0.25);
        config.setMemoryScaleOutThreshold(0.80);
        config.setMemoryScaleInThreshold(0.30);
        config.setSlotUsageScaleOutThreshold(0.80);
        config.setSlotUsageScaleInThreshold(0.20);
        config.setScaleOutCooldownSeconds(0);
        config.setScaleInCooldownSeconds(0);
        config.setRecommendationOnly(true);

        resourceManager = mock(ResourceManager.class);
    }

    // --- SlidingWindow tests ---

    @Test
    void testSlidingWindowAverage() {
        WorkerAutoscaler.SlidingWindow window = new WorkerAutoscaler.SlidingWindow(60, 10);
        // maxSize = 60 / 10 = 6
        window.add(0.5);
        window.add(0.6);
        window.add(0.7);
        assertEquals((0.5 + 0.6 + 0.7) / 3, window.getAverage(), 0.001);
    }

    @Test
    void testSlidingWindowWrapsAround() {
        WorkerAutoscaler.SlidingWindow window = new WorkerAutoscaler.SlidingWindow(60, 10);
        // maxSize = 6, fill with 7 values so the first is overwritten
        window.add(0.1);
        window.add(0.2);
        window.add(0.3);
        window.add(0.4);
        window.add(0.5);
        window.add(0.6);
        // now full: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
        window.add(0.7);
        // now: [0.7, 0.2, 0.3, 0.4, 0.5, 0.6]
        assertEquals((0.7 + 0.2 + 0.3 + 0.4 + 0.5 + 0.6) / 6, window.getAverage(), 0.001);
    }

    @Test
    void testSlidingWindowEmptyReturnsZero() {
        WorkerAutoscaler.SlidingWindow window = new WorkerAutoscaler.SlidingWindow(60, 10);
        assertEquals(0.0, window.getAverage(), 0.001);
    }

    @Test
    void testSlidingWindowSingleValue() {
        WorkerAutoscaler.SlidingWindow window = new WorkerAutoscaler.SlidingWindow(60, 10);
        window.add(0.85);
        assertEquals(0.85, window.getAverage(), 0.001);
    }

    // --- ScalingDecision tests ---

    @Test
    void testScalingDecisionNoAction() {
        WorkerAutoscaler.ScalingDecision decision = WorkerAutoscaler.ScalingDecision.noAction();
        assertEquals(ScalingAction.NO_ACTION, decision.getAction());
        assertEquals(-1, decision.getTargetWorkers());
        assertEquals("No scaling needed", decision.getReason());
    }

    @Test
    void testScalingDecisionScaleOut() {
        WorkerAutoscaler.ScalingDecision decision =
                new WorkerAutoscaler.ScalingDecision(ScalingAction.SCALE_OUT, 4, "cpu high");
        assertEquals(ScalingAction.SCALE_OUT, decision.getAction());
        assertEquals(4, decision.getTargetWorkers());
        assertEquals("cpu high", decision.getReason());
    }

    // --- evaluate() tests (scale-out / scale-in) ---

    @Test
    void testEvaluateScaleOutByCpu() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        ConcurrentMap<Address, WorkerProfile> workers = createWorkers(2, 0.85, 0.5, 0.5);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        // Seed the metric windows to simulate sustained high CPU across the
        // stabilization window
        seedMetricWindows(autoscaler, 0.85, 0.5, 0.5);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.SCALE_OUT, rec.getAction());
        assertEquals(3, rec.getTargetWorkerCount());
        assertTrue(rec.getReason().contains("Scale-out"));
    }

    @Test
    void testEvaluateScaleOutBySlotUsage() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        // 2 workers with 4 slots each, 3 assigned each = 75% for metrics but
        // we need the slotUsageRatio computed from totalSlots vs assignedSlots
        // Each worker: 1 assigned, 3 unassigned → total=4, assigned=1 → 2/8=0.25
        // To get high slot usage: 4 assigned, 0 unassigned → total=4, assigned=4 → 8/8=1.0
        ConcurrentMap<Address, WorkerProfile> workers = createWorkersWithSlots(2, 4, 0, 0.4, 0.4);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        // slotUsageRatio = (4*2) / (4*2) = 1.0 > 0.80 threshold
        seedMetricWindows(autoscaler, 0.4, 0.4, 1.0);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.SCALE_OUT, rec.getAction());
        assertEquals(3, rec.getTargetWorkerCount());
    }

    @Test
    void testEvaluateScaleIn() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        // 3 workers with low utilization
        ConcurrentMap<Address, WorkerProfile> workers = createWorkersWithSlots(3, 2, 2, 0.10, 0.15);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        // slotUsageRatio = (2*3) / (4*3) = 6/12 = 0.5 — wait, that's above 0.20
        // To get low slot usage: 0 assigned, 4 unassigned → 0/12 = 0.0
        // Let me fix: 0 assigned, 4 unassigned each
        workers = createWorkersWithSlots(3, 0, 4, 0.10, 0.15);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        seedMetricWindows(autoscaler, 0.10, 0.15, 0.0);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.SCALE_IN, rec.getAction());
        assertEquals(2, rec.getTargetWorkerCount());
        assertTrue(rec.getReason().contains("Scale-in"));
    }

    @Test
    void testEvaluateNoActionWhenMetricsAreNormal() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        ConcurrentMap<Address, WorkerProfile> workers = createWorkers(2, 0.5, 0.5, 0.5);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        seedMetricWindows(autoscaler, 0.5, 0.5, 0.5);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.NO_ACTION, rec.getAction());
    }

    @Test
    void testEvaluateNoActionWhenAtMaxWorkers() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        // 5 workers (at max), high CPU
        ConcurrentMap<Address, WorkerProfile> workers = createWorkers(5, 0.90, 0.5, 0.5);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        seedMetricWindows(autoscaler, 0.90, 0.5, 0.5);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.NO_ACTION, rec.getAction());
    }

    @Test
    void testEvaluateNoActionWhenAtMinWorkers() throws UnknownHostException {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        // 1 worker (at min), low utilization
        ConcurrentMap<Address, WorkerProfile> workers = createWorkersWithSlots(1, 0, 4, 0.10, 0.15);
        when(resourceManager.getRegisterWorker()).thenReturn(workers);

        seedMetricWindows(autoscaler, 0.10, 0.15, 0.0);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
        assertEquals(ScalingAction.NO_ACTION, rec.getAction());
    }

    @Test
    void testEvaluateNoActionWhenNoWorkers() {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        ConcurrentMap<Address, WorkerProfile> emptyWorkers = new ConcurrentHashMap<>();
        when(resourceManager.getRegisterWorker()).thenReturn(emptyWorkers);

        autoscaler.evaluate();

        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        // Should remain NoAction when no workers are registered
        assertEquals(ScalingAction.NO_ACTION, rec.getAction());
    }

    @Test
    void testEvaluateHandlesExceptionGracefully() {
        WorkerAutoscaler autoscaler = new WorkerAutoscaler(config, resourceManager);

        when(resourceManager.getRegisterWorker()).thenThrow(new RuntimeException("test error"));

        // Should not throw — evaluate() catches Exception
        autoscaler.evaluate();

        // Recommendation should still be the default no-action
        AutoscalerRecommendation rec = autoscaler.getCurrentRecommendation();
        assertNotNull(rec);
    }

    // --- Helper methods ---

    /** Create workers with specified CPU/memory/slot usage percentage. */
    private ConcurrentMap<Address, WorkerProfile> createWorkers(
            int count, double cpuLoad, double memLoad, double slotUsage)
            throws UnknownHostException {
        // Each worker has 4 total slots, with slotUsage * 4 assigned
        int totalSlotsPerWorker = 4;
        int assignedSlots = (int) (slotUsage * totalSlotsPerWorker);
        int unassignedSlots = totalSlotsPerWorker - assignedSlots;
        return createWorkersWithSlots(count, assignedSlots, unassignedSlots, cpuLoad, memLoad);
    }

    /** Create workers with explicit assigned/unassigned slot counts. */
    private ConcurrentMap<Address, WorkerProfile> createWorkersWithSlots(
            int count, int assignedSlots, int unassignedSlots, double cpuLoad, double memLoad)
            throws UnknownHostException {
        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        for (int i = 0; i < count; i++) {
            Address address = new Address("127.0.0.1", 5000 + i);
            WorkerProfile profile = mock(WorkerProfile.class);

            SlotProfile[] assigned = new SlotProfile[assignedSlots];
            for (int j = 0; j < assignedSlots; j++) {
                assigned[j] = mock(SlotProfile.class);
            }
            SlotProfile[] unassigned = new SlotProfile[unassignedSlots];
            for (int j = 0; j < unassignedSlots; j++) {
                unassigned[j] = mock(SlotProfile.class);
            }

            when(profile.getAssignedSlots()).thenReturn(assigned);
            when(profile.getUnassignedSlots()).thenReturn(unassigned);

            // Mock SystemLoadInfo
            SystemLoadInfo loadInfo = mock(SystemLoadInfo.class);
            when(loadInfo.getCpuPercentage()).thenReturn(cpuLoad);
            when(loadInfo.getMemPercentage()).thenReturn(memLoad);
            when(profile.getSystemLoadInfo()).thenReturn(loadInfo);

            workers.put(address, profile);
        }
        return workers;
    }

    /**
     * Seeds the metric windows with enough values to fill the stabilization window, so
     * evaluateScalingDecision sees stable metrics rather than zeros.
     *
     * <p>Uses reflection to access the package-private metricWindows field.
     */
    private void seedMetricWindows(
            WorkerAutoscaler autoscaler, double cpu, double memory, double slotUsage) {
        try {
            java.lang.reflect.Field field =
                    WorkerAutoscaler.class.getDeclaredField("metricWindows");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            ConcurrentMap<WorkerAutoscaler.MetricType, WorkerAutoscaler.SlidingWindow> windows =
                    (ConcurrentMap<WorkerAutoscaler.MetricType, WorkerAutoscaler.SlidingWindow>)
                            field.get(autoscaler);

            int windowSize =
                    config.getStabilizationWindowSeconds() / config.getEvaluationIntervalSeconds();
            // Fill each window with the target value enough times to be stable
            for (int i = 0; i < windowSize; i++) {
                windows.get(WorkerAutoscaler.MetricType.CPU_LOAD).add(cpu);
                windows.get(WorkerAutoscaler.MetricType.MEMORY_LOAD).add(memory);
                windows.get(WorkerAutoscaler.MetricType.SLOT_USAGE).add(slotUsage);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to seed metric windows", e);
        }
    }
}
