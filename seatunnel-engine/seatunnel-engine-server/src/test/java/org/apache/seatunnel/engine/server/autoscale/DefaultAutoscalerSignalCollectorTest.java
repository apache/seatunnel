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

package org.apache.seatunnel.engine.server.autoscale;

import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.RequestSlotOperationStats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class DefaultAutoscalerSignalCollectorTest {

    @Test
    void computesFixedSlotUtilizationAndWorkerSampleSummary() {
        FakeResourceManager resourceManager = new FakeResourceManager();
        Address worker = address(5801);
        resourceManager.workers.put(worker, worker(worker, false, 1, 3));
        resourceManager.sampleStore.record(
                new WorkerMetricsSample(worker, 10_000L, 0.4d, 0.6d), 10_000L);

        AutoscalerMetricsSnapshot snapshot = collector(resourceManager).collect();

        Assertions.assertEquals(SlotMode.FIXED, snapshot.getSlotMode());
        Assertions.assertEquals(MetricStatus.VALID, snapshot.getFixedSlotUtilization().getStatus());
        Assertions.assertEquals(0.25d, snapshot.getFixedSlotUtilization().getValue(), 0.0001d);
        Assertions.assertEquals(1, snapshot.getValidWorkerSamples());
        Assertions.assertTrue(snapshot.isScaleInMetricsValid());
    }

    @Test
    void dynamicAndMixedSlotsDoNotExposeNumericSlotUtilization() {
        FakeResourceManager dynamicResourceManager = new FakeResourceManager();
        Address worker = address(5801);
        dynamicResourceManager.workers.put(worker, worker(worker, true, 0, 0));

        AutoscalerMetricsSnapshot dynamicSnapshot = collector(dynamicResourceManager).collect();
        Assertions.assertEquals(SlotMode.DYNAMIC, dynamicSnapshot.getSlotMode());
        Assertions.assertEquals(
                MetricStatus.UNKNOWN, dynamicSnapshot.getFixedSlotUtilization().getStatus());

        FakeResourceManager mixedResourceManager = new FakeResourceManager();
        Address fixed = address(5802);
        Address dynamic = address(5803);
        mixedResourceManager.workers.put(fixed, worker(fixed, false, 1, 1));
        mixedResourceManager.workers.put(dynamic, worker(dynamic, true, 0, 0));

        AutoscalerMetricsSnapshot mixedSnapshot = collector(mixedResourceManager).collect();
        Assertions.assertEquals(SlotMode.MIXED, mixedSnapshot.getSlotMode());
        Assertions.assertEquals(
                MetricStatus.UNKNOWN, mixedSnapshot.getFixedSlotUtilization().getStatus());
    }

    @Test
    void includesShortageDeltaAndPendingFacts() {
        FakeResourceManager resourceManager = new FakeResourceManager();
        resourceManager.shortageStats.recordWaitShortage(1, "slot");
        DefaultAutoscalerSignalCollector collector =
                new DefaultAutoscalerSignalCollector(
                        resourceManager,
                        AutoscalerRuntimeConfig.defaults(),
                        () -> 2,
                        () -> 300L,
                        () -> 1_000L);

        AutoscalerMetricsSnapshot first = collector.collect();
        AutoscalerMetricsSnapshot second = collector.collect();

        Assertions.assertEquals(1L, first.getResourceShortageCount());
        Assertions.assertTrue(first.isWaitShortage());
        Assertions.assertEquals(2, first.getPendingJobCount());
        Assertions.assertEquals(300L, first.getOldestPendingDurationMillis());
        Assertions.assertEquals(0L, second.getResourceShortageCount());
    }

    private DefaultAutoscalerSignalCollector collector(FakeResourceManager resourceManager) {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder().metricsFreshnessSeconds(5).build();
        return new DefaultAutoscalerSignalCollector(
                resourceManager, config, () -> 0, () -> 0L, () -> 10_500L);
    }

    private WorkerProfile worker(
            Address address, boolean dynamicSlot, int assignedSlots, int unassignedSlots) {
        WorkerProfile workerProfile = new WorkerProfile(address);
        workerProfile.setDynamicSlot(dynamicSlot);
        workerProfile.setAssignedSlots(slots(address, assignedSlots));
        workerProfile.setUnassignedSlots(slots(address, unassignedSlots));
        return workerProfile;
    }

    private SlotProfile[] slots(Address address, int count) {
        SlotProfile[] slots = new SlotProfile[count];
        for (int i = 0; i < count; i++) {
            slots[i] =
                    new SlotProfile(address, i, new ResourceProfile(CPU.of(0), Memory.of(1)), "s");
        }
        return slots;
    }

    private static Address address(int port) {
        try {
            return new Address("127.0.0.1", port);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class FakeResourceManager implements ResourceManager {
        private final ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        private final LatestWorkerSampleStore sampleStore = new LatestWorkerSampleStore(5_000L);
        private final ResourceShortageStats shortageStats = new ResourceShortageStats();

        @Override
        public void init() {}

        @Override
        public org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture<SlotProfile>
                applyResource(
                        long jobId,
                        ResourceProfile resourceProfile,
                        Map<String, String> tagFilter) {
            return null;
        }

        @Override
        public org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture<
                        java.util.List<SlotProfile>>
                applyResources(
                        long jobId,
                        java.util.List<ResourceProfile> resourceProfile,
                        Map<String, String> tagFilter) {
            return null;
        }

        @Override
        public org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture<Void>
                releaseResources(long jobId, java.util.List<SlotProfile> profiles) {
            return null;
        }

        @Override
        public org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture<Void>
                releaseResource(long jobId, SlotProfile profile) {
            return null;
        }

        @Override
        public boolean slotActiveCheck(SlotProfile profile) {
            return false;
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {}

        @Override
        public void memberRemoved(com.hazelcast.internal.services.MembershipServiceEvent event) {}

        @Override
        public void close() {}

        @Override
        public java.util.List<SlotProfile> getUnassignedSlots(Map<String, String> tags) {
            return Collections.emptyList();
        }

        @Override
        public java.util.List<SlotProfile> getAssignedSlots(Map<String, String> tags) {
            return Collections.emptyList();
        }

        @Override
        public int workerCount(Map<String, String> tags) {
            return workers.size();
        }

        @Override
        public ConcurrentMap<Address, WorkerProfile> getRegisterWorker() {
            return workers;
        }

        @Override
        public RequestSlotOperationStats getRequestSlotOperationStats() {
            return null;
        }

        @Override
        public void reportAutoscalerMetrics(WorkerMetricsSample sample, long receiveTimeMillis) {}

        @Override
        public LatestWorkerSampleStore getAutoscalerWorkerSampleStore() {
            return sampleStore;
        }

        @Override
        public ResourceShortageStats getResourceShortageStats() {
            return shortageStats;
        }

        @Override
        public AutoscalerRuntimeConfig getAutoscalerRuntimeConfig() {
            return AutoscalerRuntimeConfig.defaults();
        }
    }
}
