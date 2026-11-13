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

package org.apache.seatunnel.engine.server.observability;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.engine.server.metrics.MetricsTestUtils.exportDynamicCounters;

public class RealtimeMetricsServiceTest {

    @Test
    public void testVertexDeltaCalculationUsesBaselineAndNeverNegativeOnReset() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        invokeUpdate(store, baseMs, newJobAggWithSourceReadNs(1L, 100_000_000L));
        Assertions.assertEquals(0L, latestVertexBucketLong(store, 1L, "sourceReadNs"));

        invokeUpdate(store, baseMs + 5000L, newJobAggWithSourceReadNs(1L, 150_000_000L));
        Assertions.assertEquals(50_000_000L, latestVertexBucketLong(store, 1L, "sourceReadNs"));

        // Simulate counter reset/rollback: current sum is smaller than last sum.
        invokeUpdate(store, baseMs + 10_000L, newJobAggWithSourceReadNs(1L, 10_000_000L));
        Assertions.assertEquals(0L, latestVertexBucketLong(store, 1L, "sourceReadNs"));
    }

    @Test
    public void testEdgeDeltaCalculationUsesBaseline() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        long queueId = 101L;
        invokeUpdate(store, baseMs, newJobAggWithQueuePutBlockedNs(queueId, 1_000L));
        Assertions.assertEquals(0L, latestEdgeBucketLong(store, queueId, "emitBlockedNs"));

        invokeUpdate(store, baseMs + 5000L, newJobAggWithQueuePutBlockedNs(queueId, 3_500L));
        Assertions.assertEquals(2_500L, latestEdgeBucketLong(store, queueId, "emitBlockedNs"));
    }

    @Test
    public void testBucketEvictionRemovesExpiredBuckets() throws Exception {
        Object store = newJobStore(5000L, 3);

        long baseMs = 1_000_000_000_000L;
        // Create buckets at 0, 1, 2, 3, 4, 5 minutes.
        for (int i = 0; i <= 5; i++) {
            invokeUpdate(store, baseMs + i * 60_000L, null);
        }

        long nowMs = baseMs + 5 * 60_000L;
        long expireBeforeMs = nowMs - 3L * 60_000L;

        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object first = buckets.peekFirst();
        Assertions.assertNotNull(first);
        long firstStartMs = getField(first, "bucketStartMs", long.class);
        Assertions.assertTrue(
                firstStartMs >= expireBeforeMs,
                "oldest bucket should not be older than retention window");
        Assertions.assertEquals(4, buckets.size());
    }

    @Test
    public void testIntermediateQueueMetricsOwnershipRestorationAndRealtimeAggregation()
            throws Exception {
        long jobId = 1001L;
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        TaskLocation taskLocationA = new TaskLocation(taskGroupLocation, 0L, 0);
        TaskLocation taskLocationB = new TaskLocation(taskGroupLocation, 1L, 0);
        long queueId = -2L;
        int capacity = 2048;

        // 1. FIRST EXECUTION:
        // Task A registers queue ID -2, capacity 2048. Task B reuses the same queue.
        SeaTunnelMetricsContext contextA = new SeaTunnelMetricsContext();
        SeaTunnelMetricsContext contextB = new SeaTunnelMetricsContext();

        TaskGroupWithIntermediateBlockingQueue taskGroup1 =
                new TaskGroupWithIntermediateBlockingQueue(
                        taskGroupLocation, "group1", Collections.emptyList());
        taskGroup1.init();

        taskGroup1.getQueueCache(queueId, capacity, contextA);
        taskGroup1.getQueueCache(queueId, capacity, contextB);

        // Simulate blocked time and queue size on Task A's queue
        contextA.counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                .inc(1_000_000_000L);
        contextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(100);

        // Save Task A's metrics snapshot
        Map<TaskLocation, SeaTunnelMetricsContext> snapshotMap = new HashMap<>();
        snapshotMap.put(taskLocationA, contextA);
        snapshotMap.put(taskLocationB, contextB);

        // 2. RESTORATION:
        // Create a new task group with an empty queue cache.
        // Task B registers queue ID -2 first.
        // Task A restores its old snapshot through
        // TaskExecutionContext.getOrCreateMetricsContext().
        TaskGroupWithIntermediateBlockingQueue taskGroup2 =
                new TaskGroupWithIntermediateBlockingQueue(
                        taskGroupLocation, "group2", Collections.emptyList());
        taskGroup2.init();

        SeaTunnelMetricsContext restoredContextB = contextB;
        taskGroup2.getQueueCache(queueId, capacity, restoredContextB);
        restoredContextB
                .counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                .inc(2_500_000_000L);
        restoredContextB.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(200);

        MetricsSnapshotStateStore snapshotStore = Mockito.mock(MetricsSnapshotStateStore.class);
        Mockito.when(
                        snapshotStore.getOrDefault(
                                Mockito.any(TaskLocation.class),
                                Mockito.any(SeaTunnelMetricsContext.class)))
                .thenAnswer(
                        inv -> snapshotMap.getOrDefault(inv.getArgument(0), inv.getArgument(1)));

        EngineStateStores stateStores = Mockito.mock(EngineStateStores.class);
        Mockito.when(stateStores.metricsSnapshotStore()).thenReturn(snapshotStore);

        SeaTunnelEngineContext engineContext = SeaTunnelEngineContext.builder(stateStores).build();
        TaskExecutionContext executionContext =
                new TaskExecutionContext(null, null, engineContext, null);

        SeaTunnelMetricsContext restoredContextA =
                executionContext.getOrCreateMetricsContext(taskLocationA);

        // 3. Feed both task contexts through actual dynamic metrics collection and production
        // aggregation
        MetricDescriptor tagger =
                DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER
                        .get()
                        .withTag(MetricTags.JOB_ID, String.valueOf(jobId));

        MetricsCompressor compressor = new MetricsCompressor();
        MetricsCollectionContext collectionContext =
                new MetricsCollectionContext() {
                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel probeLevel,
                            ProbeUnit probeUnit,
                            long value) {
                        compressor.addLong(descriptor.copy().withMetric(name), value);
                    }

                    @Override
                    public void collect(MetricDescriptor descriptor, Object value) {}

                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel level,
                            ProbeUnit unit,
                            double value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, long value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, double value) {}
                };

        restoredContextA.provideDynamicMetrics(tagger, collectionContext);
        restoredContextB.provideDynamicMetrics(tagger, collectionContext);

        RawJobMetrics rawJobMetrics = RawJobMetrics.of(compressor.getBlobAndReset());

        Method aggMethod =
                RealtimeMetricsService.class.getDeclaredMethod(
                        "aggregateRawMetrics", List.class, Set.class);
        aggMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, ?> aggByJobId =
                (Map<Long, ?>)
                        aggMethod.invoke(
                                null,
                                Collections.singletonList(rawJobMetrics),
                                Collections.singleton(jobId));
        Object jobAgg = aggByJobId.get(jobId);
        Assertions.assertNotNull(jobAgg, "jobAgg should not be null for enabled job");

        // 4. Verify metric rows in JobAgg:
        // Exactly one capacity metric row for queue ID -2
        // Exactly one queue-size metric row
        // Exactly one blocked-time metric row
        Object queueCapacityAgg = getField(jobAgg, "queueCapacity", Object.class);
        Object queueSizeAgg = getField(jobAgg, "queueSize", Object.class);
        Object queuePutBlockedNsAgg = getField(jobAgg, "queuePutBlockedNs", Object.class);

        Map<?, ?> capacityCountById = getField(queueCapacityAgg, "countById", Map.class);
        Map<?, ?> sizeCountById = getField(queueSizeAgg, "countById", Map.class);
        Map<?, ?> blockedCountById = getField(queuePutBlockedNsAgg, "countById", Map.class);

        Assertions.assertEquals(
                1,
                capacityCountById.get(queueId),
                "Expected exactly one capacity metric row for queue ID -2");
        Assertions.assertEquals(
                1,
                sizeCountById.get(queueId),
                "Expected exactly one queue-size metric row for queue ID -2");
        Assertions.assertEquals(
                1,
                blockedCountById.get(queueId),
                "Expected exactly one blocked-time metric row for queue ID -2");

        // 5. Update JobStore and verify queueCapacity = 2048, subtaskCount = 1, and backpressure
        // denominator/ratio
        long bucketMs = 5000L;
        Object store = newJobStore(bucketMs, 3);
        long nowMs = System.currentTimeMillis();
        long baseMs = nowMs - bucketMs;

        // Baseline update sets previous sum
        invokeUpdate(store, baseMs, jobAgg);

        // Next window update (+5000ms) with additional 2_500_000_000L blocked nanos
        restoredContextB
                .counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                .inc(2_500_000_000L);

        MetricsCompressor nextCompressor = new MetricsCompressor();
        MetricsCollectionContext nextCollectionContext =
                new MetricsCollectionContext() {
                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel probeLevel,
                            ProbeUnit probeUnit,
                            long value) {
                        nextCompressor.addLong(descriptor.copy().withMetric(name), value);
                    }

                    @Override
                    public void collect(MetricDescriptor descriptor, Object value) {}

                    @Override
                    public void collect(
                            MetricDescriptor descriptor,
                            String name,
                            ProbeLevel level,
                            ProbeUnit unit,
                            double value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, long value) {}

                    @Override
                    public void collect(MetricDescriptor descriptor, double value) {}
                };

        restoredContextA.provideDynamicMetrics(tagger, nextCollectionContext);
        restoredContextB.provideDynamicMetrics(tagger, nextCollectionContext);
        RawJobMetrics nextRaw = RawJobMetrics.of(nextCompressor.getBlobAndReset());

        @SuppressWarnings("unchecked")
        Map<Long, ?> nextAggByJobId =
                (Map<Long, ?>)
                        aggMethod.invoke(
                                null,
                                Collections.singletonList(nextRaw),
                                Collections.singleton(jobId));
        Object nextJobAgg = nextAggByJobId.get(jobId);

        invokeUpdate(store, nowMs, nextJobAgg);

        // Verify queueCapacity = 2048
        long queueCapacity = latestEdgeBucketLong(store, queueId, "capacitySum");
        Assertions.assertEquals(2048L, queueCapacity, "queueCapacity must be 2048");

        // Verify subtaskCount = 1
        long subtaskCount = latestEdgeBucketLong(store, queueId, "subtaskCount");
        Assertions.assertEquals(1L, subtaskCount, "subtaskCount must be 1");

        long emitBlockedNs = latestEdgeBucketLong(store, queueId, "emitBlockedNs");
        Assertions.assertEquals(2_500_000_000L, emitBlockedNs);

        // Verify backpressure denominator and bpRatio via toEdgesResponse
        Method toEdgesMethod = store.getClass().getDeclaredMethod("toEdgesResponse", long.class);
        toEdgesMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> edgesResponse =
                (Map<String, Object>) toEdgesMethod.invoke(store, 60_000L);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> edges = (List<Map<String, Object>>) edgesResponse.get("edges");
        Assertions.assertEquals(1, edges.size());
        Map<String, Object> edge = edges.get(0);
        Assertions.assertEquals(queueId, edge.get("queueId"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> points = (List<Map<String, Object>>) edge.get("points");
        Assertions.assertFalse(points.isEmpty());
        Map<String, Object> latestPoint = points.get(points.size() - 1);

        Assertions.assertEquals(1L, latestPoint.get("subtaskCount"));
        Assertions.assertEquals(2048L, latestPoint.get("queueCapacity"));
        Assertions.assertEquals(200L, latestPoint.get("queueSize"));
        Assertions.assertEquals(200.0 / 2048.0, (Double) latestPoint.get("queueFillRatio"), 1e-6);

        // Backpressure denominator: TimeUnit.MILLISECONDS.toNanos(bucketMs) * subtaskCount
        // Denominator = 5_000_000_000.0 (uses 1 current queue-bearing group)
        // Ratio = 2_500_000_000.0 / 5_000_000_000.0 = 0.50
        double expectedBpRatio =
                ((double) emitBlockedNs)
                        / ((double) TimeUnit.MILLISECONDS.toNanos(bucketMs) * subtaskCount);
        Assertions.assertEquals(expectedBpRatio, (Double) latestPoint.get("bpRatio"), 1e-6);
    }

    @Test
    public void testFiveGroupIntermediateQueueMetricsRestorationAndRealtimeAggregation()
            throws Exception {
        long jobId = 1001L;
        long queueId = -2L;
        int capacity = 2048;
        int groupCount = 5;

        Map<TaskLocation, SeaTunnelMetricsContext> snapshotMap = new HashMap<>();

        List<TaskLocation> taskLocationAList = new ArrayList<>();
        List<TaskLocation> taskLocationBList = new ArrayList<>();
        List<SeaTunnelMetricsContext> initialContextBList = new ArrayList<>();

        // 1. INITIAL EXECUTION:
        // For each of the five parallel task groups, initialize its queue group.
        // Task A accesses the queue first and claims ownership; Task B accesses second.
        for (int i = 0; i < groupCount; i++) {
            TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, (long) i);
            TaskLocation taskLocationA = new TaskLocation(taskGroupLocation, 0L, i);
            TaskLocation taskLocationB = new TaskLocation(taskGroupLocation, 1L, i);
            taskLocationAList.add(taskLocationA);
            taskLocationBList.add(taskLocationB);

            SeaTunnelMetricsContext contextA = new SeaTunnelMetricsContext();
            SeaTunnelMetricsContext contextB = new SeaTunnelMetricsContext();
            initialContextBList.add(contextB);

            TaskGroupWithIntermediateBlockingQueue taskGroup1 =
                    new TaskGroupWithIntermediateBlockingQueue(
                            taskGroupLocation, "group1-" + i, Collections.emptyList());
            taskGroup1.init();

            // Task A accesses the queue first and claims ownership
            taskGroup1.getQueueCache(queueId, capacity, contextA);
            // Task B accesses the same queue second
            taskGroup1.getQueueCache(queueId, capacity, contextB);

            // Verify ownership is initially assigned to Task A
            Map<String, Long> initialExportedA = exportDynamicCounters(contextA, jobId);
            Map<String, Long> initialExportedB = exportDynamicCounters(contextB, jobId);
            Assertions.assertEquals(
                    (long) capacity,
                    initialExportedA.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                    "Group " + i + ": Task A must own the initial intermediate queue capacity");
            Assertions.assertFalse(
                    initialExportedB.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                    "Group "
                            + i
                            + ": Task B must not own the intermediate queue capacity initially");

            // Populate Task A with historical values
            contextA.counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                    .inc(1_000_000_000L);
            contextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(100);
            // Register an unrelated cumulative metric on Task A to verify persistence across
            // restoration
            contextA.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(500);

            // Store both task contexts in snapshot store
            snapshotMap.put(taskLocationA, contextA);
            snapshotMap.put(taskLocationB, contextB);
        }

        // Set up TaskExecutionContext backed by the snapshot store
        MetricsSnapshotStateStore snapshotStore = Mockito.mock(MetricsSnapshotStateStore.class);
        Mockito.when(
                        snapshotStore.getOrDefault(
                                Mockito.any(TaskLocation.class),
                                Mockito.any(SeaTunnelMetricsContext.class)))
                .thenAnswer(
                        inv -> snapshotMap.getOrDefault(inv.getArgument(0), inv.getArgument(1)));

        EngineStateStores stateStores = Mockito.mock(EngineStateStores.class);
        Mockito.when(stateStores.metricsSnapshotStore()).thenReturn(snapshotStore);

        SeaTunnelEngineContext engineContext = SeaTunnelEngineContext.builder(stateStores).build();
        TaskExecutionContext executionContext =
                new TaskExecutionContext(null, null, engineContext, null);

        // Verify non-queue tasks do not pollute queue metrics aggregation.
        TaskGroupLocation coordinatorGroupLocation = new TaskGroupLocation(jobId, 0, 0L);
        TaskLocation coordinatorTaskLocation = new TaskLocation(coordinatorGroupLocation, 0L, 0);
        SeaTunnelMetricsContext coordinatorContext = new SeaTunnelMetricsContext();
        coordinatorContext.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(100);
        snapshotMap.put(coordinatorTaskLocation, coordinatorContext);
        SeaTunnelMetricsContext restoredCoordinatorContext =
                executionContext.getOrCreateMetricsContext(coordinatorTaskLocation);

        // 2. RESTORATION:
        // For each group, create and initialize a new TaskGroupWithIntermediateBlockingQueue.
        // Task B accesses the new queue first and becomes the new owner.
        // Task A restores through TaskExecutionContext.getOrCreateMetricsContext().
        List<SeaTunnelMetricsContext> restoredContextAList = new ArrayList<>();
        List<SeaTunnelMetricsContext> restoredContextBList = new ArrayList<>();

        for (int i = 0; i < groupCount; i++) {
            TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, (long) i);
            TaskLocation taskLocationA = taskLocationAList.get(i);

            TaskGroupWithIntermediateBlockingQueue taskGroup2 =
                    new TaskGroupWithIntermediateBlockingQueue(
                            taskGroupLocation, "group2-" + i, Collections.emptyList());
            taskGroup2.init();

            // Task B is reused from the initial run rather than restored via
            // getOrCreateMetricsContext.
            SeaTunnelMetricsContext restoredContextB = initialContextBList.get(i);
            // Task B accesses the new queue first and becomes the owner
            taskGroup2.getQueueCache(queueId, capacity, restoredContextB);

            // Set full queue size = 2048 and baseline blocked nanos = 2_500_000_000L
            restoredContextB.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(2048);
            restoredContextB
                    .counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                    .inc(2_500_000_000L);

            // Restore Task A through TaskExecutionContext (which cleans stale queue metrics)
            SeaTunnelMetricsContext restoredContextA =
                    executionContext.getOrCreateMetricsContext(taskLocationA);

            // Verify Task A's stale queue metrics have been cleaned
            Map<String, Long> restoredExportedA = exportDynamicCounters(restoredContextA, jobId);
            Assertions.assertFalse(
                    restoredExportedA.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                    "Group " + i + ": Task A stale capacity must be cleaned on restore");
            Assertions.assertFalse(
                    restoredExportedA.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                    "Group " + i + ": Task A stale queue size must be cleaned on restore");
            Assertions.assertFalse(
                    restoredExportedA.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                    "Group " + i + ": Task A stale put blocked nanos must be cleaned on restore");
            Assertions.assertFalse(
                    restoredExportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE),
                    "Group "
                            + i
                            + ": Task A stale aggregate queue size must be cleaned on restore");

            // Verify Task A's unrelated cumulative metrics survive
            Assertions.assertEquals(
                    500L,
                    restoredExportedA.get(MetricNames.SOURCE_RECEIVED_COUNT),
                    "Group "
                            + i
                            + ": Task A unrelated cumulative metrics must survive restoration");

            // Task A accesses the queue second
            taskGroup2.getQueueCache(queueId, capacity, restoredContextA);

            // Verify exactly one queue metric owner in this group (Task B owns capacity 2048)
            Map<String, Long> restoredExportedB = exportDynamicCounters(restoredContextB, jobId);
            Assertions.assertEquals(
                    (long) capacity,
                    restoredExportedB.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId));
            Map<String, Long> postAccessExportedA = exportDynamicCounters(restoredContextA, jobId);
            Assertions.assertFalse(
                    postAccessExportedA.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId));

            restoredContextAList.add(restoredContextA);
            restoredContextBList.add(restoredContextB);
        }

        // 3. METRICS COLLECTION & AGGREGATION:
        MetricDescriptor tagger =
                DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER
                        .get()
                        .withTag(MetricTags.JOB_ID, String.valueOf(jobId));

        MetricsCompressor compressor = new MetricsCompressor();
        MetricsCollectionContext collectionContext = createMetricsCollectionContext(compressor);

        // Collect all ten restored worker contexts plus the non-queue coordinator context
        for (int i = 0; i < groupCount; i++) {
            restoredContextAList.get(i).provideDynamicMetrics(tagger, collectionContext);
            restoredContextBList.get(i).provideDynamicMetrics(tagger, collectionContext);
        }
        restoredCoordinatorContext.provideDynamicMetrics(tagger, collectionContext);

        RawJobMetrics rawJobMetrics = RawJobMetrics.of(compressor.getBlobAndReset());

        Method aggMethod =
                RealtimeMetricsService.class.getDeclaredMethod(
                        "aggregateRawMetrics", List.class, Set.class);
        aggMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, ?> aggByJobId =
                (Map<Long, ?>)
                        aggMethod.invoke(
                                null,
                                Collections.singletonList(rawJobMetrics),
                                Collections.singleton(jobId));
        Object jobAgg = aggByJobId.get(jobId);
        Assertions.assertNotNull(jobAgg, "jobAgg should not be null for enabled job");

        // Verify aggregated metric rows for queue ID -2 across five groups:
        // Exactly five metric owners for queue ID -2
        Object queueCapacityAgg = getField(jobAgg, "queueCapacity", Object.class);
        Object queueSizeAgg = getField(jobAgg, "queueSize", Object.class);
        Object queuePutBlockedNsAgg = getField(jobAgg, "queuePutBlockedNs", Object.class);

        Map<?, ?> capacityCountById = getField(queueCapacityAgg, "countById", Map.class);
        Map<?, ?> sizeCountById = getField(queueSizeAgg, "countById", Map.class);
        Map<?, ?> blockedCountById = getField(queuePutBlockedNsAgg, "countById", Map.class);

        Assertions.assertEquals(
                1,
                capacityCountById.size(),
                "Coordinator metrics must not appear in queue capacity aggregation");
        Assertions.assertEquals(
                1,
                sizeCountById.size(),
                "Coordinator metrics must not appear in queue size aggregation");
        Assertions.assertEquals(
                1,
                blockedCountById.size(),
                "Coordinator metrics must not appear in queue blocked nanos aggregation");
        Assertions.assertFalse(
                capacityCountById.containsKey(coordinatorTaskLocation.getTaskID()),
                "Coordinator task must not appear in queue capacity aggregation");
        Assertions.assertFalse(
                sizeCountById.containsKey(coordinatorTaskLocation.getTaskID()),
                "Coordinator task must not appear in queue size aggregation");
        Assertions.assertFalse(
                blockedCountById.containsKey(coordinatorTaskLocation.getTaskID()),
                "Coordinator task must not appear in queue blocked nanos aggregation");

        Assertions.assertEquals(
                5,
                capacityCountById.get(queueId),
                "Expected exactly five capacity metric rows (one per group) for queue ID -2");
        Assertions.assertEquals(
                5,
                sizeCountById.get(queueId),
                "Expected exactly five queue-size metric rows (one per group) for queue ID -2");
        Assertions.assertEquals(
                5,
                blockedCountById.get(queueId),
                "Expected exactly five blocked-time metric rows (one per group) for queue ID -2");

        // Baseline update into JobStore
        long bucketMs = 5000L;
        Object store = newJobStore(bucketMs, 3);
        long nowMs = System.currentTimeMillis();
        long baseMs = nowMs - bucketMs;

        invokeUpdate(store, baseMs, jobAgg);

        // Next window update (+5000ms):
        // Each of the five owners contributes another 2,500,000,000L blocked nanos (50% of bucket
        // allowance)
        for (int i = 0; i < groupCount; i++) {
            restoredContextBList
                    .get(i)
                    .counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                    .inc(2_500_000_000L);
        }

        MetricsCompressor nextCompressor = new MetricsCompressor();
        MetricsCollectionContext nextCollectionContext =
                createMetricsCollectionContext(nextCompressor);

        for (int i = 0; i < groupCount; i++) {
            restoredContextAList.get(i).provideDynamicMetrics(tagger, nextCollectionContext);
            restoredContextBList.get(i).provideDynamicMetrics(tagger, nextCollectionContext);
        }
        restoredCoordinatorContext.provideDynamicMetrics(tagger, nextCollectionContext);

        RawJobMetrics nextRaw = RawJobMetrics.of(nextCompressor.getBlobAndReset());

        @SuppressWarnings("unchecked")
        Map<Long, ?> nextAggByJobId =
                (Map<Long, ?>)
                        aggMethod.invoke(
                                null,
                                Collections.singletonList(nextRaw),
                                Collections.singleton(jobId));
        Object nextJobAgg = nextAggByJobId.get(jobId);

        invokeUpdate(store, nowMs, nextJobAgg);

        // Verify underlying bucket aggregations:
        // queueCapacity = 5 * 2048 = 10240
        long queueCapacity = latestEdgeBucketLong(store, queueId, "capacitySum");
        Assertions.assertEquals(
                10240L, queueCapacity, "queueCapacity must be 10240 (5 groups * 2048)");

        // subtaskCount = 5
        long subtaskCount = latestEdgeBucketLong(store, queueId, "subtaskCount");
        Assertions.assertEquals(5L, subtaskCount, "subtaskCount must be 5");

        // emitBlockedNs = 5 * 2,500,000,000 = 12,500,000,000
        long emitBlockedNs = latestEdgeBucketLong(store, queueId, "emitBlockedNs");
        Assertions.assertEquals(12_500_000_000L, emitBlockedNs);

        // queueSizeSum = 5 * 2048 = 10240
        long queueSizeSum = latestEdgeBucketLong(store, queueId, "queueSizeSum");
        Assertions.assertEquals(
                10240L, queueSizeSum, "queueSizeSum must be 10240 (5 groups * 2048)");

        // Verify edge response aggregation using the populated in-memory store.
        RealtimeMetricsService service = new RealtimeMetricsService(null, null);
        try {
            Field jobStoresField = RealtimeMetricsService.class.getDeclaredField("jobStores");
            jobStoresField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Long, Object> jobStores = (Map<Long, Object>) jobStoresField.get(service);
            jobStores.put(jobId, store);

            Map<String, Object> edgesResponse = service.getJobEdges(jobId, 60_000L);
            Assertions.assertNotNull(edgesResponse);
            Assertions.assertEquals(bucketMs, edgesResponse.get("bucketMs"));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> edges =
                    (List<Map<String, Object>>) edgesResponse.get("edges");
            Assertions.assertEquals(1, edges.size());
            Map<String, Object> edge = edges.get(0);
            Assertions.assertEquals(queueId, edge.get("queueId"));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> points = (List<Map<String, Object>>) edge.get("points");
            Assertions.assertFalse(points.isEmpty());
            Map<String, Object> latestPoint = points.get(points.size() - 1);

            // Assertions for the 5-group full queue scenario:
            Assertions.assertEquals(5L, latestPoint.get("subtaskCount"));
            Assertions.assertEquals(10240L, latestPoint.get("queueCapacity"));
            Assertions.assertEquals(10240L, latestPoint.get("queueSize"));
            Assertions.assertEquals(1.0, (Double) latestPoint.get("queueFillRatio"), 1e-6);

            // Backpressure denominator: TimeUnit.MILLISECONDS.toNanos(bucketMs) * subtaskCount
            // Denominator = 5_000_000_000 * 5 = 25_000_000_000.0
            // emitBlockedNs = 12_500_000_000.0
            // bpRatio = 12_500_000_000.0 / 25_000_000_000.0 = 0.50
            double expectedBpRatio =
                    ((double) emitBlockedNs)
                            / ((double) TimeUnit.MILLISECONDS.toNanos(bucketMs) * subtaskCount);
            Assertions.assertEquals(expectedBpRatio, (Double) latestPoint.get("bpRatio"), 1e-6);
        } finally {
            service.shutdown();
        }
    }

    private static Object newJobStore(long bucketMs, int retentionMinutes) throws Exception {
        Class<?> cls = Class.forName(RealtimeMetricsService.class.getName() + "$JobStore");
        Constructor<?> c = cls.getDeclaredConstructor(long.class, int.class);
        c.setAccessible(true);
        return c.newInstance(bucketMs, retentionMinutes);
    }

    private static Object newJobAggWithSourceReadNs(long vertexId, long sum) throws Exception {
        Object jobAgg = newJobAgg();
        Object aggById = getField(jobAgg, "sourceReadNs", Object.class);
        invokeAggAdd(aggById, vertexId, sum);
        return jobAgg;
    }

    private static Object newJobAggWithQueuePutBlockedNs(long queueId, long sum) throws Exception {
        Object jobAgg = newJobAgg();
        Object aggById = getField(jobAgg, "queuePutBlockedNs", Object.class);
        invokeAggAdd(aggById, queueId, sum);
        return jobAgg;
    }

    private static Object newJobAgg() throws Exception {
        Class<?> cls = Class.forName(RealtimeMetricsService.class.getName() + "$JobAgg");
        Constructor<?> c = cls.getDeclaredConstructor();
        c.setAccessible(true);
        return c.newInstance();
    }

    private static void invokeUpdate(Object store, long nowMs, Object jobAgg) throws Exception {
        Method m = store.getClass().getDeclaredMethod("update", long.class, jobAggParamClass());
        m.setAccessible(true);
        m.invoke(store, nowMs, jobAgg);
    }

    private static Class<?> jobAggParamClass() throws Exception {
        return Class.forName(RealtimeMetricsService.class.getName() + "$JobAgg");
    }

    private static void invokeAggAdd(Object aggById, long id, long value) throws Exception {
        Method add = aggById.getClass().getDeclaredMethod("add", long.class, long.class);
        add.setAccessible(true);
        add.invoke(aggById, id, value);
    }

    private static long latestVertexBucketLong(Object store, long vertexId, String field)
            throws Exception {
        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object lastBucket = buckets.peekLast();
        Assertions.assertNotNull(lastBucket);
        Map<?, ?> vertices = getField(lastBucket, "vertices", Map.class);
        Object vertexBucket = vertices.get(vertexId);
        Assertions.assertNotNull(vertexBucket);
        return getField(vertexBucket, field, long.class);
    }

    private static long latestEdgeBucketLong(Object store, long queueId, String field)
            throws Exception {
        Deque<?> buckets = getField(store, "buckets", Deque.class);
        Object lastBucket = buckets.peekLast();
        Assertions.assertNotNull(lastBucket);
        Map<?, ?> edges = getField(lastBucket, "edges", Map.class);
        Object edgeBucket = edges.get(queueId);
        Assertions.assertNotNull(edgeBucket);
        return getField(edgeBucket, field, long.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String field, Class<T> type) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return (T) f.get(target);
    }

    private static MetricsCollectionContext createMetricsCollectionContext(
            MetricsCompressor compressor) {
        return new MetricsCollectionContext() {
            @Override
            public void collect(
                    MetricDescriptor descriptor,
                    String name,
                    ProbeLevel probeLevel,
                    ProbeUnit probeUnit,
                    long value) {
                compressor.addLong(descriptor.copy().withMetric(name), value);
            }

            @Override
            public void collect(MetricDescriptor descriptor, Object value) {}

            @Override
            public void collect(
                    MetricDescriptor descriptor,
                    String name,
                    ProbeLevel level,
                    ProbeUnit unit,
                    double value) {}

            @Override
            public void collect(MetricDescriptor descriptor, long value) {}

            @Override
            public void collect(MetricDescriptor descriptor, double value) {}
        };
    }
}
