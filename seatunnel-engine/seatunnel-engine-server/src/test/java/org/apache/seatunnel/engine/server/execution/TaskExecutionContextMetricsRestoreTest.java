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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.group.AbstractTaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateBlockingQueue;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateDisruptor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.seatunnel.engine.server.metrics.MetricsTestUtils.exportDynamicCounters;

public class TaskExecutionContextMetricsRestoreTest {

    private Map<TaskLocation, SeaTunnelMetricsContext> snapshotMap;
    private TaskExecutionContext executionContext;

    @BeforeEach
    void setUp() {
        snapshotMap = new HashMap<>();
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
        executionContext = new TaskExecutionContext(null, null, engineContext, null);
    }

    @Test
    void testGetOrCreateMetricsContextCleansIntermediateQueueMetricsOnRestore() {
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1001L, 1, 1L);
        TaskLocation taskLocationA = new TaskLocation(taskGroupLocation, 0L, 0);

        long queueId = -2L;
        SeaTunnelMetricsContext snapshotContextA = new SeaTunnelMetricsContext();
        // Transient queue metrics from old run
        snapshotContextA.counter(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId).set(2048);
        snapshotContextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(100);
        snapshotContextA
                .counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                .set(50_000);
        snapshotContextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE).set(100);

        // Unrelated metrics that must survive
        snapshotContextA.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(500);
        snapshotContextA.counter(MetricNames.SOURCE_RECEIVED_BYTES).inc(10240);

        snapshotMap.put(taskLocationA, snapshotContextA);

        // Perform restore
        SeaTunnelMetricsContext restoredContextA =
                executionContext.getOrCreateMetricsContext(taskLocationA);

        Map<String, Long> exported = exportDynamicCounters(restoredContextA, 1001L);

        // IntermediateQueueCapacity#-2 is removed
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                "IntermediateQueueCapacity#-2 should be removed");

        // IntermediateQueueSize#-2 is removed
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                "IntermediateQueueSize#-2 should be removed");

        // IntermediateQueuePutBlockedNanos#-2 is removed
        Assertions.assertFalse(
                exported.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                "IntermediateQueuePutBlockedNanos#-2 should be removed");

        // Aggregate intermediate queue metrics are handled according to cleanup implementation
        Assertions.assertFalse(
                exported.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE),
                "Aggregate intermediateQueueSize without suffix should be removed");

        // Unrelated source metrics survive with original values
        Assertions.assertTrue(
                exported.containsKey(MetricNames.SOURCE_RECEIVED_COUNT),
                "SOURCE_RECEIVED_COUNT should survive restoration");
        Assertions.assertEquals(500L, exported.get(MetricNames.SOURCE_RECEIVED_COUNT));
        Assertions.assertTrue(
                exported.containsKey(MetricNames.SOURCE_RECEIVED_BYTES),
                "SOURCE_RECEIVED_BYTES should survive restoration");
        Assertions.assertEquals(10240L, exported.get(MetricNames.SOURCE_RECEIVED_BYTES));

        // Newly registered queue metrics have fresh values
        restoredContextA.counter(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId).set(4096);
        restoredContextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(5);

        Map<String, Long> exportedAfterRegister = exportDynamicCounters(restoredContextA, 1001L);
        Assertions.assertEquals(
                4096L,
                exportedAfterRegister.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId));
        Assertions.assertEquals(
                5L, exportedAfterRegister.get(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId));
    }

    @Test
    void testGetOrCreateMetricsContextReturnsFreshContextForNewTask() {
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1001L, 1, 1L);
        TaskLocation newTaskLocation = new TaskLocation(taskGroupLocation, 2L, 0);

        SeaTunnelMetricsContext context =
                executionContext.getOrCreateMetricsContext(newTaskLocation);

        Assertions.assertNotNull(context);
        Map<String, Long> exported = exportDynamicCounters(context, 1001L);
        Assertions.assertTrue(exported.isEmpty(), "Fresh context should have no metrics");
    }

    @Test
    void testQueueOwnershipChangeAcrossRestoration_BlockingQueue() {
        runQueueOwnershipChangeAcrossRestoration(
                (loc, name) ->
                        new TaskGroupWithIntermediateBlockingQueue(
                                loc, name, Collections.emptyList()));
    }

    @Test
    void testQueueOwnershipChangeAcrossRestoration_Disruptor() {
        runQueueOwnershipChangeAcrossRestoration(
                (loc, name) ->
                        new TaskGroupWithIntermediateDisruptor(loc, name, Collections.emptyList()));
    }

    private void runQueueOwnershipChangeAcrossRestoration(
            BiFunction<TaskGroupLocation, String, AbstractTaskGroupWithIntermediateQueue>
                    taskGroupFactory) {
        long jobId = 1001L;
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        TaskLocation taskLocationA = new TaskLocation(taskGroupLocation, 0L, 0);
        TaskLocation taskLocationB = new TaskLocation(taskGroupLocation, 1L, 0);
        long queueId = -2L;
        int capacity = 2048;

        // 1. FIRST EXECUTION:
        // Create Task A and Task B contexts and the first queue group
        SeaTunnelMetricsContext contextA = new SeaTunnelMetricsContext();
        SeaTunnelMetricsContext contextB = new SeaTunnelMetricsContext();

        AbstractTaskGroupWithIntermediateQueue taskGroup1 =
                taskGroupFactory.apply(taskGroupLocation, "group1");
        taskGroup1.init();

        // Task A obtains the queue first, registering its metrics
        taskGroup1.getQueueCache(queueId, capacity, contextA);

        // Task B obtains the same queue second, reusing it without re-registering queue metrics
        taskGroup1.getQueueCache(queueId, capacity, contextB);

        // Simulate queue activity on Task A's queue
        contextA.counter(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId).set(50);
        contextA.counter(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId)
                .inc(10_000);

        // Register unrelated cumulative metric on Task A
        contextA.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(500);

        // Verify initial queue ownership on first execution before restoration:
        Map<String, Long> initialExportedA = exportDynamicCounters(contextA, jobId);
        Map<String, Long> initialExportedB = exportDynamicCounters(contextB, jobId);

        // 1. Task A owns and exports all three queue-specific metrics with initial values
        Assertions.assertEquals(
                2048L,
                initialExportedA.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                "Initial execution: Task A must export intermediate queue capacity 2048");
        Assertions.assertEquals(
                50L,
                initialExportedA.get(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                "Initial execution: Task A must export intermediate queue size 50");
        Assertions.assertEquals(
                10_000L,
                initialExportedA.get(
                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                "Initial execution: Task A must export intermediate queue put blocked nanos 10000");

        // 2. Task B does not own or export any of the queue-specific metrics
        Assertions.assertFalse(
                initialExportedB.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                "Initial execution: Task B must not export intermediate queue capacity");
        Assertions.assertFalse(
                initialExportedB.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                "Initial execution: Task B must not export intermediate queue size");
        Assertions.assertFalse(
                initialExportedB.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                "Initial execution: Task B must not export intermediate queue put blocked nanos");

        // Store both task metrics contexts in the snapshot store
        snapshotMap.put(taskLocationA, contextA);
        snapshotMap.put(taskLocationB, contextB);

        // 2. RESTORATION:
        // Create a new queue group with an empty queue cache
        AbstractTaskGroupWithIntermediateQueue taskGroup2 =
                taskGroupFactory.apply(taskGroupLocation, "group2");
        taskGroup2.init();

        // Restore Task B first using getOrCreateMetricsContext()
        SeaTunnelMetricsContext restoredContextB =
                executionContext.getOrCreateMetricsContext(taskLocationB);
        // Task B obtains the queue first, making it the new metrics owner in taskGroup2
        taskGroup2.getQueueCache(queueId, capacity, restoredContextB);

        // Restore Task A second using getOrCreateMetricsContext()
        SeaTunnelMetricsContext restoredContextA =
                executionContext.getOrCreateMetricsContext(taskLocationA);
        // Task A obtains the already-created queue second
        taskGroup2.getQueueCache(queueId, capacity, restoredContextA);

        // 3. ASSERTIONS:
        Map<String, Long> exportedA = exportDynamicCounters(restoredContextA, jobId);
        Map<String, Long> exportedB = exportDynamicCounters(restoredContextB, jobId);

        // Task A's restored context must not contain the old queue metrics
        Assertions.assertFalse(
                exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                "Stale snapshot registrations must not survive ownership changes: Task A should not contain old queue capacity");
        Assertions.assertFalse(
                exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                "Stale snapshot registrations must not survive ownership changes: Task A should not contain old queue size");
        Assertions.assertFalse(
                exportedA.containsKey(
                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                "Stale snapshot registrations must not survive ownership changes: Task A should not contain old blocked nanos");

        // Task A retains SourceReceivedCount = 500
        Assertions.assertEquals(
                500L,
                exportedA.get(MetricNames.SOURCE_RECEIVED_COUNT),
                "Task A must retain its cumulative source metrics across restoration");

        // Task B contains the new queue capacity metric with value 2048
        Assertions.assertEquals(
                2048L,
                exportedB.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                "Task B must contain the newly registered queue capacity with value 2048");
        Assertions.assertEquals(
                0L,
                exportedB.get(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                "Task B must contain the newly registered queue size with initial value 0");
        Assertions.assertEquals(
                0L,
                exportedB.get(MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                "Task B must contain the newly registered queue put blocked nanos with initial value 0");

        // Exactly one of the two contexts exports the queue-ID-specific capacity metric
        int capacityOwnerCount =
                (exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId)
                                ? 1
                                : 0)
                        + (exportedB.containsKey(
                                        MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId)
                                ? 1
                                : 0);
        Assertions.assertEquals(
                1,
                capacityOwnerCount,
                "Stale snapshot registrations must not survive ownership changes: exactly one task context must export queue capacity");

        // Exactly one of the two contexts exports the queue-ID-specific blocked nanos metric
        int blockedNanosOwnerCount =
                (exportedA.containsKey(
                                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS
                                                + "#"
                                                + queueId)
                                ? 1
                                : 0)
                        + (exportedB.containsKey(
                                        MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS
                                                + "#"
                                                + queueId)
                                ? 1
                                : 0);
        Assertions.assertEquals(
                1,
                blockedNanosOwnerCount,
                "Stale snapshot registrations must not survive ownership changes: exactly one task context must export queue put blocked nanos");

        // Exactly one of the two contexts exports the queue-ID-specific size metric
        int sizeOwnerCount =
                (exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId) ? 1 : 0)
                        + (exportedB.containsKey(
                                        MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId)
                                ? 1
                                : 0);
        Assertions.assertEquals(
                1,
                sizeOwnerCount,
                "Stale snapshot registrations must not survive ownership changes: exactly one task context must export queue size");
    }

    @Test
    void testRepeatedRestorationWithAlternatingQueueOwnership_BlockingQueue() {
        runRepeatedRestorationWithAlternatingQueueOwnership(
                (loc, name) ->
                        new TaskGroupWithIntermediateBlockingQueue(
                                loc, name, Collections.emptyList()));
    }

    @Test
    void testRepeatedRestorationWithAlternatingQueueOwnership_Disruptor() {
        runRepeatedRestorationWithAlternatingQueueOwnership(
                (loc, name) ->
                        new TaskGroupWithIntermediateDisruptor(loc, name, Collections.emptyList()));
    }

    private void runRepeatedRestorationWithAlternatingQueueOwnership(
            BiFunction<TaskGroupLocation, String, AbstractTaskGroupWithIntermediateQueue>
                    taskGroupFactory) {
        long jobId = 1001L;
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        TaskLocation taskLocationA = new TaskLocation(taskGroupLocation, 0L, 0);
        TaskLocation taskLocationB = new TaskLocation(taskGroupLocation, 1L, 0);
        long queueId = -2L;
        int capacity = 2048;

        // 5 total executions: execution 1 is initial, executions 2..5 are four restoration cycles
        for (int execution = 1; execution <= 5; execution++) {
            // Alternate ownership: Executions 1, 3, 5 -> Task A is owner; Executions 2, 4 -> Task B
            // is owner
            boolean taskAIsOwner = (execution % 2 != 0);

            SeaTunnelMetricsContext contextA;
            SeaTunnelMetricsContext contextB;

            if (execution == 1) {
                // Initial execution: fresh contexts
                contextA = new SeaTunnelMetricsContext();
                contextB = new SeaTunnelMetricsContext();
            } else {
                // Restoration cycles: restore BOTH task contexts through
                // getOrCreateMetricsContext()
                // which invokes cleanIntermediateQueueMetrics() on the restored snapshots
                contextA = executionContext.getOrCreateMetricsContext(taskLocationA);
                contextB = executionContext.getOrCreateMetricsContext(taskLocationB);
            }

            SeaTunnelMetricsContext ownerContext = taskAIsOwner ? contextA : contextB;
            SeaTunnelMetricsContext followerContext = taskAIsOwner ? contextB : contextA;

            // Fresh task group for every execution
            AbstractTaskGroupWithIntermediateQueue taskGroup =
                    taskGroupFactory.apply(taskGroupLocation, "group-exec-" + execution);
            taskGroup.init();

            // Initialize the designated owner first
            taskGroup.getQueueCache(queueId, capacity, ownerContext);

            // Initialize the follower second (reuses queue without re-registering queue metrics)
            taskGroup.getQueueCache(queueId, capacity, followerContext);

            // Increment Task A's cumulative source counter by 100
            contextA.counter(MetricNames.SOURCE_RECEIVED_COUNT).inc(100);

            // Save both contexts into snapshotMap for subsequent restoration
            snapshotMap.put(taskLocationA, contextA);
            snapshotMap.put(taskLocationB, contextB);

            // Export metrics and verify ownership
            Map<String, Long> exportedA = exportDynamicCounters(contextA, jobId);
            Map<String, Long> exportedB = exportDynamicCounters(contextB, jobId);
            Map<String, Long> ownerExported = taskAIsOwner ? exportedA : exportedB;
            Map<String, Long> followerExported = taskAIsOwner ? exportedB : exportedA;

            // The owner exports queue capacity value 2048
            Assertions.assertEquals(
                    2048L,
                    ownerExported.get(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                    "Execution "
                            + execution
                            + ": The designated queue owner must export queue capacity 2048");

            // The follower contains none of the three queue-specific metric keys
            Assertions.assertFalse(
                    followerExported.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId),
                    "Execution "
                            + execution
                            + ": The follower context must not export stale queue capacity");
            Assertions.assertFalse(
                    followerExported.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId),
                    "Execution "
                            + execution
                            + ": The follower context must not export stale queue size");
            Assertions.assertFalse(
                    followerExported.containsKey(
                            MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + queueId),
                    "Execution "
                            + execution
                            + ": The follower context must not export stale queue blocked nanos");

            // Exactly one context contains each of the three queue-specific metrics
            int capacityOwnerCount =
                    (exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId)
                                    ? 1
                                    : 0)
                            + (exportedB.containsKey(
                                            MetricNames.INTERMEDIATE_QUEUE_CAPACITY + "#" + queueId)
                                    ? 1
                                    : 0);
            Assertions.assertEquals(
                    1,
                    capacityOwnerCount,
                    "Execution " + execution + ": Exactly one context must export queue capacity");

            int sizeOwnerCount =
                    (exportedA.containsKey(MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId)
                                    ? 1
                                    : 0)
                            + (exportedB.containsKey(
                                            MetricNames.INTERMEDIATE_QUEUE_SIZE + "#" + queueId)
                                    ? 1
                                    : 0);
            Assertions.assertEquals(
                    1,
                    sizeOwnerCount,
                    "Execution " + execution + ": Exactly one context must export queue size");

            int blockedNanosOwnerCount =
                    (exportedA.containsKey(
                                            MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS
                                                    + "#"
                                                    + queueId)
                                    ? 1
                                    : 0)
                            + (exportedB.containsKey(
                                            MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS
                                                    + "#"
                                                    + queueId)
                                    ? 1
                                    : 0);
            Assertions.assertEquals(
                    1,
                    blockedNanosOwnerCount,
                    "Execution "
                            + execution
                            + ": Exactly one context must export queue put blocked nanos");

            // Task A retains its cumulative SourceReceivedCount across restorations (100, 200, 300,
            // 400, 500)
            long expectedSourceCount = execution * 100L;
            Assertions.assertEquals(
                    expectedSourceCount,
                    exportedA.get(MetricNames.SOURCE_RECEIVED_COUNT),
                    "Execution "
                            + execution
                            + ": Task A must retain its cumulative SourceReceivedCount across restorations");
        }
    }
}
