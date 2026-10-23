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

package org.apache.seatunnel.engine.server.common.statestore.metrics.hazelcast;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Covers metrics snapshot persistence and bounded mutation retries against real Hazelcast buckets.
 */
class HazelcastMetricsSnapshotStateStoreTest {

    private static HazelcastInstance hazelcastInstance;
    private static final String METRIC_NAME = "test.metric";

    @BeforeAll
    static void beforeAll() {
        Config config = new Config();
        config.setClusterName("HazelcastMetricsSnapshotStateStoreTest-" + System.nanoTime());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void afterAll() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void mergeShouldStoreAndOverwriteSnapshots() {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-merge");
        iMap.clear();
        HazelcastMetricsSnapshotStateStore store = new HazelcastMetricsSnapshotStateStore(iMap, 8);

        TaskLocation taskOne = taskLocation(1L, 10, 100L, 0L, 0);
        TaskLocation taskTwo = taskLocation(1L, 11, 101L, 0L, 0);
        SeaTunnelMetricsContext metricsOne = metricsContextWithCounterValue(1);
        SeaTunnelMetricsContext metricsTwo = metricsContextWithCounterValue(2);
        SeaTunnelMetricsContext updatedMetricsOne = metricsContextWithCounterValue(3);

        Map<TaskLocation, SeaTunnelMetricsContext> initialSnapshot = new LinkedHashMap<>();
        initialSnapshot.put(taskOne, metricsOne);
        initialSnapshot.put(taskTwo, metricsTwo);
        store.merge(initialSnapshot);

        assertCounterValue(1, store.get(taskOne));
        assertCounterValue(2, store.get(taskTwo));
        awaitSize(store, 2);

        store.merge(singletonSnapshot(taskOne, updatedMetricsOne));

        assertCounterValue(3, store.get(taskOne));
        assertCounterValue(2, store.get(taskTwo));
        awaitSize(store, 2);
    }

    @Test
    void removeShouldDeleteSingleTaskSnapshot() {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-remove");
        iMap.clear();
        HazelcastMetricsSnapshotStateStore store = new HazelcastMetricsSnapshotStateStore(iMap, 8);

        TaskLocation taskOne = taskLocation(2L, 20, 200L, 0L, 0);
        TaskLocation taskTwo = taskLocation(2L, 21, 201L, 0L, 0);
        SeaTunnelMetricsContext metricsOne = metricsContextWithCounterValue(10);
        SeaTunnelMetricsContext metricsTwo = metricsContextWithCounterValue(20);

        Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new LinkedHashMap<>();
        snapshot.put(taskOne, metricsOne);
        snapshot.put(taskTwo, metricsTwo);
        store.merge(snapshot);

        store.remove(taskOne);

        assertNull(store.get(taskOne));
        assertCounterValue(20, store.get(taskTwo));
        awaitSize(store, 1);

        store.remove(taskTwo);

        assertNull(store.get(taskTwo));
        awaitSize(store, 0);
    }

    @Test
    void removePipelineShouldDeleteOnlyMatchingPipelineSnapshots() {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-remove-pipeline");
        iMap.clear();
        HazelcastMetricsSnapshotStateStore store = new HazelcastMetricsSnapshotStateStore(iMap, 8);

        PipelineLocation pipelineToRemove = new PipelineLocation(3L, 30);
        PipelineLocation pipelineToKeep = new PipelineLocation(3L, 31);
        TaskLocation removedOne = taskLocation(3L, 30, 300L, 0L, 0);
        TaskLocation removedTwo = taskLocation(3L, 30, 301L, 0L, 0);
        TaskLocation kept = taskLocation(3L, 31, 302L, 0L, 0);
        SeaTunnelMetricsContext removedOneMetrics = metricsContextWithCounterValue(100);
        SeaTunnelMetricsContext removedTwoMetrics = metricsContextWithCounterValue(200);
        SeaTunnelMetricsContext keptMetrics = metricsContextWithCounterValue(300);

        Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new LinkedHashMap<>();
        snapshot.put(removedOne, removedOneMetrics);
        snapshot.put(removedTwo, removedTwoMetrics);
        snapshot.put(kept, keptMetrics);
        store.merge(snapshot);

        store.removePipeline(pipelineToRemove);

        assertNull(store.get(removedOne));
        assertNull(store.get(removedTwo));
        assertCounterValue(300, store.get(kept));
        awaitSize(store, 1);
        assertEquals(pipelineToKeep, kept.getTaskGroupLocation().getPipelineLocation());
    }

    @Test
    void sizeShouldCountTaskSnapshotsInsteadOfPartitionBuckets() {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-size");
        iMap.clear();
        HazelcastMetricsSnapshotStateStore store = new HazelcastMetricsSnapshotStateStore(iMap, 1);

        TaskLocation taskOne = taskLocation(4L, 40, 400L, 0L, 0);
        TaskLocation taskTwo = taskLocation(4L, 40, 401L, 0L, 1);

        Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new LinkedHashMap<>();
        snapshot.put(taskOne, metricsContextWithCounterValue(1));
        snapshot.put(taskTwo, metricsContextWithCounterValue(2));
        store.merge(snapshot);

        assertEquals(1, iMap.size());
        awaitSize(store, 2);
    }

    /**
     * A concurrent initial report must survive a put-if-absent conflict without losing either
     * writer's task snapshot.
     */
    @Test
    void mergeShouldPreserveConcurrentBucketCreation() {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-concurrent-create");
        TaskLocation reported = taskLocation(5L, 50, 500L, 0L, 0);
        TaskLocation concurrent = taskLocation(6L, 60, 600L, 0L, 0);
        AtomicInteger attempts = new AtomicInteger();
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted =
                withConcurrentWrites(
                        iMap,
                        partition -> {
                            if (attempts.getAndIncrement() == 0) {
                                iMap.set(
                                        partition,
                                        singletonSnapshot(
                                                concurrent, metricsContextWithCounterValue(2)));
                            }
                        });

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            store.merge(singletonSnapshot(reported, metricsContextWithCounterValue(1)));

            assertCounterValue(1, store.get(reported));
            assertCounterValue(2, store.get(concurrent));
            assertEquals(2, attempts.get());
            awaitSize(store, 2);
        }
    }

    /**
     * A retry must reread the bucket and retain another pipeline's snapshots even when the first
     * attempt would have removed the entire bucket.
     */
    @ParameterizedTest
    @EnumSource(Mutation.class)
    void mutationShouldPreserveConcurrentPipelineSnapshots(Mutation mutation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-concurrent-update-" + mutation);
        TaskLocation target = taskLocation(7L, 70, 700L, 0L, 0);
        TaskLocation concurrent = taskLocation(8L, 80, 800L, 0L, 0);
        iMap.put(0L, singletonSnapshot(target, metricsContextWithCounterValue(1)));
        AtomicInteger attempts = new AtomicInteger();
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted =
                withConcurrentWrites(
                        iMap,
                        partition -> {
                            if (attempts.getAndIncrement() == 0) {
                                Map<TaskLocation, SeaTunnelMetricsContext> snapshot =
                                        iMap.get(partition);
                                snapshot.put(concurrent, metricsContextWithCounterValue(2));
                                iMap.set(partition, snapshot);
                            }
                        });

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            applyMutation(store, target, mutation);

            assertCounterValue(2, store.get(concurrent));
            assertEquals(2, attempts.get());
            if (mutation == Mutation.MERGE) {
                assertCounterValue(100, store.get(target));
                awaitSize(store, 2);
            } else {
                assertNull(store.get(target));
                awaitSize(store, 1);
            }
        }
    }

    /**
     * The last allowed attempt may still succeed; exhausting earlier conflicts must not reject a
     * successful update or delete at the retry boundary.
     */
    @ParameterizedTest
    @EnumSource(Mutation.class)
    void mutationShouldSucceedOnLastAllowedAttempt(Mutation mutation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-last-attempt-" + mutation);
        TaskLocation target = taskLocation(12L, 120, 1200L, 0L, 0);
        iMap.put(0L, singletonSnapshot(target, metricsContextWithCounterValue(0)));
        AtomicInteger attempts = new AtomicInteger();
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted =
                withConcurrentWrites(
                        iMap,
                        partition -> {
                            int attempt = attempts.incrementAndGet();
                            if (attempt < 10) {
                                iMap.set(
                                        partition,
                                        singletonSnapshot(
                                                target, metricsContextWithCounterValue(attempt)));
                            }
                        });

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            applyMutation(store, target, mutation);

            assertEquals(10, attempts.get());
            if (mutation == Mutation.MERGE) {
                assertCounterValue(100, store.get(target));
            } else {
                assertNull(store.get(target));
            }
        }
    }

    /**
     * Sustained conflicts must surface a failure while preserving the winning writer's snapshot.
     * The conflict injector also has a cap so the unfixed implementation fails instead of hanging.
     */
    @ParameterizedTest
    @EnumSource(Mutation.class)
    void mutationShouldFailAfterRepeatedConflicts(Mutation mutation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-conflict-limit-" + mutation);
        TaskLocation target = taskLocation(9L, 90, 900L, 0L, 0);
        iMap.put(0L, singletonSnapshot(target, metricsContextWithCounterValue(0)));
        AtomicInteger conflicts = new AtomicInteger();
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted =
                withConcurrentWrites(
                        iMap,
                        partition -> {
                            int conflict = conflicts.incrementAndGet();
                            assertTrue(conflict <= 20, "Metrics mutation kept retrying conflicts");
                            iMap.set(
                                    partition,
                                    singletonSnapshot(
                                            target, metricsContextWithCounterValue(conflict)));
                        });

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            SeaTunnelEngineException failure =
                    assertThrows(
                            SeaTunnelEngineException.class,
                            () -> applyMutation(store, target, mutation));

            assertTrue(failure.getMessage().contains("metrics partition 0"));
            assertEquals(10, conflicts.get());
            assertCounterValue(conflicts.get(), store.get(target));
            awaitSize(store, 1);
        }
    }

    /**
     * Interruption between the bucket read and mutation must stop the retry and retain the flag.
     */
    @ParameterizedTest
    @EnumSource(Mutation.class)
    void mutationShouldPreserveInterruption(Mutation mutation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-interrupted-" + mutation);
        TaskLocation target = taskLocation(10L, 100, 1000L, 0L, 0);
        iMap.put(0L, singletonSnapshot(target, metricsContextWithCounterValue(1)));
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted = spy(iMap);
        doAnswer(
                        invocation -> {
                            Long partition = invocation.getArgument(0);
                            BiFunction<
                                            Long,
                                            Map<TaskLocation, SeaTunnelMetricsContext>,
                                            Map<TaskLocation, SeaTunnelMetricsContext>>
                                    remapping = invocation.getArgument(1);
                            return iMap.compute(
                                    partition,
                                    (key, current) -> {
                                        Thread.currentThread().interrupt();
                                        return remapping.apply(key, current);
                                    });
                        })
                .when(intercepted)
                .compute(anyLong(), any());

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            try {
                assertThrows(
                        SeaTunnelEngineException.class,
                        () -> applyMutation(store, target, mutation));
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                // Do not leak the deliberately injected interrupt into other tests or shutdown.
                Thread.interrupted();
            }
            assertCounterValue(1, store.get(target));
        }
    }

    /**
     * Backend failures must reach the existing caller failure paths while leaving the previously
     * stored snapshot available for recovery.
     */
    @ParameterizedTest
    @EnumSource(Mutation.class)
    void mutationShouldPropagateBackendFailure(Mutation mutation) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap =
                hazelcastInstance.getMap("metrics-snapshot-backend-failure-" + mutation);
        TaskLocation target = taskLocation(11L, 110, 1100L, 0L, 0);
        iMap.put(0L, singletonSnapshot(target, metricsContextWithCounterValue(1)));
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted = spy(iMap);
        IllegalStateException failure = new IllegalStateException("test backend failure");
        doThrow(failure).when(intercepted).compute(anyLong(), any());

        try (HazelcastMetricsSnapshotStateStore store =
                new HazelcastMetricsSnapshotStateStore(intercepted, 1)) {
            assertSame(
                    failure,
                    assertThrows(
                            IllegalStateException.class,
                            () -> applyMutation(store, target, mutation)));
            assertCounterValue(1, store.get(target));
        }
    }

    /**
     * Routes each injected conflict through the public merge, task-removal or pipeline-removal
     * entry point, using the same target snapshot.
     */
    private static void applyMutation(
            HazelcastMetricsSnapshotStateStore store, TaskLocation target, Mutation mutation) {
        switch (mutation) {
            case MERGE:
                store.merge(singletonSnapshot(target, metricsContextWithCounterValue(100)));
                break;
            case REMOVE_TASK:
                store.remove(target);
                break;
            case REMOVE_PIPELINE:
                store.removePipeline(target.getTaskGroupLocation().getPipelineLocation());
                break;
            default:
                throw new AssertionError("Unexpected mutation: " + mutation);
        }
    }

    /**
     * Injects an actual map write between remapping and Hazelcast's conditional write. The real
     * compute loop, binary comparisons and serialization remain responsible for detecting
     * conflicts.
     */
    private static IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> withConcurrentWrites(
            IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> iMap,
            LongConsumer concurrentWrite) {
        IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> intercepted = spy(iMap);
        doAnswer(
                        invocation -> {
                            Long partition = invocation.getArgument(0);
                            BiFunction<
                                            Long,
                                            Map<TaskLocation, SeaTunnelMetricsContext>,
                                            Map<TaskLocation, SeaTunnelMetricsContext>>
                                    remapping = invocation.getArgument(1);
                            return iMap.compute(
                                    partition,
                                    (key, current) -> {
                                        Map<TaskLocation, SeaTunnelMetricsContext> updated =
                                                remapping.apply(key, current);
                                        concurrentWrite.accept(key);
                                        return updated;
                                    });
                        })
                .when(intercepted)
                .compute(anyLong(), any());
        return intercepted;
    }

    /**
     * Mutation entry points that share the retry limit and must preserve the same interruption and
     * backend failure behavior.
     */
    private enum Mutation {
        MERGE,
        REMOVE_TASK,
        REMOVE_PIPELINE
    }

    private static void awaitSize(HazelcastMetricsSnapshotStateStore store, int expectedSize) {
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(expectedSize, store.size()));
    }

    private static Map<TaskLocation, SeaTunnelMetricsContext> singletonSnapshot(
            TaskLocation taskLocation, SeaTunnelMetricsContext metricsContext) {
        Map<TaskLocation, SeaTunnelMetricsContext> snapshot = new HashMap<>();
        snapshot.put(taskLocation, metricsContext);
        return snapshot;
    }

    private static SeaTunnelMetricsContext metricsContextWithCounterValue(long value) {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        metricsContext.counter(METRIC_NAME).inc(value);
        return metricsContext;
    }

    private static void assertCounterValue(long expected, SeaTunnelMetricsContext metricsContext) {
        assertEquals(expected, metricsContext.counter(METRIC_NAME).getCount());
    }

    private static TaskLocation taskLocation(
            long jobId,
            int pipelineId,
            long taskGroupId,
            long taskInGroupIndex,
            int parallelismIndex) {
        return new TaskLocation(
                new TaskGroupLocation(jobId, pipelineId, taskGroupId),
                taskInGroupIndex,
                parallelismIndex);
    }
}
