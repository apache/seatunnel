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

import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
