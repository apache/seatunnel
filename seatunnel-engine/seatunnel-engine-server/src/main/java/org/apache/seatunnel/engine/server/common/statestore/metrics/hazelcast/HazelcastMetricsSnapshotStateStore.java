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

import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Implementation backed by a partitioned Hazelcast metrics {@link IMap}. */
public class HazelcastMetricsSnapshotStateStore
        implements MetricsSnapshotStateStore, AutoCloseable {

    private final IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> metricsImap;
    private final int partitionCount;
    private final AtomicLong activePartitionKeyCount = new AtomicLong();
    private final AtomicLong taskSnapshotCount = new AtomicLong();
    private final Object metricsStatsLock = new Object();
    private final Set<Long> dirtyPartitionKeys = ConcurrentHashMap.newKeySet();
    private final Map<Long, RunningJobMetricsStats> statsByPartitionKey = new HashMap<>();
    private final AtomicBoolean statsInitializing = new AtomicBoolean(false);
    private volatile UUID metricsListenerId;

    public HazelcastMetricsSnapshotStateStore(
            IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> metricsImap,
            int partitionCount) {
        this.metricsImap = metricsImap;
        this.partitionCount = partitionCount;
        initStats();
    }

    @Override
    public void merge(Map<TaskLocation, SeaTunnelMetricsContext> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return;
        }

        Map<Long, Map<TaskLocation, SeaTunnelMetricsContext>> partitioned = new HashMap<>();
        snapshot.forEach(
                (key, value) -> {
                    long partition = partition(key);
                    partitioned.computeIfAbsent(partition, k -> new HashMap<>()).put(key, value);
                });

        partitioned
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            metricsImap.compute(
                                    entry.getKey(),
                                    (k, oldVal) -> {
                                        if (oldVal == null) oldVal = new HashMap<>();
                                        oldVal.putAll(entry.getValue());
                                        return oldVal;
                                    });
                        });
    }

    @Override
    public SeaTunnelMetricsContext get(TaskLocation taskLocation) {
        Map<TaskLocation, SeaTunnelMetricsContext> partitionMap =
                metricsImap.get(partition(taskLocation));
        if (partitionMap == null) {
            return null;
        }
        return partitionMap.get(taskLocation);
    }

    @Override
    public void remove(final TaskLocation taskLocation) {
        metricsImap.compute(
                partition(taskLocation),
                (ignored, current) -> {
                    if (current == null) {
                        return null;
                    }
                    Map<TaskLocation, SeaTunnelMetricsContext> updated = new HashMap<>(current);
                    updated.remove(taskLocation);
                    return updated.isEmpty() ? null : updated;
                });
    }

    @Override
    public void removePipeline(final PipelineLocation pipelineLocation) {
        Map<Long, List<TaskLocation>> partitionedTasks = new HashMap<>();
        for (Map.Entry<Long, Map<TaskLocation, SeaTunnelMetricsContext>> entry :
                metricsImap.entrySet()) {
            long partition = entry.getKey();
            List<TaskLocation> tasksToRemove =
                    entry.getValue().keySet().stream()
                            .filter(
                                    t ->
                                            t.getTaskGroupLocation()
                                                    .getPipelineLocation()
                                                    .equals(pipelineLocation))
                            .collect(Collectors.toList());
            if (!tasksToRemove.isEmpty()) {
                partitionedTasks.put(partition, tasksToRemove);
            }
        }

        partitionedTasks
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            long partition = entry.getKey();
                            List<TaskLocation> tasks = entry.getValue();
                            metricsImap.compute(
                                    partition,
                                    (k, oldVal) -> {
                                        if (oldVal != null) {
                                            tasks.forEach(oldVal::remove);
                                            if (oldVal.isEmpty()) return null;
                                        }
                                        return oldVal;
                                    });
                        });
    }

    @Override
    public boolean containsPipeline(PipelineLocation pipelineLocation) {
        for (Map<TaskLocation, SeaTunnelMetricsContext> partitionMap : metricsImap.values()) {
            boolean found =
                    partitionMap.keySet().stream()
                            .anyMatch(
                                    taskLocation ->
                                            pipelineLocation.equals(
                                                    taskLocation
                                                            .getTaskGroupLocation()
                                                            .getPipelineLocation()));
            if (found) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return Math.toIntExact(taskSnapshotCount.get());
    }

    @Override
    public boolean isEmpty() {
        return taskSnapshotCount.get() == 0L;
    }

    @Override
    public int activePartitionKeyCount() {
        return Math.toIntExact(activePartitionKeyCount.get());
    }

    private long partition(TaskLocation taskLocation) {
        return (taskLocation.hashCode() & Integer.MAX_VALUE) % partitionCount;
    }

    @Override
    public void close() {
        removeMetricsListener();
    }

    private void initStats() {
        removeMetricsListener();
        metricsListenerId = metricsImap.addEntryListener(new MetricsEntryListener(), true);
        statsInitializing.set(true);
        dirtyPartitionKeys.clear();

        Map<Long, RunningJobMetricsStats> snapshotStats = new HashMap<>();
        metricsImap.forEach(
                (partitionKey, metrics) ->
                        snapshotStats.put(partitionKey, toRunningJobMetricsStats(metrics)));

        Set<Long> dirtyKeysDuringSnapshot = new HashSet<>(dirtyPartitionKeys);
        for (Long partitionKey : dirtyKeysDuringSnapshot) {
            snapshotStats.put(
                    partitionKey, toRunningJobMetricsStats(metricsImap.get(partitionKey)));
        }

        synchronized (metricsStatsLock) {
            statsByPartitionKey.clear();
            activePartitionKeyCount.set(0L);
            taskSnapshotCount.set(0L);
            snapshotStats.forEach(this::replaceRunningJobMetricsStatsLocked);
            statsInitializing.set(false);

            Set<Long> postSnapshotDirtyKeys = new HashSet<>(dirtyPartitionKeys);
            dirtyPartitionKeys.clear();
            for (Long partitionKey : postSnapshotDirtyKeys) {
                replaceRunningJobMetricsStatsLocked(
                        partitionKey, toRunningJobMetricsStats(metricsImap.get(partitionKey)));
            }
        }
    }

    private RunningJobMetricsStats toRunningJobMetricsStats(
            Map<TaskLocation, SeaTunnelMetricsContext> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return RunningJobMetricsStats.EMPTY;
        }
        return new RunningJobMetricsStats(1L, metrics.size());
    }

    private void replaceRunningJobMetricsStatsLocked(
            Long partitionKey, RunningJobMetricsStats stats) {
        RunningJobMetricsStats currentStats = statsByPartitionKey.get(partitionKey);
        if (currentStats != null) {
            activePartitionKeyCount.addAndGet(-currentStats.partitionKeyCount);
            taskSnapshotCount.addAndGet(-currentStats.taskSnapshotCount);
        }

        if (stats.isEmpty()) {
            statsByPartitionKey.remove(partitionKey);
            return;
        }

        statsByPartitionKey.put(partitionKey, stats);
        activePartitionKeyCount.addAndGet(stats.partitionKeyCount);
        taskSnapshotCount.addAndGet(stats.taskSnapshotCount);
    }

    private void replaceRunningJobMetricsStats(
            Long partitionKey, Map<TaskLocation, SeaTunnelMetricsContext> metrics) {
        if (statsInitializing.get()) {
            dirtyPartitionKeys.add(partitionKey);
            return;
        }
        synchronized (metricsStatsLock) {
            if (statsInitializing.get()) {
                dirtyPartitionKeys.add(partitionKey);
                return;
            }
            replaceRunningJobMetricsStatsLocked(partitionKey, toRunningJobMetricsStats(metrics));
        }
    }

    private void removeMetricsListener() {
        if (metricsListenerId != null) {
            metricsImap.removeEntryListener(metricsListenerId);
            metricsListenerId = null;
        }
        statsInitializing.set(false);
        dirtyPartitionKeys.clear();
        synchronized (metricsStatsLock) {
            statsByPartitionKey.clear();
        }
        activePartitionKeyCount.set(0L);
        taskSnapshotCount.set(0L);
    }

    private final class MetricsEntryListener
            implements EntryAddedListener<Long, Map<TaskLocation, SeaTunnelMetricsContext>>,
                    EntryUpdatedListener<Long, Map<TaskLocation, SeaTunnelMetricsContext>>,
                    EntryRemovedListener<Long, Map<TaskLocation, SeaTunnelMetricsContext>> {

        @Override
        public void entryAdded(EntryEvent<Long, Map<TaskLocation, SeaTunnelMetricsContext>> event) {
            replaceRunningJobMetricsStats(event.getKey(), event.getValue());
        }

        @Override
        public void entryUpdated(
                EntryEvent<Long, Map<TaskLocation, SeaTunnelMetricsContext>> event) {
            replaceRunningJobMetricsStats(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(
                EntryEvent<Long, Map<TaskLocation, SeaTunnelMetricsContext>> event) {
            replaceRunningJobMetricsStats(event.getKey(), null);
        }
    }

    private static final class RunningJobMetricsStats {
        private static final RunningJobMetricsStats EMPTY = new RunningJobMetricsStats(0L, 0L);

        private final long partitionKeyCount;
        private final long taskSnapshotCount;

        private RunningJobMetricsStats(long partitionKeyCount, long taskSnapshotCount) {
            this.partitionKeyCount = partitionKeyCount;
            this.taskSnapshotCount = taskSnapshotCount;
        }

        private boolean isEmpty() {
            return partitionKeyCount == 0L && taskSnapshotCount == 0L;
        }
    }
}
