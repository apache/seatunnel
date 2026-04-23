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

import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.Map;

/** Implementation backed by a partitioned Hazelcast metrics {@link IMap}. */
public class HazelcastMetricsSnapshotStateStore implements MetricsSnapshotStateStore {

    private final IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> metricsImap;
    private final int partitionCount;

    public HazelcastMetricsSnapshotStateStore(
            IMap<Long, Map<TaskLocation, SeaTunnelMetricsContext>> metricsImap,
            int partitionCount) {
        this.metricsImap = metricsImap;
        this.partitionCount = partitionCount;
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
        return partitionMap == null ? null : partitionMap.get(taskLocation);
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
        for (long partition = 0; partition < partitionCount; partition++) {
            // Clean each metrics bucket in one compute to avoid full-map scans and removal races.
            metricsImap.compute(
                    partition,
                    (k, oldVal) -> {
                        if (oldVal == null || oldVal.isEmpty()) {
                            return oldVal;
                        }
                        oldVal.entrySet()
                                .removeIf(
                                        entry ->
                                                pipelineLocation.equals(
                                                        entry.getKey()
                                                                .getTaskGroupLocation()
                                                                .getPipelineLocation()));
                        return oldVal.isEmpty() ? null : oldVal;
                    });
        }
    }

    @Override
    public int size() {
        int count = 0;
        for (Map<TaskLocation, SeaTunnelMetricsContext> partitionMap : metricsImap.values()) {
            count += partitionMap.size();
        }
        return count;
    }

    private long partition(TaskLocation taskLocation) {
        return (taskLocation.hashCode() & Integer.MAX_VALUE) % partitionCount;
    }
}
