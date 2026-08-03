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

package org.apache.seatunnel.engine.server.common.statestore.metrics;

import org.apache.seatunnel.engine.server.common.statestore.StateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import java.util.Collections;
import java.util.Map;

/**
 * Store for runtime task metrics snapshots.
 *
 * <p>Here, {@code merge()} does not mean merging the internals of {@link SeaTunnelMetricsContext}.
 * It means batch-applying the latest snapshots for multiple tasks.
 *
 * <p>In the current SeaTunnel metrics reporting flow, each task sends its latest {@link
 * SeaTunnelMetricsContext} to the coordinator, and the coordinator replaces the snapshot by task
 * key. The Hazelcast implementation uses {@code compute()} internally to safely update the outer
 * partition map, while the RocksDB implementation uses task location directly as the key, so
 * task-level overwrite preserves the same storage meaning. Even when a backend uses internal
 * partition buckets, the exposed {@link StateStore} contract remains task-based, so {@link #size()}
 * must report the number of task snapshots rather than backend-specific buckets.
 */
public interface MetricsSnapshotStateStore
        extends StateStore<TaskLocation, SeaTunnelMetricsContext> {

    /**
     * Applies metrics snapshots for multiple tasks in a single batch.
     *
     * @param snapshot snapshot batch to apply
     */
    void merge(Map<TaskLocation, SeaTunnelMetricsContext> snapshot);

    /**
     * Stores a single task snapshot by delegating to {@link #merge(Map)}.
     *
     * @param taskLocation task location to store
     * @param metricsContext metrics snapshot to store
     */
    @Override
    default void put(TaskLocation taskLocation, SeaTunnelMetricsContext metricsContext) {
        merge(Collections.singletonMap(taskLocation, metricsContext));
    }

    /**
     * Checks whether a task snapshot exists.
     *
     * @param taskLocation task location to check
     * @return {@code true} if a snapshot exists for the task
     */
    @Override
    default boolean containsKey(TaskLocation taskLocation) {
        return get(taskLocation) != null;
    }

    /**
     * Conditional insertion is intentionally not exposed for metrics snapshots because the current
     * engine model treats them as latest-snapshot overwrites.
     *
     * @param taskLocation task location to store
     * @param metricsContext metrics snapshot to store
     * @return never returns normally
     */
    @Override
    default SeaTunnelMetricsContext putIfAbsent(
            TaskLocation taskLocation, SeaTunnelMetricsContext metricsContext) {
        throw new UnsupportedOperationException(
                "Metrics snapshots are updated through merge semantics rather than putIfAbsent.");
    }

    /**
     * Removes all task metrics belonging to a specific pipeline.
     *
     * @param pipelineLocation pipeline location to remove
     */
    void removePipeline(PipelineLocation pipelineLocation);

    /**
     * Checks whether any task snapshot exists for a specific pipeline.
     *
     * @param pipelineLocation pipeline location to check
     * @return {@code true} if any snapshot exists for the pipeline
     */
    boolean containsPipeline(PipelineLocation pipelineLocation);

    /**
     * Returns the number of active backend partition buckets currently holding task snapshots.
     *
     * <p>This value is observability-oriented and may differ from {@link #size()}, which reports
     * logical task snapshot count.
     *
     * @return active backend partition bucket count
     */
    int activePartitionKeyCount();
}
