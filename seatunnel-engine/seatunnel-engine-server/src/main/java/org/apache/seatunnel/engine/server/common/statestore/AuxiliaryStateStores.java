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

package org.apache.seatunnel.engine.server.common.statestore;

import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;

/**
 * Bundle of state that is closer to observability, recent history, or cleanup than to failover
 * correctness.
 *
 * <p>This group collects states that can often tolerate more staleness, or for which limited loss
 * is less likely to break system correctness immediately.
 */
public interface AuxiliaryStateStores extends AutoCloseable {
    /**
     * Returns the store for runtime task metrics snapshots.
     *
     * @return metrics snapshot store
     */
    MetricsSnapshotStateStore metricsSnapshotStore();

    /**
     * Returns the store for checkpoint overviews.
     *
     * @return checkpoint overview state store
     */
    CheckpointOverviewStateStore checkpointOverviewStateStore();

    /**
     * Releases resources owned by auxiliary stores.
     *
     * <p>Implementations that only wrap non-closeable stores may keep the default no-op behavior.
     */
    @Override
    default void close() {
        // no-op
    }
}
