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
 * Top-level bundle of state stores used directly by the engine.
 *
 * <p>This is the high-level port that lets engine code depend on state semantics rather than
 * concrete implementations such as {@code HazelcastRuntimeStateStore}.
 *
 * <p>Implementation construction is expected to stay in Hazelcast/RocksDB-specific providers, while
 * engine code depends only on this interface.
 */
public interface EngineStateStores extends AutoCloseable {
    /**
     * Returns the bundle of control state that must be treated as authoritative during leader
     * handoff.
     *
     * @return authoritative state stores
     */
    AuthoritativeStateStores authoritative();

    /**
     * Returns the bundle of auxiliary state used mainly for observability, recent history, or
     * cleanup.
     *
     * @return auxiliary state stores
     */
    AuxiliaryStateStores auxiliary();

    /**
     * Returns the store for runtime task metrics snapshots.
     *
     * @return metrics snapshot store
     */
    default MetricsSnapshotStateStore metricsSnapshotStore() {
        return auxiliary().metricsSnapshotStore();
    }

    /**
     * Returns the store for checkpoint overviews.
     *
     * @return checkpoint overview state store
     */
    default CheckpointOverviewStateStore checkpointOverviewStateStore() {
        return auxiliary().checkpointOverviewStateStore();
    }

    /**
     * Releases resources owned by the stores.
     *
     * <p>Hazelcast implementations are usually no-op, while RocksDB implementations use this to
     * close the underlying database resources.
     */
    @Override
    void close();
}
