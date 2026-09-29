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

import java.util.Objects;

/** Default immutable implementation of {@link AuxiliaryStateStores}. */
public class DefaultAuxiliaryStateStores implements AuxiliaryStateStores {
    private final MetricsSnapshotStateStore metricsSnapshotStore;
    private final CheckpointOverviewStateStore checkpointOverviewStateStore;

    public DefaultAuxiliaryStateStores(
            MetricsSnapshotStateStore metricsSnapshotStore,
            CheckpointOverviewStateStore checkpointOverviewStateStore) {
        this.metricsSnapshotStore =
                Objects.requireNonNull(metricsSnapshotStore, "metricsSnapshotStore");
        this.checkpointOverviewStateStore =
                Objects.requireNonNull(
                        checkpointOverviewStateStore, "checkpointOverviewStateStore");
    }

    @Override
    public MetricsSnapshotStateStore metricsSnapshotStore() {
        return metricsSnapshotStore;
    }

    @Override
    public CheckpointOverviewStateStore checkpointOverviewStateStore() {
        return checkpointOverviewStateStore;
    }

    @Override
    public void close() {
        closeIfPossible(checkpointOverviewStateStore);
        closeIfPossible(metricsSnapshotStore);
    }

    private void closeIfPossible(Object store) {
        if (!(store instanceof AutoCloseable)) {
            return;
        }
        try {
            ((AutoCloseable) store).close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close auxiliary state store", e);
        }
    }
}
