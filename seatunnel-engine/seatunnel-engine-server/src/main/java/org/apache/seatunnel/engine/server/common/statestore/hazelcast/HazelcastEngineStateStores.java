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

package org.apache.seatunnel.engine.server.common.statestore.hazelcast;

import org.apache.seatunnel.engine.server.common.statestore.AuthoritativeStateStores;
import org.apache.seatunnel.engine.server.common.statestore.AuxiliaryStateStores;
import org.apache.seatunnel.engine.server.common.statestore.DefaultAuthoritativeStateStores;
import org.apache.seatunnel.engine.server.common.statestore.DefaultAuxiliaryStateStores;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.hazelcast.HazelcastCheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.hazelcast.HazelcastMetricsSnapshotStateStore;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Objects;

import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.CHECKPOINT_MONITOR;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.RUNNING_JOB_METRICS;

/**
 * {@link EngineStateStores} implementation backed by Hazelcast.
 *
 * <p>Engine code is not expected to reference this implementation directly. It is intended to be
 * created only during bootstrap and injected through interfaces.
 */
public class HazelcastEngineStateStores implements EngineStateStores {

    private final NodeEngine nodeEngine;
    private final int metricsPartitionCount;
    private volatile AuthoritativeStateStores authoritativeStateStores;
    private volatile AuxiliaryStateStores auxiliaryStateStores;

    public HazelcastEngineStateStores(NodeEngine nodeEngine, int metricsPartitionCount) {
        Objects.requireNonNull(nodeEngine, "nodeEngine");
        this.nodeEngine = nodeEngine;
        this.metricsPartitionCount = metricsPartitionCount;
    }

    private void ensureInitialized() {
        if (authoritativeStateStores != null && auxiliaryStateStores != null) {
            return;
        }
        synchronized (this) {
            if (authoritativeStateStores != null && auxiliaryStateStores != null) {
                return;
            }

            MetricsSnapshotStateStore metricsSnapshotStore =
                    new HazelcastMetricsSnapshotStateStore(
                            nodeEngine.getHazelcastInstance().getMap(RUNNING_JOB_METRICS),
                            metricsPartitionCount);
            CheckpointOverviewStateStore checkpointOverviewStateStore =
                    new HazelcastCheckpointOverviewStateStore(
                            nodeEngine.getHazelcastInstance().getMap(CHECKPOINT_MONITOR));
            this.authoritativeStateStores = new DefaultAuthoritativeStateStores();
            this.auxiliaryStateStores =
                    new DefaultAuxiliaryStateStores(
                            metricsSnapshotStore, checkpointOverviewStateStore);
        }
    }

    @Override
    public AuthoritativeStateStores authoritative() {
        ensureInitialized();
        return authoritativeStateStores;
    }

    @Override
    public AuxiliaryStateStores auxiliary() {
        ensureInitialized();
        return auxiliaryStateStores;
    }

    @Override
    public void close() {
        if (auxiliaryStateStores != null) {
            auxiliaryStateStores.close();
        }
        if (authoritativeStateStores != null) {
            authoritativeStateStores.close();
        }
    }
}
