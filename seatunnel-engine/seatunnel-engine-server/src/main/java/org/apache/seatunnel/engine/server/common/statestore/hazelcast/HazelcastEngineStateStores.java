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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.common.jar.ConnectorJarReferenceStateStore;
import org.apache.seatunnel.engine.server.common.jar.hazelcast.HazelcastConnectorJarReferenceStateStore;
import org.apache.seatunnel.engine.server.common.statestore.AuthoritativeStateStores;
import org.apache.seatunnel.engine.server.common.statestore.AuxiliaryStateStores;
import org.apache.seatunnel.engine.server.common.statestore.DefaultAuthoritativeStateStores;
import org.apache.seatunnel.engine.server.common.statestore.DefaultAuxiliaryStateStores;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.hazelcast.HazelcastCheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.cleanup.PendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.common.statestore.cleanup.hazelcast.HazelcastPendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.hazelcast.HazelcastCounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.HistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.ObservableHistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.hazelcast.HazelcastHistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.hazelcast.HazelcastMetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.common.statestore.runtime.RuntimeStateStore;
import org.apache.seatunnel.engine.server.common.statestore.runtime.hazelcast.HazelcastRuntimeStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.CHECKPOINT_ID;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.CHECKPOINT_MONITOR;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.CONNECTOR_JAR_REF_COUNTERS;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.FINISHED_JOB_METRICS;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.FINISHED_JOB_STATE;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.FINISHED_JOB_VERTEX_INFO;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.OWNED_SLOT_PROFILES;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.PENDING_PIPELINE_CLEANUP;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.RUNNING_JOB_INFO;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.RUNNING_JOB_METRICS;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.RUNNING_JOB_STATE;
import static org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames.STATE_TIMESTAMPS;

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

            RuntimeStateStore<Long, JobInfo> runningJobInfoStore =
                    new HazelcastRuntimeStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(RUNNING_JOB_INFO));
            RuntimeStateStore<Object, Object> runningJobStateStore =
                    new HazelcastRuntimeStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(RUNNING_JOB_STATE));
            RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore =
                    new HazelcastRuntimeStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(STATE_TIMESTAMPS));
            RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
                    ownedSlotProfilesStore =
                            new HazelcastRuntimeStateStore<>(
                                    nodeEngine.getHazelcastInstance().getMap(OWNED_SLOT_PROFILES));
            HistoricalStateStore<Long, JobHistoryService.JobState> finishedJobStateStore =
                    new HazelcastHistoricalStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(FINISHED_JOB_STATE));
            HistoricalStateStore<Long, JobMetrics> finishedJobMetricsStore =
                    new HazelcastHistoricalStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(FINISHED_JOB_METRICS));
            ObservableHistoricalStateStore<Long, JobDAGInfo> finishedJobDagInfoStore =
                    new HazelcastHistoricalStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(FINISHED_JOB_VERTEX_INFO));
            MetricsSnapshotStateStore metricsSnapshotStore =
                    new HazelcastMetricsSnapshotStateStore(
                            nodeEngine.getHazelcastInstance().getMap(RUNNING_JOB_METRICS),
                            metricsPartitionCount);
            CounterStateStore<String> checkpointCounterStore =
                    new HazelcastCounterStateStore<>(
                            nodeEngine.getHazelcastInstance().getMap(CHECKPOINT_ID));
            PendingPipelineCleanupStore pendingPipelineCleanupStore =
                    new HazelcastPendingPipelineCleanupStore(
                            nodeEngine.getHazelcastInstance().getMap(PENDING_PIPELINE_CLEANUP));
            CheckpointOverviewStateStore checkpointOverviewStateStore =
                    new HazelcastCheckpointOverviewStateStore(
                            nodeEngine.getHazelcastInstance().getMap(CHECKPOINT_MONITOR));
            ConnectorJarReferenceStateStore connectorJarReferenceStateStore =
                    new HazelcastConnectorJarReferenceStateStore(
                            nodeEngine.getHazelcastInstance().getMap(CONNECTOR_JAR_REF_COUNTERS));
            this.authoritativeStateStores =
                    new DefaultAuthoritativeStateStores(
                            runningJobInfoStore,
                            runningJobStateStore,
                            runningJobStateTimestampsStore,
                            ownedSlotProfilesStore,
                            checkpointCounterStore,
                            pendingPipelineCleanupStore,
                            connectorJarReferenceStateStore);
            this.auxiliaryStateStores =
                    new DefaultAuxiliaryStateStores(
                            finishedJobStateStore,
                            finishedJobMetricsStore,
                            finishedJobDagInfoStore,
                            metricsSnapshotStore,
                            checkpointOverviewStateStore);
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
        // no-op
    }
}
