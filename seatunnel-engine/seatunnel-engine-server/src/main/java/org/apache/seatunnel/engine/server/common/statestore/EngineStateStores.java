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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.cleanup.PendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.HistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.ObservableHistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.common.statestore.runtime.RuntimeStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import java.util.Map;

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
     * Returns the store for running job info.
     *
     * @return running job info store
     */
    default RuntimeStateStore<Long, JobInfo> runningJobInfoStore() {
        return authoritative().runningJobInfoStore();
    }

    /**
     * Returns the store for running job, pipeline, or task execution state.
     *
     * <p>The current engine structure mixes multiple key/value shapes, so this contract remains
     * {@code Object}-based for now.
     *
     * @return running job state store
     */
    default RuntimeStateStore<Object, Object> runningJobStateStore() {
        return authoritative().runningJobStateStore();
    }

    /**
     * Returns the store for timestamps associated with running state.
     *
     * @return running job state timestamps store
     */
    default RuntimeStateStore<Object, Long[]> runningJobStateTimestampsStore() {
        return authoritative().runningJobStateTimestampsStore();
    }

    /**
     * Returns the store for owned slot profiles by pipeline.
     *
     * @return owned slot profiles store
     */
    default RuntimeStateStore<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesStore() {
        return authoritative().ownedSlotProfilesStore();
    }

    /**
     * Returns the store for finished job state.
     *
     * @return finished job state store
     */
    default HistoricalStateStore<Long, JobHistoryService.JobState> finishedJobStateStore() {
        return auxiliary().finishedJobStateStore();
    }

    /**
     * Returns the store for finished job metrics.
     *
     * @return finished job metrics store
     */
    default HistoricalStateStore<Long, JobMetrics> finishedJobMetricsStore() {
        return auxiliary().finishedJobMetricsStore();
    }

    /**
     * Returns the store for finished job DAG info.
     *
     * <p>The current engine uses expiration events from this store to trigger log cleanup, so it is
     * exposed as an observable historical store.
     *
     * @return finished job dag info store
     */
    default ObservableHistoricalStateStore<Long, JobDAGInfo> finishedJobDagInfoStore() {
        return auxiliary().finishedJobDagInfoStore();
    }

    /**
     * Returns the store for runtime task metrics snapshots.
     *
     * @return metrics snapshot store
     */
    default MetricsSnapshotStateStore metricsSnapshotStore() {
        return auxiliary().metricsSnapshotStore();
    }

    /**
     * Returns the checkpoint ID counter store.
     *
     * <p>The current key format is a string composed from {@code jobId + pipelineId}.
     *
     * @return checkpoint counter store
     */
    default CounterStateStore<String> checkpointCounterStore() {
        return authoritative().checkpointCounterStore();
    }

    /**
     * Returns the store for pending pipeline cleanup.
     *
     * @return pending pipeline cleanup store
     */
    default PendingPipelineCleanupStore pendingPipelineCleanupStore() {
        return authoritative().pendingPipelineCleanupStore();
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
