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
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.HistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.history.ObservableHistoricalStateStore;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.master.JobHistoryService;

/**
 * Bundle of state that is closer to observability, recent history, or cleanup than to failover
 * correctness.
 *
 * <p>This group collects states that can often tolerate more staleness, or for which limited loss
 * is less likely to break system correctness immediately.
 */
public interface AuxiliaryStateStores {

    /**
     * Returns the store for finished job state.
     *
     * @return finished job state store
     */
    HistoricalStateStore<Long, JobHistoryService.JobState> finishedJobStateStore();

    /**
     * Returns the store for finished job metrics.
     *
     * @return finished job metrics store
     */
    HistoricalStateStore<Long, JobMetrics> finishedJobMetricsStore();

    /**
     * Returns the store for finished job DAG info.
     *
     * <p>The current engine uses expiration events from this store to trigger log cleanup, so it is
     * exposed as an observable historical store.
     *
     * @return finished job dag info store
     */
    ObservableHistoricalStateStore<Long, JobDAGInfo> finishedJobDagInfoStore();

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
}
