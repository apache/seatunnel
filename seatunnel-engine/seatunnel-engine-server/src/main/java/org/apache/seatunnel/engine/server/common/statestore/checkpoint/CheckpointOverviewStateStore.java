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

package org.apache.seatunnel.engine.server.common.statestore.checkpoint;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.common.statestore.StateStore;

import java.util.function.Consumer;

/**
 * Store for checkpoint monitoring overview state.
 *
 * <p>This store keeps checkpoint overview data used by monitoring and REST queries. It is
 * operational and observability-oriented state rather than core failover state.
 */
public interface CheckpointOverviewStateStore extends StateStore<Long, CheckpointOverview> {

    /**
     * Updates the overview for the given job and pipeline.
     *
     * <p>If no overview exists for the job, a new one is created. If no pipeline overview exists,
     * it is created before applying the updater.
     *
     * @param jobId job identifier
     * @param pipelineId pipeline identifier
     * @param updater pipeline-level updater
     */
    void updateOverview(long jobId, int pipelineId, Consumer<PipelineCheckpointOverview> updater);

    /**
     * Returns the number of jobs currently tracked by the overview store.
     *
     * @return tracked job count
     */
    long getOverviewJobCount();

    /**
     * Returns the total number of in-progress checkpoints across all tracked jobs and pipelines.
     *
     * @return in-progress checkpoint count
     */
    long getInProgressCheckpointCount();

    /**
     * Returns the total number of retained checkpoint history entries across all tracked jobs and
     * pipelines.
     *
     * @return retained checkpoint history entry count
     */
    long getRetainedHistoryCount();
}
