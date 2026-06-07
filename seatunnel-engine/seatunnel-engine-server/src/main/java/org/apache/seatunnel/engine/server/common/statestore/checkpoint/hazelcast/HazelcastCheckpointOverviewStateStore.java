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

package org.apache.seatunnel.engine.server.common.statestore.checkpoint.hazelcast;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;

import com.hazelcast.map.IMap;

import java.util.Objects;
import java.util.function.Consumer;

/** Hazelcast-backed implementation of {@link CheckpointOverviewStateStore}. */
public class HazelcastCheckpointOverviewStateStore implements CheckpointOverviewStateStore {

    private final IMap<Long, CheckpointOverview> overviewMap;

    public HazelcastCheckpointOverviewStateStore(IMap<Long, CheckpointOverview> overviewMap) {
        this.overviewMap = Objects.requireNonNull(overviewMap, "overviewMap");
    }

    @Override
    public CheckpointOverview get(Long jobId) {
        return overviewMap.get(jobId);
    }

    @Override
    public void put(Long jobId, CheckpointOverview overview) {
        overviewMap.put(jobId, overview);
    }

    @Override
    public CheckpointOverview putIfAbsent(Long jobId, CheckpointOverview overview) {
        return overviewMap.putIfAbsent(jobId, overview);
    }

    @Override
    public void remove(Long jobId) {
        overviewMap.remove(jobId);
    }

    @Override
    public boolean containsKey(Long jobId) {
        return overviewMap.containsKey(jobId);
    }

    @Override
    public boolean isEmpty() {
        return overviewMap.isEmpty();
    }

    @Override
    public int size() {
        return overviewMap.size();
    }

    @Override
    public void updateOverview(
            long jobId, int pipelineId, Consumer<PipelineCheckpointOverview> updater) {
        overviewMap.compute(
                jobId,
                (id, overview) -> {
                    CheckpointOverview snapshot =
                            overview == null ? new CheckpointOverview(jobId) : overview;
                    PipelineCheckpointOverview pipeline = snapshot.getOrCreatePipeline(pipelineId);
                    updater.accept(pipeline);
                    snapshot.setUpdatedAt(System.currentTimeMillis());
                    return snapshot;
                });
    }
}
