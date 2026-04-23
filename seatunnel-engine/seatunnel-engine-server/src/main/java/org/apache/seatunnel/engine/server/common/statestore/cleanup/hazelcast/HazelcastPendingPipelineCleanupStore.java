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

package org.apache.seatunnel.engine.server.common.statestore.cleanup.hazelcast;

import org.apache.seatunnel.engine.server.common.statestore.cleanup.PendingPipelineCleanupStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;

import com.hazelcast.map.IMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Hazelcast-backed implementation of {@link PendingPipelineCleanupStore}.
 *
 * <p>This implementation preserves the conditional update semantics currently used by the
 * coordinator cleanup flow.
 */
public class HazelcastPendingPipelineCleanupStore implements PendingPipelineCleanupStore {

    private final IMap<PipelineLocation, PipelineCleanupRecord> cleanupIMap;

    public HazelcastPendingPipelineCleanupStore(
            IMap<PipelineLocation, PipelineCleanupRecord> cleanupIMap) {
        this.cleanupIMap = Objects.requireNonNull(cleanupIMap, "cleanupIMap");
    }

    @Override
    public PipelineCleanupRecord get(PipelineLocation pipelineLocation) {
        return cleanupIMap.get(pipelineLocation);
    }

    @Override
    public void put(
            PipelineLocation pipelineLocation, PipelineCleanupRecord pipelineCleanupRecord) {
        cleanupIMap.put(pipelineLocation, pipelineCleanupRecord);
    }

    @Override
    public PipelineCleanupRecord putIfAbsent(
            PipelineLocation pipelineLocation, PipelineCleanupRecord pipelineCleanupRecord) {
        return cleanupIMap.putIfAbsent(pipelineLocation, pipelineCleanupRecord);
    }

    @Override
    public void remove(PipelineLocation pipelineLocation) {
        cleanupIMap.remove(pipelineLocation);
    }

    @Override
    public boolean containsKey(PipelineLocation pipelineLocation) {
        return cleanupIMap.containsKey(pipelineLocation);
    }

    @Override
    public boolean replace(
            PipelineLocation pipelineLocation,
            PipelineCleanupRecord expected,
            PipelineCleanupRecord updated) {
        return cleanupIMap.replace(pipelineLocation, expected, updated);
    }

    @Override
    public boolean remove(PipelineLocation pipelineLocation, PipelineCleanupRecord expected) {
        return cleanupIMap.remove(pipelineLocation, expected);
    }

    @Override
    public Set<Map.Entry<PipelineLocation, PipelineCleanupRecord>> entrySet() {
        return cleanupIMap.entrySet();
    }

    @Override
    public Collection<PipelineCleanupRecord> values() {
        return cleanupIMap.values();
    }

    @Override
    public boolean isEmpty() {
        return cleanupIMap.isEmpty();
    }

    @Override
    public int size() {
        return cleanupIMap.size();
    }
}
