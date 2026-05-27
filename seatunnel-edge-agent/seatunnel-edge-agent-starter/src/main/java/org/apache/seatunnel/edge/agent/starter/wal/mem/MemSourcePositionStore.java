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

package org.apache.seatunnel.edge.agent.starter.wal.mem;

import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * In-memory position store for NON delivery mode. Positions live only for the current process
 * lifetime and are lost on restart — matching the stateless guarantee of NON mode.
 */
public class MemSourcePositionStore implements EdgeSourcePositionStore {

    private final Map<String, Map<String, EdgeSourcePosition>> store = new HashMap<>();

    @Override
    public EdgeSourcePosition load(String sourceId, String partition) {
        Map<String, EdgeSourcePosition> partitions = store.get(sourceId);
        return partitions != null ? partitions.get(partition) : null;
    }

    @Override
    public Map<String, EdgeSourcePosition> loadBySource(String sourceId) {
        return store.getOrDefault(sourceId, Collections.emptyMap());
    }

    @Override
    public void save(EdgeSourcePosition position) {
        store.computeIfAbsent(position.getSourceId(), k -> new HashMap<>())
                .put(position.getPartition(), position);
    }
}
