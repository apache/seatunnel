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

package org.apache.seatunnel.edge.agent.connector;

import java.util.Map;

public interface EdgeSourcePositionStore {

    /**
     * Loads the last persisted position for one source partition.
     *
     * <p>Called from {@code EdgeInputReader.open()} or during poll to resume tailing. Returns
     * {@code null} when no position exists (start from configured beginning).
     *
     * @param sourceId logical input id from agent YAML
     * @param partition file path or other partition key
     * @return stored position or {@code null}
     * @throws Exception if the backing store read fails
     */
    EdgeSourcePosition load(String sourceId, String partition) throws Exception;

    /**
     * Loads all partitions for a source id.
     *
     * <p>Used when a reader manages multiple files under one {@code input.id}.
     *
     * @param sourceId logical input id
     * @return map of partition key to position (empty if none)
     * @throws Exception if the read fails
     */
    Map<String, EdgeSourcePosition> loadBySource(String sourceId) throws Exception;

    /**
     * Persists the latest read position for a partition.
     *
     * <p>Called from the scheduler when flushing events to WAL (same batch as {@code
     * WalStore.append}). Positions should advance monotonically per partition.
     *
     * @param position position to persist (source id and partition required)
     * @throws Exception if the write fails
     */
    void save(EdgeSourcePosition position) throws Exception;
}
