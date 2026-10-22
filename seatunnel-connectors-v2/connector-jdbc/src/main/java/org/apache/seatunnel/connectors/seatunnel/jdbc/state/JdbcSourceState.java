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

package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class JdbcSourceState implements Serializable {
    private static final long serialVersionUID = -6441009212721284346L;
    /** Tables that have not yet been split into JDBC chunks. */
    private List<TablePath> pendingTables;
    /** Unassigned splits that still need to be handed back to source readers. */
    private Map<Integer, List<JdbcSourceSplit>> pendingSplits;
    /**
     * Remaining unfinished splits per table, including in-flight reader-owned splits that are no
     * longer present in {@code pendingSplits}.
     */
    private Map<TablePath, Integer> unfinishedSplitsPerTable;
    /**
     * Readers that have participated in each table and must receive the final close-table event.
     */
    private Map<TablePath, Set<Integer>> readersPerTable;

    public JdbcSourceState(
            List<TablePath> pendingTables, Map<Integer, List<JdbcSourceSplit>> pendingSplits) {
        this(pendingTables, pendingSplits, null, null);
    }

    public JdbcSourceState(
            List<TablePath> pendingTables,
            Map<Integer, List<JdbcSourceSplit>> pendingSplits,
            Map<TablePath, Integer> unfinishedSplitsPerTable,
            Map<TablePath, Set<Integer>> readersPerTable) {
        this.pendingTables = pendingTables;
        this.pendingSplits = pendingSplits;
        this.unfinishedSplitsPerTable = unfinishedSplitsPerTable;
        this.readersPerTable = readersPerTable;
    }

    public Map<TablePath, Set<Integer>> getReadersPerTableOrEmpty() {
        return readersPerTable == null ? new HashMap<>() : readersPerTable;
    }

    public Map<TablePath, Integer> getUnfinishedSplitsPerTableOrEmpty() {
        return unfinishedSplitsPerTable == null ? new HashMap<>() : unfinishedSplitsPerTable;
    }

    /** Returns true when the checkpoint was written before close-table tracking fields existed. */
    public boolean isLegacyTableState() {
        return unfinishedSplitsPerTable == null && readersPerTable == null;
    }
}
