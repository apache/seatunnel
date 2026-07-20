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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class IncrementalSourceTest {

    @Test
    void testSnapshotOnlyRestoreKeepsCheckpointedTables() {
        TableId processedTable = TableId.parse("db.processed");
        TableId remainingTable = TableId.parse("db.remaining");
        TableId remainingSplitTable = TableId.parse("db.remaining_split");
        TableId assignedSplitTable = TableId.parse("db.assigned_split");
        TableId newlyDiscoveredTable = TableId.parse("db.new");
        Set<TableId> checkpointedTables =
                new HashSet<>(
                        Arrays.asList(
                                processedTable,
                                remainingTable,
                                remainingSplitTable,
                                assignedSplitTable));
        SnapshotSplit remainingSplit =
                new SnapshotSplit("db.remaining_split.0", remainingSplitTable, null, null, null);
        SnapshotSplit assignedSplit =
                new SnapshotSplit("db.assigned_split.0", assignedSplitTable, null, null, null);
        HybridPendingSplitsState checkpointState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(processedTable),
                                Collections.singletonList(remainingSplit),
                                Collections.singletonMap(assignedSplit.splitId(), assignedSplit),
                                Collections.emptyMap(),
                                false,
                                Collections.singletonList(remainingTable),
                                false,
                                true),
                        null);

        Set<TableId> discoveredTables =
                new HashSet<>(
                        Arrays.asList(remainingTable, remainingSplitTable, newlyDiscoveredTable));

        Assertions.assertEquals(
                checkpointedTables,
                IncrementalSource.getCapturedTablesForRestore(
                        StartupMode.SNAPSHOT, discoveredTables, checkpointState));
        Assertions.assertEquals(
                discoveredTables,
                IncrementalSource.getCapturedTablesForRestore(
                        StartupMode.INITIAL, discoveredTables, checkpointState));
    }
}
