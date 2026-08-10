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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IncrementalSourceReaderTest {

    @Test
    void shouldPruneRemovedTablesFromRestoredIncrementalSplit() {
        TableId retainedTable = TableId.parse("inventory.customers");
        TableId removedTable = TableId.parse("inventory.orders");
        Map<TableId, byte[]> historyTableChanges = new HashMap<>();
        historyTableChanges.put(retainedTable, new byte[] {1});
        historyTableChanges.put(removedTable, new byte[] {2});
        IncrementalSplit restoredSplit =
                new IncrementalSplit(
                        "incremental-0",
                        Arrays.asList(retainedTable, removedTable),
                        new TestOffset(),
                        null,
                        Arrays.asList(
                                completedSplit("customers-0", retainedTable),
                                completedSplit("orders-0", removedTable)),
                        Arrays.asList(catalogTable(retainedTable), catalogTable(removedTable)),
                        historyTableChanges);

        IncrementalSplit prunedSplit =
                IncrementalSourceReader.pruneRemovedTables(
                        restoredSplit, new HashSet<>(Collections.singletonList(retainedTable)));

        assertEquals(Collections.singletonList(retainedTable), prunedSplit.getTableIds());
        assertEquals(1, prunedSplit.getCompletedSnapshotSplitInfos().size());
        assertEquals(
                retainedTable, prunedSplit.getCompletedSnapshotSplitInfos().get(0).getTableId());
        assertEquals(
                Collections.singleton(retainedTable),
                prunedSplit.getHistoryTableChanges().keySet());
        assertEquals(
                Collections.singletonList(retainedTable),
                prunedSplit.getCheckpointTables().stream()
                        .map(table -> toTableId(table.getTableId()))
                        .collect(Collectors.toList()));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldPreserveLegacyCheckpointDataTypeWhenPruningRestoredIncrementalSplit() {
        TableId retainedTable = TableId.parse("inventory.customers");
        TableId removedTable = TableId.parse("inventory.orders");
        IncrementalSplit legacyRestoredSplit =
                new IncrementalSplit(
                        "incremental-0",
                        Arrays.asList(retainedTable, removedTable),
                        new TestOffset(),
                        null,
                        Arrays.asList(
                                completedSplit("customers-0", retainedTable),
                                completedSplit("orders-0", removedTable)),
                        BasicType.STRING_TYPE);

        IncrementalSplit prunedSplit =
                IncrementalSourceReader.pruneRemovedTables(
                        legacyRestoredSplit,
                        new HashSet<>(Collections.singletonList(retainedTable)));

        assertEquals(BasicType.STRING_TYPE, prunedSplit.getCheckpointDataType());
    }

    private static CompletedSnapshotSplitInfo completedSplit(String splitId, TableId tableId) {
        return new CompletedSnapshotSplitInfo(
                splitId, tableId, null, null, null, new SnapshotSplitWatermark(null, null, null));
    }

    private static CatalogTable catalogTable(TableId tableId) {
        return CatalogTable.of(
                TableIdentifier.of(null, tableId.catalog(), tableId.schema(), tableId.table()),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null,
                null);
    }

    private static TableId toTableId(TableIdentifier tableIdentifier) {
        return new TableId(
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getSchemaName(),
                tableIdentifier.getTableName());
    }

    private static class TestOffset extends Offset {
        @Override
        public int compareTo(Offset offset) {
            return 0;
        }
    }
}
