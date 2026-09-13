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

package org.apache.seatunnel.connectors.cdc.base.source.split;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** Verifies restored incremental split state is retained only for currently captured tables. */
public class IncrementalSplitTest {

    private static final TableId KEPT_TABLE =
            new TableId("alpha_online", null, "account_histories");
    private static final TableId REMOVED_TABLE =
            new TableId("alpha_online", null, "account_interests");

    /** Converts regular checkpoint table paths to the default Debezium table identifier format. */
    private static final Function<TablePath, TableId> DEFAULT_TABLE_ID_CONVERTER =
            tablePath ->
                    new TableId(
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

    @Test
    public void testPruneTablesRemovesDeletedTableState() {
        Map<TableId, byte[]> historyTableChanges = new HashMap<>();
        historyTableChanges.put(KEPT_TABLE, new byte[] {1});
        historyTableChanges.put(REMOVED_TABLE, new byte[] {2});

        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                        null,
                        null,
                        Arrays.asList(
                                completedSnapshotSplitInfo("kept-split", KEPT_TABLE),
                                completedSnapshotSplitInfo("removed-split", REMOVED_TABLE)),
                        Arrays.asList(catalogTable(KEPT_TABLE), catalogTable(REMOVED_TABLE)),
                        historyTableChanges);

        IncrementalSplit pruned =
                split.pruneTables(
                        Collections.singletonList(KEPT_TABLE), DEFAULT_TABLE_ID_CONVERTER);

        Assertions.assertEquals(Collections.singletonList(KEPT_TABLE), pruned.getTableIds());
        Assertions.assertEquals(1, pruned.getCompletedSnapshotSplitInfos().size());
        Assertions.assertEquals(
                KEPT_TABLE, pruned.getCompletedSnapshotSplitInfos().get(0).getTableId());
        Assertions.assertEquals(1, pruned.getCheckpointTables().size());
        Assertions.assertEquals(
                TablePath.of("alpha_online.account_histories"),
                pruned.getCheckpointTables().get(0).getTablePath());
        Assertions.assertEquals(
                Collections.singleton(KEPT_TABLE), pruned.getHistoryTableChanges().keySet());
    }

    @Test
    public void testPruneTablesPreservesLegacyCheckpointDataTypeWhenTableRemoved() {
        IncrementalSplit split = legacyCheckpointSplit();

        IncrementalSplit pruned =
                split.pruneTables(
                        Collections.singletonList(KEPT_TABLE), DEFAULT_TABLE_ID_CONVERTER);

        Assertions.assertEquals(Collections.singletonList(KEPT_TABLE), pruned.getTableIds());
        Assertions.assertSame(BasicType.STRING_TYPE, pruned.getCheckpointDataType());
    }

    @Test
    public void testPruneTablesPreservesLegacyCheckpointDataTypeWhenTablesUnchanged() {
        IncrementalSplit split = legacyCheckpointSplit();

        IncrementalSplit pruned =
                split.pruneTables(
                        Arrays.asList(KEPT_TABLE, REMOVED_TABLE), DEFAULT_TABLE_ID_CONVERTER);

        Assertions.assertEquals(Arrays.asList(KEPT_TABLE, REMOVED_TABLE), pruned.getTableIds());
        Assertions.assertSame(BasicType.STRING_TYPE, pruned.getCheckpointDataType());
    }

    /** Verifies checkpoint schemas use the dialect converter instead of a generic table id. */
    @Test
    public void testPruneTablesUsesDialectSpecificTableIdConverter() {
        TableId db2TableId = new TableId("", "DB2INST1", "CUSTOMERS");
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-db2",
                        Collections.singletonList(db2TableId),
                        null,
                        null,
                        Collections.emptyList(),
                        Collections.singletonList(
                                catalogTable(TablePath.of("SAMPLE", "DB2INST1", "CUSTOMERS"))),
                        Collections.emptyMap());

        IncrementalSplit pruned =
                split.pruneTables(
                        Collections.singletonList(db2TableId),
                        tablePath ->
                                new TableId(
                                        "", tablePath.getSchemaName(), tablePath.getTableName()));

        Assertions.assertEquals(Collections.singletonList(db2TableId), pruned.getTableIds());
        Assertions.assertEquals(1, pruned.getCheckpointTables().size());
        Assertions.assertEquals(
                TablePath.of("SAMPLE", "DB2INST1", "CUSTOMERS"),
                pruned.getCheckpointTables().get(0).getTablePath());
    }

    /**
     * Verifies pruneTables tolerates a null tableIds/completedSnapshotSplitInfos the same way it
     * already tolerates null checkpointTables/historyTableChanges, instead of throwing an NPE
     * during checkpoint recovery.
     */
    @Test
    public void testPruneTablesToleratesNullTableIdsAndCompletedSnapshotSplitInfos() {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-null-state", null, null, null, null, null, null);

        IncrementalSplit pruned =
                split.pruneTables(
                        Collections.singletonList(KEPT_TABLE), DEFAULT_TABLE_ID_CONVERTER);

        Assertions.assertTrue(pruned.getTableIds().isEmpty());
        Assertions.assertTrue(pruned.getCompletedSnapshotSplitInfos().isEmpty());
    }

    @SuppressWarnings("deprecation")
    private static IncrementalSplit legacyCheckpointSplit() {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-legacy",
                        Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                        null,
                        null,
                        Collections.emptyList());
        return new IncrementalSplit(split, BasicType.STRING_TYPE);
    }

    private static CompletedSnapshotSplitInfo completedSnapshotSplitInfo(
            String splitId, TableId tableId) {
        return new CompletedSnapshotSplitInfo(splitId, tableId, null, null, null, null);
    }

    private static CatalogTable catalogTable(TableId tableId) {
        return catalogTable(TablePath.of(tableId.catalog(), tableId.table()));
    }

    /** Creates a catalog table whose identifier is stored in checkpoint schema state. */
    private static CatalogTable catalogTable(TablePath tablePath) {
        return CatalogTable.of(
                TableIdentifier.of("test", tablePath),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }
}
