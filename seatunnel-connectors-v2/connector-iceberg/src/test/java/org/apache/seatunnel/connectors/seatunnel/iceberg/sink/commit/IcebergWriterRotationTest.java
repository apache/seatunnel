/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergWriterFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Writes through a real task writer with {@code flush_row_size} set and commits through the
 * aggregated committer, then reads the table back with Iceberg's own reader.
 */
class IcebergWriterRotationTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("db", "events");

    private InMemoryCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
    }

    @AfterEach
    void tearDown() throws Exception {
        catalog.close();
    }

    @Test
    void appendOnlyRotationKeepsOneSnapshotPerCheckpoint() throws Exception {
        IcebergSinkConfig config = config(2L, false, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());

        List<WriteResult> results =
                write(table, config, insert(1, "a"), insert(2, "b"), insert(3, "c"));

        assertEquals(2, results.size(), "two rows per task writer, so three rows need two");
        commit(table, config, 1L, results);
        assertEquals(rows("1=a", "2=b", "3=c"), read(table));
        assertEquals(1, snapshotCount(table));
        assertNull(
                table.currentSnapshot()
                        .summary()
                        .get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_COMMIT_GROUP));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "bucket(id, 4)"})
    void upsertDeleteAndUpdateAfterRotationApplyToEarlierRows(String partitionKeys)
            throws Exception {
        IcebergSinkConfig config = config(2L, true, partitionKeys);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());

        // the writer is rotated after the second row, so key 1 and key 2 are inserted by the
        // first task writer and deleted or updated by the second one
        List<WriteResult> results =
                write(
                        table,
                        config,
                        insert(1, "a"),
                        insert(2, "b"),
                        row(RowKind.DELETE, 1, "a"),
                        row(RowKind.UPDATE_AFTER, 2, "b2"),
                        insert(3, "c"));

        assertEquals(3, results.size());
        commit(table, config, 1L, results);

        assertEquals(rows("2=b2", "3=c"), read(table));
        assertEquals(3, snapshotCount(table));
        assertEquals(
                "3",
                table.currentSnapshot()
                        .summary()
                        .get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_COMMIT_GROUP_COUNT));
    }

    @Test
    void singleSnapshotForRotatedUpsertResultsLeavesStaleRow() throws Exception {
        // Shows why the committer keeps the order: committing the same files as one snapshot,
        // as the committer did before, keeps the deleted and the replaced row.
        IcebergSinkConfig config = config(2L, true, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());

        List<WriteResult> results =
                write(
                        table,
                        config,
                        insert(1, "a"),
                        insert(2, "b"),
                        row(RowKind.DELETE, 1, "a"),
                        row(RowKind.UPDATE_AFTER, 2, "b2"));
        RowDelta rowDelta = table.newRowDelta();
        for (WriteResult result : results) {
            result.getDataFiles().forEach(rowDelta::addRows);
            result.getDeleteFiles().forEach(rowDelta::addDeletes);
        }
        rowDelta.commit();

        assertEquals(rows("1=a", "2=b", "2=b2"), read(table));
    }

    @Test
    void cdcUpdateAfterRotationReplacesEarlierRow() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
        options.put(IcebergSinkOptions.FLUSH_ROW_SIZE.key(), 1L);
        IcebergSinkConfig config = sinkConfig(options);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());

        List<WriteResult> results =
                write(
                        table,
                        config,
                        insert(1, "a"),
                        row(RowKind.UPDATE_BEFORE, 1, "a"),
                        row(RowKind.UPDATE_AFTER, 1, "a2"));

        commit(table, config, 1L, results);
        assertEquals(rows("1=a2"), read(table));
    }

    @Test
    void parallelWritersWithDifferentRotationCounts() throws Exception {
        IcebergSinkConfig config = config(1L, true, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());

        List<WriteResult> writer0 =
                write(
                        table,
                        config,
                        insert(1, "a"),
                        row(RowKind.UPDATE_AFTER, 1, "a2"),
                        row(RowKind.DELETE, 1, "a2"));
        List<WriteResult> writer1 = write(table, config, insert(2, "b"));

        IcebergAggregatedCommitter committer = committer(table, config);
        committer.commit(
                Collections.singletonList(
                        committer.combine(
                                Arrays.asList(
                                        new IcebergCommitInfo(writer0, 1L),
                                        new IcebergCommitInfo(writer1, 1L)))));

        assertEquals(rows("2=b"), read(table));
    }

    @Test
    void restoreCommitsOnlyGroupsMissingFromTable() throws Exception {
        IcebergSinkConfig config = config(2L, true, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        List<WriteResult> results =
                write(
                        table,
                        config,
                        insert(1, "a"),
                        insert(2, "b"),
                        row(RowKind.DELETE, 1, "a"),
                        insert(3, "c"));
        List<IcebergCommitInfo> infos =
                Collections.singletonList(new IcebergCommitInfo(results, 7L));
        List<List<WriteResult>> groups = IcebergAggregatedCommitter.planCommits(infos);
        assertEquals(2, groups.size());

        // the job failed after the first snapshot of checkpoint 7 reached the table
        IcebergFilesCommitter.of(config, loader(table))
                .doCommitInOrder(groups.subList(0, 1), 7L, false);
        assertEquals(rows("1=a", "2=b"), read(table));

        IcebergAggregatedCommitter committer = committer(table, config);
        IcebergAggregatedCommitInfo aggregated = committer.combine(infos);
        committer.restoreCommit(Collections.singletonList(aggregated));
        assertEquals(rows("2=b", "3=c"), read(table));
        assertEquals(2, snapshotCount(table));

        // restoring the same checkpoint again changes nothing
        committer.restoreCommit(Collections.singletonList(aggregated));
        assertEquals(2, snapshotCount(table));
        assertEquals(rows("2=b", "3=c"), read(table));
    }

    @Test
    void restoreSkipsCheckpointCommittedAsSingleSnapshot() throws Exception {
        IcebergSinkConfig config = config(2L, true, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        List<WriteResult> results =
                write(table, config, insert(1, "a"), insert(2, "b"), row(RowKind.DELETE, 1, "a"));
        // a snapshot written for checkpoint 7 without group properties covers all of it
        IcebergFilesCommitter.of(config, loader(table)).doCommit(results, 7L);
        long snapshots = snapshotCount(table);

        IcebergAggregatedCommitter committer = committer(table, config);
        committer.restoreCommit(
                Collections.singletonList(
                        committer.combine(
                                Collections.singletonList(new IcebergCommitInfo(results, 7L)))));

        assertEquals(snapshots, snapshotCount(table));
    }

    @Test
    void schemaChangeInsideCheckpointKeepsUpsertCorrect() throws Exception {
        // Applying a schema change completes the task writer as well, so this was affected by the
        // same problem before rotation existed.
        IcebergSinkConfig config = config(null, true, null);
        Table table = SchemaUtils.autoCreateTable(catalog, TABLE, config, idAndName());
        RecordWriter writer = new IcebergWriterFactory(loader(table), config).createWriter(null);
        SeaTunnelRowType before = idAndName().toPhysicalRowDataType();
        writer.write(insert(1, "a"), before);

        SeaTunnelRowType after =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "note"},
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        writer.applySchemaChange(
                after,
                AlterTableAddColumnEvent.add(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                                "test", "db", "events"),
                        PhysicalColumn.of(
                                "note", BasicType.STRING_TYPE, (Long) null, true, null, null)));
        SeaTunnelRow update = new SeaTunnelRow(new Object[] {1L, "a2", "n"});
        update.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(update, after);

        commit(table, config, 1L, writer.complete());
        assertEquals(rows("1=a2"), read(table));
    }

    private void commit(
            Table table, IcebergSinkConfig config, long checkpointId, List<WriteResult> results)
            throws Exception {
        IcebergAggregatedCommitter committer = committer(table, config);
        committer.commit(
                Collections.singletonList(
                        committer.combine(
                                Collections.singletonList(
                                        new IcebergCommitInfo(results, checkpointId)))));
    }

    private static IcebergAggregatedCommitter committer(Table table, IcebergSinkConfig config)
            throws Exception {
        IcebergAggregatedCommitter committer = new IcebergAggregatedCommitter(config, null);
        Field field = IcebergAggregatedCommitter.class.getDeclaredField("filesCommitter");
        field.setAccessible(true);
        field.set(committer, IcebergFilesCommitter.of(config, loader(table)));
        return committer;
    }

    private static List<WriteResult> write(
            Table table, IcebergSinkConfig config, SeaTunnelRow... rows) {
        RecordWriter writer = new IcebergWriterFactory(loader(table), config).createWriter(null);
        SeaTunnelRowType rowType = idAndName().toPhysicalRowDataType();
        for (SeaTunnelRow row : rows) {
            writer.write(row, rowType);
        }
        return writer.complete();
    }

    /** Rows as "id=name", sorted, keeping duplicates. */
    private static List<String> read(Table table) throws IOException {
        table.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                rows.add(record.getField("id") + "=" + record.getField("name"));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> rows(String... rows) {
        List<String> sorted = new ArrayList<>(Arrays.asList(rows));
        Collections.sort(sorted);
        return sorted;
    }

    private static long snapshotCount(Table table) {
        table.refresh();
        Map<Long, Snapshot> snapshots = new TreeMap<>();
        table.snapshots().forEach(snapshot -> snapshots.put(snapshot.snapshotId(), snapshot));
        return snapshots.size();
    }

    private static SeaTunnelRow insert(long id, String name) {
        return row(RowKind.INSERT, id, name);
    }

    private static SeaTunnelRow row(RowKind kind, long id, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(kind);
        return row;
    }

    private static IcebergTableLoader loader(Table table) {
        IcebergTableLoader loader = mock(IcebergTableLoader.class);
        when(loader.loadTable()).thenReturn(table);
        when(loader.getTableIdentifier()).thenReturn(TABLE);
        return loader;
    }

    private static IcebergSinkConfig config(Long flushRowSize, boolean upsert, String partitions) {
        Map<String, Object> options = new HashMap<>();
        if (flushRowSize != null) {
            options.put(IcebergSinkOptions.FLUSH_ROW_SIZE.key(), flushRowSize);
        }
        if (upsert) {
            options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), "id");
            options.put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
        }
        if (partitions != null && !partitions.isEmpty()) {
            options.put(IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(), partitions);
        }
        return sinkConfig(options);
    }

    private static IcebergSinkConfig sinkConfig(Map<String, Object> options) {
        Map<String, Object> config = new HashMap<>(options);
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        return new IcebergSinkConfig(ReadonlyConfig.fromMap(config));
    }

    private static TableSchema idAndName() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                .column(
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, (Long) null, true, null, null))
                .build();
    }
}
