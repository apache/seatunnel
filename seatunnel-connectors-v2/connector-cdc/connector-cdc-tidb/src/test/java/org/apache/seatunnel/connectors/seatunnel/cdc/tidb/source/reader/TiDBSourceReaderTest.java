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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer.SeaTunnelRowStreamingRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import org.junit.jupiter.api.Test;
import org.tikv.cdc.CDCClient;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.StringType;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TiDBSourceReaderTest {

    private static final long TABLE_ID = 42L;
    private static final long ROW_HANDLE = 7L;
    private static final long START_TS = 100L;
    private static final long COMMIT_TS = 200L;
    private static final long RESOLVED_TS = 300L;
    private static final String TABLE_KEY = "database.table";

    @Test
    void shouldAdvanceSplitWithTheSlowestRegionResolvedTimestamp() throws Exception {
        TiDBSourceConfig config =
                TiDBSourceConfig.builder().startupMode(StartupMode.LATEST).batchSize(1).build();
        TiDBSourceReader reader =
                new TiDBSourceReader(
                        mock(SourceReader.Context.class),
                        config,
                        Collections.singletonList(mock(CatalogTable.class)));
        TiDBSourceSplit split =
                new TiDBSourceSplit(
                        "database", "table", mock(Coprocessor.KeyRange.class), 10L, null, true);
        CDCClient cdcClient = mock(CDCClient.class);
        when(cdcClient.get()).thenReturn(null);
        when(cdcClient.getMinResolvedTs()).thenReturn(100L);
        when(cdcClient.getMaxResolvedTs()).thenReturn(200L);
        cdcClients(reader).put(split, cdcClient);

        reader.captureStreamingEvents(split, mock(Collector.class));

        assertEquals(100L, split.getResolvedTs());
    }

    @Test
    void flushRowsShouldHoldCommitUntilMatchingPrewriteArrives() throws Exception {
        TiDBSourceReader reader = new TiDBSourceReader(null, null, Collections.emptyList());
        Cdcpb.Event.Row commit = row(Cdcpb.Event.LogType.COMMIT, START_TS, COMMIT_TS);

        assertTrue(handleRow(reader, commit, TABLE_KEY));

        long safeResolvedTs = flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS, TABLE_KEY);

        assertEquals(COMMIT_TS - 1, safeResolvedTs);
        assertEquals(1, commits(reader, TABLE_KEY).size());
        assertTrue(committedEvents(reader, TABLE_KEY).isEmpty());

        Cdcpb.Event.Row prewrite = row(Cdcpb.Event.LogType.PREWRITE, START_TS, 0L);

        assertTrue(handleRow(reader, prewrite, TABLE_KEY));

        safeResolvedTs = flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS, TABLE_KEY);

        assertEquals(RESOLVED_TS, safeResolvedTs);
        assertTrue(commits(reader, TABLE_KEY).isEmpty());
        assertTrue(preWrites(reader, TABLE_KEY).isEmpty());
        BlockingQueue<Cdcpb.Event.Row> committedEvents = committedEvents(reader, TABLE_KEY);
        assertFalse(committedEvents.isEmpty());
        assertSame(prewrite, committedEvents.poll());
        assertTrue(committedEvents.isEmpty());
    }

    @Test
    void transactionBuffersShouldBeIsolatedPerTable() throws Exception {
        TiDBSourceReader reader = new TiDBSourceReader(null, null, Collections.emptyList());
        Cdcpb.Event.Row prewriteOne =
                row(Cdcpb.Event.LogType.PREWRITE, TABLE_ID, ROW_HANDLE, START_TS, 0L);
        Cdcpb.Event.Row commitOne =
                row(Cdcpb.Event.LogType.COMMIT, TABLE_ID, ROW_HANDLE, START_TS, COMMIT_TS);
        Cdcpb.Event.Row prewriteTwo =
                row(Cdcpb.Event.LogType.PREWRITE, TABLE_ID + 1, ROW_HANDLE + 1, START_TS + 1, 0L);
        Cdcpb.Event.Row commitTwo =
                row(
                        Cdcpb.Event.LogType.COMMIT,
                        TABLE_ID + 1,
                        ROW_HANDLE + 1,
                        START_TS + 1,
                        COMMIT_TS + 1);

        assertTrue(handleRow(reader, prewriteOne, "db.table_one"));
        assertTrue(handleRow(reader, commitOne, "db.table_one"));
        assertTrue(handleRow(reader, prewriteTwo, "db.table_two"));
        assertTrue(handleRow(reader, commitTwo, "db.table_two"));

        assertEquals(
                RESOLVED_TS, flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS, "db.table_two"));
        assertEquals(
                RESOLVED_TS, flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS, "db.table_one"));

        assertSame(prewriteOne, committedEvents(reader, "db.table_one").poll());
        assertTrue(committedEvents(reader, "db.table_one").isEmpty());
        assertSame(prewriteTwo, committedEvents(reader, "db.table_two").poll());
        assertTrue(committedEvents(reader, "db.table_two").isEmpty());
    }

    @Test
    void addSplitsShouldDropSplitsOfTablesRemovedFromConfig() throws Exception {
        TiDBSourceReader reader =
                new TiDBSourceReader(
                        mock(SourceReader.Context.class),
                        TiDBSourceConfig.builder()
                                .startupMode(StartupMode.LATEST)
                                .batchSize(1)
                                .build(),
                        Collections.singletonList(catalogTable()));
        TiDBSourceSplit keptSplit =
                new TiDBSourceSplit(
                        "db", "table_one", mock(Coprocessor.KeyRange.class), 0L, null, true);
        TiDBSourceSplit droppedSplit =
                new TiDBSourceSplit(
                        "db", "table_two", mock(Coprocessor.KeyRange.class), 0L, null, true);

        reader.addSplits(Arrays.asList(keptSplit, droppedSplit));

        List<TiDBSourceSplit> sourceSplits = sourceSplits(reader);
        assertEquals(1, sourceSplits.size());
        assertSame(keptSplit, sourceSplits.get(0));
    }

    @Test
    void streamingDeserializerShouldRouteBySplitTableAndFailFastForUnknownTable() throws Exception {
        TiDBSourceReader reader = new TiDBSourceReader(null, null, Collections.emptyList());
        SeaTunnelRowStreamingRecordDeserializer tableOneDeserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo(), catalogTable());
        streamingDeserializers(reader).put("db.table_one", tableOneDeserializer);

        Method requireStreamingDeserializer =
                TiDBSourceReader.class.getDeclaredMethod(
                        "requireStreamingDeserializer", TiDBSourceSplit.class);
        requireStreamingDeserializer.setAccessible(true);

        TiDBSourceSplit splitOne =
                new TiDBSourceSplit(
                        "db", "table_one", mock(Coprocessor.KeyRange.class), 0L, null, true);
        assertSame(tableOneDeserializer, requireStreamingDeserializer.invoke(reader, splitOne));

        TiDBSourceSplit splitTwo =
                new TiDBSourceSplit(
                        "db", "table_two", mock(Coprocessor.KeyRange.class), 0L, null, true);
        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> requireStreamingDeserializer.invoke(reader, splitTwo));
        assertTrue(exception.getCause() instanceof IllegalStateException);
        assertTrue(exception.getCause().getMessage().contains("not configured"));
    }

    private static Cdcpb.Event.Row row(Cdcpb.Event.LogType type, long startTs, long commitTs) {
        return row(type, TABLE_ID, ROW_HANDLE, startTs, commitTs);
    }

    private static Cdcpb.Event.Row row(
            Cdcpb.Event.LogType type, long tableId, long rowHandle, long startTs, long commitTs) {
        return Cdcpb.Event.Row.newBuilder()
                .setType(type)
                .setStartTs(startTs)
                .setCommitTs(commitTs)
                .setKey(RowKey.toRowKey(tableId, rowHandle).toByteString())
                .build();
    }

    private static TiTableInfo tableInfo() {
        List<TiColumnInfo> columns =
                Arrays.asList(
                        new TiColumnInfo(1L, "id", 0, IntegerType.BIGINT, true),
                        new TiColumnInfo(2L, "name", 1, StringType.VARCHAR, false));
        return new TiTableInfo(
                TABLE_ID,
                CIStr.newCIStr("test_table"),
                "utf8mb4",
                "utf8mb4_bin",
                true,
                columns,
                Collections.emptyList(),
                "",
                0L,
                2L,
                0L,
                0L,
                null,
                null,
                null,
                0L,
                0L,
                0L,
                null);
    }

    private static CatalogTable catalogTable() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("test_catalog", "db", "table_one"),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static boolean handleRow(TiDBSourceReader reader, Cdcpb.Event.Row row, String tableKey)
            throws Exception {
        Method handleRow =
                TiDBSourceReader.class.getDeclaredMethod(
                        "handleRow", Cdcpb.Event.Row.class, String.class);
        handleRow.setAccessible(true);
        return (Boolean) handleRow.invoke(reader, row, tableKey);
    }

    private static long flushRowsAndGetSafeResolvedTs(
            TiDBSourceReader reader, long resolvedTs, String tableKey) throws Exception {
        Method flushRowsAndGetSafeResolvedTs =
                TiDBSourceReader.class.getDeclaredMethod(
                        "flushRowsAndGetSafeResolvedTs", long.class, String.class);
        flushRowsAndGetSafeResolvedTs.setAccessible(true);
        return (Long) flushRowsAndGetSafeResolvedTs.invoke(reader, resolvedTs, tableKey);
    }

    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits(
            TiDBSourceReader reader, String tableKey) throws Exception {
        return rowBuffer(reader, "commits", tableKey);
    }

    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> preWrites(
            TiDBSourceReader reader, String tableKey) throws Exception {
        return rowBuffer(reader, "preWrites", tableKey);
    }

    @SuppressWarnings("unchecked")
    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> rowBuffer(
            TiDBSourceReader reader, String fieldName, String tableKey) throws Exception {
        Field field = TiDBSourceReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        Map<String, TreeMap<RowKeyWithTs, Cdcpb.Event.Row>> buffers =
                (Map<String, TreeMap<RowKeyWithTs, Cdcpb.Event.Row>>) field.get(reader);
        return buffers.computeIfAbsent(tableKey, ignored -> new TreeMap<>());
    }

    private static BlockingQueue<Cdcpb.Event.Row> committedEvents(
            TiDBSourceReader reader, String tableKey) throws Exception {
        Field field = TiDBSourceReader.class.getDeclaredField("committedEvents");
        field.setAccessible(true);
        Map<String, BlockingQueue<Cdcpb.Event.Row>> queues =
                (Map<String, BlockingQueue<Cdcpb.Event.Row>>) field.get(reader);
        return queues.computeIfAbsent(tableKey, ignored -> new LinkedBlockingQueue<>());
    }

    @SuppressWarnings("unchecked")
    private static List<TiDBSourceSplit> sourceSplits(TiDBSourceReader reader)
            throws ReflectiveOperationException {
        Field field = TiDBSourceReader.class.getDeclaredField("sourceSplits");
        field.setAccessible(true);
        return (List<TiDBSourceSplit>) field.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static Map<TiDBSourceSplit, CDCClient> cdcClients(TiDBSourceReader reader)
            throws ReflectiveOperationException {
        Field cacheField = TiDBSourceReader.class.getDeclaredField("cacheCDCClient");
        cacheField.setAccessible(true);
        return (Map<TiDBSourceSplit, CDCClient>) cacheField.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, SeaTunnelRowStreamingRecordDeserializer> streamingDeserializers(
            TiDBSourceReader reader) throws ReflectiveOperationException {
        Field field = TiDBSourceReader.class.getDeclaredField("streamingDeserializers");
        field.setAccessible(true);
        return (Map<String, SeaTunnelRowStreamingRecordDeserializer>) field.get(reader);
    }
}
