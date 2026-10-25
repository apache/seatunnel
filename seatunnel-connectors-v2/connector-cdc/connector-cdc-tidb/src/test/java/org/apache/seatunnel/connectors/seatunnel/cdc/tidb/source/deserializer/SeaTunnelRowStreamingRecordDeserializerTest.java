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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.tikv.common.codec.TableCodec;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.StringType;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeaTunnelRowStreamingRecordDeserializerTest {

    private static final long TABLE_ID = 42L;
    private static final long HANDLE = 7L;

    @Test
    void deserializeDeleteShouldUseValueWhenOldValueIsEmpty() throws Exception {
        TiTableInfo tableInfo = tableInfo(true);
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo, catalogTable());
        byte[] encodedRow =
                TableCodec.encodeRow(
                        tableInfo.getColumns(), new Object[] {HANDLE, "Alice"}, true, false);
        TestCollector collector = new TestCollector();

        deserializer.deserialize(deleteRow(ByteString.copyFrom(encodedRow)), collector);

        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertEquals("test_db.test_table", row.getTableId());
        assertEquals(HANDLE, row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    @Test
    void deserializeDeleteShouldUseHandleWhenValuesAreEmptyForPkHandleTable() throws Exception {
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo(true), catalogTable());
        TestCollector collector = new TestCollector();

        deserializer.deserialize(deleteRow(ByteString.EMPTY), collector);

        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertEquals("test_db.test_table", row.getTableId());
        assertEquals(HANDLE, row.getField(0));
        assertNull(row.getField(1));
    }

    @Test
    void deserializePutShouldSetTableId() throws Exception {
        TiTableInfo tableInfo = tableInfo(true);
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo, catalogTable());
        byte[] encodedRow =
                TableCodec.encodeRow(
                        tableInfo.getColumns(), new Object[] {HANDLE, "Alice"}, true, false);
        TestCollector collector = new TestCollector();

        deserializer.deserialize(putRow(ByteString.copyFrom(encodedRow)), collector);

        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertEquals("test_db.test_table", row.getTableId());
        assertEquals(HANDLE, row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    @Test
    void deserializeSnapshotShouldSetTableId() throws Exception {
        TiTableInfo tableInfo = tableInfo(true);
        SeaTunnelRowSnapshotRecordDeserializer deserializer =
                new SeaTunnelRowSnapshotRecordDeserializer(tableInfo, catalogTable());
        byte[] encodedRow =
                TableCodec.encodeRow(
                        tableInfo.getColumns(), new Object[] {HANDLE, "Alice"}, true, false);
        TestCollector collector = new TestCollector();

        deserializer.deserialize(
                Kvrpcpb.KvPair.newBuilder()
                        .setKey(RowKey.toRowKey(TABLE_ID, HANDLE).toByteString())
                        .setValue(ByteString.copyFrom(encodedRow))
                        .build(),
                collector);

        SeaTunnelRow row = collector.rows.get(0);
        assertEquals("test_db.test_table", row.getTableId());
        assertEquals(HANDLE, row.getField(0));
        assertEquals("Alice", row.getField(1));
    }

    @Test
    void deserializeDeleteShouldFailClearlyWhenValuesAreEmptyForNonPkHandleTable() {
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo(false), catalogTable());
        TestCollector collector = new TestCollector();

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> deserializer.deserialize(deleteRow(ByteString.EMPTY), collector));

        assertTrue(exception.getMessage().contains("both value and oldValue are empty"));
    }

    private static Cdcpb.Event.Row deleteRow(ByteString value) {
        return Cdcpb.Event.Row.newBuilder()
                .setType(Cdcpb.Event.LogType.PREWRITE)
                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                .setKey(RowKey.toRowKey(TABLE_ID, HANDLE).toByteString())
                .setValue(value)
                .build();
    }

    private static Cdcpb.Event.Row putRow(ByteString value) {
        return Cdcpb.Event.Row.newBuilder()
                .setType(Cdcpb.Event.LogType.PREWRITE)
                .setOpType(Cdcpb.Event.Row.OpType.PUT)
                .setKey(RowKey.toRowKey(TABLE_ID, HANDLE).toByteString())
                .setValue(value)
                .build();
    }

    private static TiTableInfo tableInfo(boolean pkIsHandle) {
        List<TiColumnInfo> columns =
                Arrays.asList(
                        new TiColumnInfo(1L, "id", 0, IntegerType.BIGINT, true),
                        new TiColumnInfo(2L, "name", 1, StringType.VARCHAR, false));
        return new TiTableInfo(
                TABLE_ID,
                CIStr.newCIStr("test_table"),
                "utf8mb4",
                "utf8mb4_bin",
                pkIsHandle,
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
                TableIdentifier.of("test_catalog", "test_db", "test_table"),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
