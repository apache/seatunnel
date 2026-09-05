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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.IndexType;
import org.tikv.common.meta.SchemaState;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType.EncodeType;
import org.tikv.common.types.DecimalType;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.StringType;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;
import static org.tikv.common.codec.TableCodec.encodeRow;

class CommonHandleDecoderTest {

    private static final String REGION = "east";
    private static final long SEQ = 42L;
    private static final BigDecimal AMOUNT = new BigDecimal("19.95");
    private static final String NAME = "Alice";

    @Test
    void restoresDecimalPrimaryKeyFromCommonHandleRecordKey() throws Exception {
        TiColumnInfo primaryKey = new TiColumnInfo(1, "id", 0, new DecimalType(10, 0), true);
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiTableInfo tableInfo = tableInfo(primaryKey, name);
        Object[] values =
                decodeObjects(
                        encodeRow(
                                Collections.singletonList(name),
                                new Object[] {"Alice"},
                                false,
                                false),
                        null,
                        tableInfo);

        assertNull(values[0]);
        assertEquals("Alice", values[1]);

        CommonHandleDecoder.restorePrimaryKeyColumns(
                recordKey(tableInfo.getId(), primaryKey, new BigDecimal("1001")),
                values,
                tableInfo);

        assertEquals(new BigDecimal("1001"), new BigDecimal(values[0].toString()));
        assertEquals("Alice", values[1]);
    }

    @Test
    void snapshotDeserializerRestoresCompositeMixedTypeHandle() throws Exception {
        TiTableInfo tableInfo = compositeHandleTableInfo();
        byte[] key = compositeRecordKey(tableInfo);
        byte[] value = nonPrimaryKeyRowValue(tableInfo);

        SeaTunnelRowSnapshotRecordDeserializer deserializer =
                new SeaTunnelRowSnapshotRecordDeserializer(tableInfo, compositeCatalogTable());
        ListCollector collector = new ListCollector();
        deserializer.deserialize(kvPair(key, value), collector);

        assertEquals(1, collector.rows.size());
        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertCompositeHandleFields(row);
    }

    @Test
    void streamingPutRestoresCompositeMixedTypeHandle() throws Exception {
        TiTableInfo tableInfo = compositeHandleTableInfo();
        byte[] key = compositeRecordKey(tableInfo);
        byte[] value = nonPrimaryKeyRowValue(tableInfo);

        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo, compositeCatalogTable());
        ListCollector collector = new ListCollector();
        Cdcpb.Event.Row put =
                Cdcpb.Event.Row.newBuilder()
                        .setOpType(Cdcpb.Event.Row.OpType.PUT)
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .setOldValue(ByteString.EMPTY)
                        .build();
        deserializer.deserialize(put, collector);

        assertEquals(1, collector.rows.size());
        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.INSERT, row.getRowKind());
        assertCompositeHandleFields(row);
    }

    @Test
    void streamingDeleteRestoresCompositeMixedTypeHandle() throws Exception {
        TiTableInfo tableInfo = compositeHandleTableInfo();
        byte[] key = compositeRecordKey(tableInfo);
        byte[] value = nonPrimaryKeyRowValue(tableInfo);

        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo, compositeCatalogTable());
        ListCollector collector = new ListCollector();
        Cdcpb.Event.Row delete =
                Cdcpb.Event.Row.newBuilder()
                        .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                        .setKey(ByteString.copyFrom(key))
                        .setOldValue(ByteString.copyFrom(value))
                        .build();
        deserializer.deserialize(delete, collector);

        assertEquals(1, collector.rows.size());
        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertCompositeHandleFields(row);
    }

    private void assertCompositeHandleFields(SeaTunnelRow row) {
        assertEquals(REGION, row.getField(0));
        assertEquals(SEQ, row.getField(1));
        assertEquals(0, AMOUNT.compareTo((BigDecimal) row.getField(2)));
        assertEquals(NAME, row.getField(3));
    }

    private Kvrpcpb.KvPair kvPair(byte[] key, byte[] value) {
        return Kvrpcpb.KvPair.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
    }

    private byte[] nonPrimaryKeyRowValue(TiTableInfo tableInfo) throws Exception {
        TiColumnInfo name = tableInfo.getColumn("name");
        return encodeRow(Collections.singletonList(name), new Object[] {NAME}, false, false);
    }

    private byte[] compositeRecordKey(TiTableInfo tableInfo) throws Exception {
        CodecDataOutput output = new CodecDataOutput();
        output.writeByte('t');
        IntegerCodec.writeLong(output, tableInfo.getId());
        output.writeByte('_');
        output.writeByte('r');
        tableInfo.getColumn("region").getType().encode(output, EncodeType.KEY, REGION);
        tableInfo.getColumn("seq").getType().encode(output, EncodeType.KEY, SEQ);
        tableInfo.getColumn("amount").getType().encode(output, EncodeType.KEY, AMOUNT);
        return output.toBytes();
    }

    private CatalogTable compositeCatalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "region", BasicType.STRING_TYPE, 64L, false, null, ""))
                        .column(PhysicalColumn.of("seq", BasicType.LONG_TYPE, 20L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "amount",
                                        new org.apache.seatunnel.api.table.type.DecimalType(10, 2),
                                        10L,
                                        false,
                                        null,
                                        ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""))
                        .primaryKey(
                                PrimaryKey.of("primary", Arrays.asList("region", "seq", "amount")))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("tidb", "test", "orders"),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private TiTableInfo compositeHandleTableInfo() {
        TiColumnInfo region = new TiColumnInfo(1, "region", 0, StringType.VARCHAR, true);
        TiColumnInfo seq = new TiColumnInfo(2, "seq", 1, IntegerType.BIGINT, true);
        TiColumnInfo amount = new TiColumnInfo(3, "amount", 2, new DecimalType(10, 2), true);
        TiColumnInfo name = new TiColumnInfo(4, "name", 3, StringType.VARCHAR, false);
        TiIndexInfo primaryIndex =
                new TiIndexInfo(
                        1,
                        CIStr.newCIStr("primary"),
                        CIStr.newCIStr("orders"),
                        Arrays.asList(
                                new TiIndexColumn(CIStr.newCIStr("region"), region.getOffset(), -1),
                                new TiIndexColumn(CIStr.newCIStr("seq"), seq.getOffset(), -1),
                                new TiIndexColumn(
                                        CIStr.newCIStr("amount"), amount.getOffset(), -1)),
                        true,
                        true,
                        SchemaState.StatePublic.getStateCode(),
                        "",
                        IndexType.IndexTypeBtree.getTypeCode(),
                        false);
        return new TiTableInfo(
                102,
                CIStr.newCIStr("orders"),
                "utf8mb4",
                "utf8mb4_bin",
                false,
                Arrays.asList(region, seq, amount, name),
                Collections.singletonList(primaryIndex),
                "",
                0,
                4,
                1,
                0,
                null,
                null,
                null,
                0,
                0,
                0,
                null);
    }

    private TiTableInfo tableInfo(TiColumnInfo primaryKey, TiColumnInfo name) {
        TiIndexInfo primaryIndex =
                new TiIndexInfo(
                        1,
                        CIStr.newCIStr("primary"),
                        CIStr.newCIStr("people"),
                        Collections.singletonList(
                                new TiIndexColumn(
                                        CIStr.newCIStr("id"), primaryKey.getOffset(), -1)),
                        true,
                        true,
                        SchemaState.StatePublic.getStateCode(),
                        "",
                        IndexType.IndexTypeBtree.getTypeCode(),
                        false);
        return new TiTableInfo(
                101,
                CIStr.newCIStr("people"),
                "utf8mb4",
                "utf8mb4_bin",
                false,
                Arrays.asList(primaryKey, name),
                Collections.singletonList(primaryIndex),
                "",
                0,
                2,
                1,
                0,
                null,
                null,
                null,
                0,
                0,
                0,
                null);
    }

    private byte[] recordKey(long tableId, TiColumnInfo primaryKey, BigDecimal primaryKeyValue) {
        CodecDataOutput output = new CodecDataOutput();
        output.writeByte('t');
        IntegerCodec.writeLong(output, tableId);
        output.writeByte('_');
        output.writeByte('r');
        primaryKey.getType().encode(output, EncodeType.KEY, primaryKeyValue);
        return output.toBytes();
    }

    private static final class ListCollector implements Collector<SeaTunnelRow> {
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
