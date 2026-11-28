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
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.IndexType;
import org.tikv.common.meta.SchemaState;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType;
import org.tikv.common.types.DataType.EncodeType;
import org.tikv.common.types.DecimalType;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.MySQLType;
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

import static org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tikv.common.codec.TableCodec.decodeObjects;
import static org.tikv.common.codec.TableCodec.encodeRow;

/**
 * Covers reconstructed clustered primary keys through the real snapshot and CDC deserializers.
 * Narrow keys, composite alignment, collation originals and exact row kinds defend data identity.
 */
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
                tableInfo,
                true);

        assertEquals(new BigDecimal("1001"), new BigDecimal(values[0].toString()));
        assertEquals("Alice", values[1]);
    }

    @Test
    void snapshotDeserializerRestoresCompositeMixedTypeHandle() throws Exception {
        TiTableInfo tableInfo = compositeHandleTableInfo();
        byte[] key = compositeRecordKey(tableInfo);
        byte[] value = nonPrimaryKeyRowValue(tableInfo);

        SeaTunnelRowSnapshotRecordDeserializer deserializer =
                new SeaTunnelRowSnapshotRecordDeserializer(
                        tableInfo, compositeCatalogTable(), true);
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
                new SeaTunnelRowStreamingRecordDeserializer(
                        tableInfo, compositeCatalogTable(), true);
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
                new SeaTunnelRowStreamingRecordDeserializer(
                        tableInfo, compositeCatalogTable(), true);
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

    @Test
    void shortDecimalHandlesReachSnapshotAndEveryStreamingRowKind() throws Exception {
        TiColumnInfo id = new TiColumnInfo(1, "id", 0, new DecimalType(9, 0), true);
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiTableInfo table = tableInfo(id, name);
        byte[] key = recordKey(table.getId(), id, new BigDecimal("17"));
        assertTrue(
                key.length - 11 < Long.BYTES, "This regression must use a sub-eight-byte handle");
        byte[] value =
                encodeRow(Collections.singletonList(name), new Object[] {NAME}, false, false);
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        deserializeSeaTunnelDataType("id", "DECIMAL(9,0)"),
                                        9L,
                                        false,
                                        null,
                                        ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""))
                        .primaryKey(PrimaryKey.of("primary", Collections.singletonList("id")))
                        .build();
        CatalogTable catalog =
                CatalogTable.of(
                        TableIdentifier.of("tidb", "test", "people"),
                        schema,
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
        ListCollector collector = new ListCollector();
        new SeaTunnelRowSnapshotRecordDeserializer(table, catalog, true)
                .deserialize(kvPair(key, value), collector);
        SeaTunnelRowStreamingRecordDeserializer streaming =
                new SeaTunnelRowStreamingRecordDeserializer(table, catalog, true);
        streaming.deserialize(
                Cdcpb.Event.Row.newBuilder()
                        .setOpType(Cdcpb.Event.Row.OpType.PUT)
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build(),
                collector);
        streaming.deserialize(
                Cdcpb.Event.Row.newBuilder()
                        .setOpType(Cdcpb.Event.Row.OpType.PUT)
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .setOldValue(ByteString.copyFrom(value))
                        .build(),
                collector);
        streaming.deserialize(
                Cdcpb.Event.Row.newBuilder()
                        .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                        .setKey(ByteString.copyFrom(key))
                        .setOldValue(ByteString.copyFrom(value))
                        .build(),
                collector);
        assertEquals(4, collector.rows.size());
        assertEquals(
                Arrays.asList(RowKind.INSERT, RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE),
                Arrays.asList(
                        collector.rows.get(0).getRowKind(),
                        collector.rows.get(1).getRowKind(),
                        collector.rows.get(2).getRowKind(),
                        collector.rows.get(3).getRowKind()));
        for (SeaTunnelRow row : collector.rows) {
            assertEquals(new BigDecimal("17"), new BigDecimal(row.getField(0).toString()));
            assertEquals(NAME, row.getField(1));
        }
    }

    @Test
    void preservesRowValueStringWhileRestoringOtherCompositeColumns() throws Exception {
        TiTableInfo table = compositeHandleTableInfo();
        Object[] values = {"original collation-sensitive value", null, null, NAME};
        CommonHandleDecoder.restorePrimaryKeyColumns(
                compositeRecordKey(table), values, table, true);
        assertEquals("original collation-sensitive value", values[0]);
        assertEquals(SEQ, values[1]);
        assertEquals(AMOUNT, new BigDecimal(values[2].toString()));
        assertEquals(NAME, values[3]);
    }

    @Test
    void doesNotDecodeNonClusteredKeysOrAcceptMalformedRecordPrefixes() {
        TiColumnInfo id = new TiColumnInfo(1, "id", 0, new DecimalType(9, 0), true);
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiTableInfo table = tableInfo(id, name);
        byte[] hiddenRowId = RowKey.toRowKey(table.getId(), 42).toByteString().toByteArray();
        Object[] values = {null, NAME};
        CommonHandleDecoder.restorePrimaryKeyColumns(hiddenRowId, values, table, false);
        assertNull(values[0]);
        assertEquals(NAME, values[1]);
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CommonHandleDecoder.restorePrimaryKeyColumns(
                                        new byte[] {'i'}, values, table, true));
        assertTrue(error.getMessage().contains(table.getName()));
    }

    @Test
    void keyOnlyCommonHandleDeletePreservesPrimaryKeyAndNullNonKeyImage() throws Exception {
        TiColumnInfo id = new TiColumnInfo(1, "id", 0, new DecimalType(9, 0), true);
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiTableInfo table = tableInfo(id, name);
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        deserializeSeaTunnelDataType("id", "DECIMAL(9,0)"),
                                        9L,
                                        false,
                                        null,
                                        ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""))
                        .primaryKey(PrimaryKey.of("primary", Collections.singletonList("id")))
                        .build();
        CatalogTable catalog =
                CatalogTable.of(
                        TableIdentifier.of("tidb", "test", "people"),
                        schema,
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
        ListCollector collector = new ListCollector();
        new SeaTunnelRowStreamingRecordDeserializer(table, catalog, true)
                .deserialize(
                        Cdcpb.Event.Row.newBuilder()
                                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                                .setKey(
                                        ByteString.copyFrom(
                                                recordKey(table.getId(), id, new BigDecimal("17"))))
                                .build(),
                        collector);
        assertEquals(1, collector.rows.size());
        assertEquals(RowKind.DELETE, collector.rows.get(0).getRowKind());
        assertEquals(
                new BigDecimal("17"), new BigDecimal(collector.rows.get(0).getField(0).toString()));
        assertNull(collector.rows.get(0).getField(1));
    }

    @Test
    void imageLessDeleteStillRestoresBinaryCollatedCharKey() throws Exception {
        // CHAR is space-trimmed by the type itself, so a *_bin comparison key is still the stored
        // value, which is the type distinction TiDB's own restore decision makes.
        TiColumnInfo code = stringPrimaryKeyColumn(MySQLType.TypeString, "utf8mb4_bin");
        TiTableInfo table = stringKeyTableInfo(code, "utf8mb4_bin");
        String value = "bob@example.com";
        byte[] key = stringRecordKey(table, code, value);
        ListCollector collector = new ListCollector();
        new SeaTunnelRowStreamingRecordDeserializer(table, stringKeyCatalogTable(), true)
                .deserialize(
                        Cdcpb.Event.Row.newBuilder()
                                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                                .setKey(ByteString.copyFrom(key))
                                .build(),
                        collector);

        assertEquals(1, collector.rows.size());
        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertEquals(value, row.getField(0));
        assertNull(row.getField(1));
    }

    @Test
    void imageLessDeleteStillRestoresExactBinaryCollatedVarcharKey() throws Exception {
        // The exact binary collation stores the value bytes themselves, trailing spaces included.
        TiColumnInfo code = stringPrimaryKeyColumn(MySQLType.TypeVarchar, "binary");
        TiTableInfo table = stringKeyTableInfo(code, "binary");
        String value = "bob ";
        byte[] key = stringRecordKey(table, code, value);
        ListCollector collector = new ListCollector();
        new SeaTunnelRowStreamingRecordDeserializer(table, stringKeyCatalogTable(), true)
                .deserialize(
                        Cdcpb.Event.Row.newBuilder()
                                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                                .setKey(ByteString.copyFrom(key))
                                .build(),
                        collector);

        assertEquals(1, collector.rows.size());
        SeaTunnelRow row = collector.rows.get(0);
        assertEquals(RowKind.DELETE, row.getRowKind());
        assertEquals(value, row.getField(0));
        assertNull(row.getField(1));
    }

    /**
     * A {@code *_bin} collation is a padding collation for a variable-length string: TiDB encodes
     * the comparison key, whose bin padding implementation drops trailing spaces, so the stored
     * value cannot be recovered from the key. The key below is the server-side encoding of a stored
     * {@code 'bob '}: the padding collator encodes it exactly like {@code 'bob'}, which is the
     * pinned fixture for this boundary.
     */
    @Test
    void imageLessDeleteRejectsPaddingBinaryCollatedVarcharKey() throws Exception {
        TiColumnInfo code = stringPrimaryKeyColumn(MySQLType.TypeVarchar, "utf8mb4_bin");
        TiTableInfo table = stringKeyTableInfo(code, "utf8mb4_bin");
        // Stored value 'bob ' with trailing space; the padding collator kept only 'bob'.
        byte[] key = stringRecordKey(table, code, "bob");
        ListCollector collector = new ListCollector();
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(table, stringKeyCatalogTable(), true);

        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                deserializer.deserialize(
                                        Cdcpb.Event.Row.newBuilder()
                                                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                                                .setKey(ByteString.copyFrom(key))
                                                .build(),
                                        collector));

        assertTrue(error.getMessage().contains("code"), error.getMessage());
        assertTrue(error.getMessage().contains("utf8mb4_bin"), error.getMessage());
        assertTrue(
                collector.rows.isEmpty(),
                "a key that may have lost trailing spaces must not emit a row identity");
    }

    @Test
    void imageLessDeleteFailsFastForNonBinaryCollatedStringKey() throws Exception {
        TiColumnInfo code = stringPrimaryKeyColumn("utf8mb4_general_ci");
        TiTableInfo table = stringKeyTableInfo(code, "utf8mb4_general_ci");
        byte[] key = stringRecordKey(table, code, "bob@example.com");
        ListCollector collector = new ListCollector();
        SeaTunnelRowStreamingRecordDeserializer deserializer =
                new SeaTunnelRowStreamingRecordDeserializer(table, stringKeyCatalogTable(), true);

        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                deserializer.deserialize(
                                        Cdcpb.Event.Row.newBuilder()
                                                .setOpType(Cdcpb.Event.Row.OpType.DELETE)
                                                .setKey(ByteString.copyFrom(key))
                                                .build(),
                                        collector));

        assertTrue(error.getMessage().contains("code"), error.getMessage());
        assertTrue(error.getMessage().contains("utf8mb4_general_ci"), error.getMessage());
        assertTrue(collector.rows.isEmpty());
    }

    @Test
    void stringKeyReversibilityFollowsCollationAndTypeSemantics() {
        // The exact binary collation stores the value bytes.
        assertTrue(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeVarchar, "binary")));
        assertTrue(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeString, "binary")));
        // A *_bin collation only round-trips a fixed-length CHAR, whose value the type trims
        // itself.
        assertTrue(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeString, "utf8mb4_bin")));
        assertTrue(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeString, "utf8_bin")));
        // A padding collation drops trailing spaces from a variable-length string key.
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeVarchar, "utf8mb4_bin")));
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeVarchar, "utf8_bin")));
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeBlob, "utf8mb4_bin")));
        // A weight-based collation never stores the original bytes.
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeVarchar, "utf8mb4_general_ci")));
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(
                        stringType(MySQLType.TypeString, "utf8mb4_general_ci")));
        // Unknown or missing collations are not reversible.
        assertFalse(
                CommonHandleDecoder.isReversibleStringKey(stringType(MySQLType.TypeVarchar, "")));
    }

    private DataType stringType(MySQLType mySQLType, String collation) {
        return new StringType(
                new TiColumnInfo.InternalTypeHolder(
                        mySQLType.getTypeCode(),
                        0,
                        320L,
                        0,
                        "utf8mb4",
                        collation,
                        Collections.<String>emptyList()));
    }

    private TiColumnInfo stringPrimaryKeyColumn(String collation) {
        return stringPrimaryKeyColumn(MySQLType.TypeVarchar, collation);
    }

    private TiColumnInfo stringPrimaryKeyColumn(MySQLType mySQLType, String collation) {
        return new TiColumnInfo(1L, "code", 0, stringType(mySQLType, collation), true);
    }

    private TiTableInfo stringKeyTableInfo(TiColumnInfo primaryKey, String collation) {
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiIndexInfo primaryIndex =
                new TiIndexInfo(
                        1,
                        CIStr.newCIStr("primary"),
                        CIStr.newCIStr("users"),
                        Collections.singletonList(
                                new TiIndexColumn(
                                        CIStr.newCIStr("code"), primaryKey.getOffset(), -1)),
                        true,
                        true,
                        SchemaState.StatePublic.getStateCode(),
                        "",
                        IndexType.IndexTypeBtree.getTypeCode(),
                        false);
        return new TiTableInfo(
                103,
                CIStr.newCIStr("users"),
                "utf8mb4",
                collation,
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

    private CatalogTable stringKeyCatalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "code", BasicType.STRING_TYPE, 64L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""))
                        .primaryKey(PrimaryKey.of("primary", Collections.singletonList("code")))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("tidb", "test", "users"),
                schema,
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private byte[] stringRecordKey(TiTableInfo tableInfo, TiColumnInfo primaryKey, String value)
            throws Exception {
        CodecDataOutput output = new CodecDataOutput();
        output.writeByte('t');
        IntegerCodec.writeLong(output, tableInfo.getId());
        output.writeByte('_');
        output.writeByte('r');
        primaryKey.getType().encode(output, EncodeType.KEY, value);
        return output.toBytes();
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
                                        deserializeSeaTunnelDataType("amount", "DECIMAL(10,2)"),
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

    /**
     * Keeps emitted row images for exact field and row-kind assertions on deserializer entry
     * points. No mock interaction stands in for the resulting CDC data.
     */
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
