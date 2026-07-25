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

package org.apache.seatunnel.connectors.bigquery.convert;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.CHANGE_TYPE;
import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.SEQUENCE_NUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQuerySerializerTest {

    private CatalogTable createCatalogTable(List<Column> columns) {
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        return CatalogTable.of(
                TableIdentifier.of("test", "test", "test_table"),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private ReadonlyConfig createConfig(String sequenceNumberColumn) {
        Map<String, Object> map = new HashMap<>();
        if (sequenceNumberColumn != null) {
            map.put(BigQuerySinkOptions.SEQUENCE_NUMBER_COLUMN.key(), sequenceNumberColumn);
        }
        return ReadonlyConfig.fromMap(map);
    }

    @Test
    void testConvertInsertRowToUpsert() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice"});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, true);

        assertEquals("UPSERT", result.getString(CHANGE_TYPE));
        assertEquals(1L, result.getLong("id"));
        assertEquals("Alice", result.getString("name"));
    }

    @Test
    void testConvertUpdateAfterRowToUpsert() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {2L, "Bob"});
        row.setRowKind(RowKind.UPDATE_AFTER);

        JSONObject result = serializer.convert(row, true);

        assertEquals("UPSERT", result.getString(CHANGE_TYPE));
    }

    @Test
    void testConvertDeleteRowToDelete() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {3L, "Charlie"});
        row.setRowKind(RowKind.DELETE);

        JSONObject result = serializer.convert(row, true);

        assertEquals("DELETE", result.getString(CHANGE_TYPE));
    }

    @Test
    void testConvertUpdateBeforeRowToDelete() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {4L, "Dave"});
        row.setRowKind(RowKind.UPDATE_BEFORE);

        JSONObject result = serializer.convert(row, true);

        assertEquals("DELETE", result.getString(CHANGE_TYPE));
    }

    @Test
    void testConvertWithoutChangeType() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig("id"));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice"});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, false);

        assertFalse(result.has(CHANGE_TYPE));
        assertFalse(result.has(SEQUENCE_NUM));
        assertEquals(1L, result.getLong("id"));
    }

    @Test
    void testConvertLongSequenceNumberToHex() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "updated_at", BasicType.LONG_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig("updated_at"));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice", 255L});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, true);

        assertEquals("UPSERT", result.getString(CHANGE_TYPE));
        assertEquals("FF", result.getString(SEQUENCE_NUM));
    }

    @Test
    void testConvertIntSequenceNumberToHex() {
        assertEquals("FF", convertSequenceNumber(255, BasicType.INT_TYPE));
    }

    @Test
    void testConvertIntegralBigDecimalSequenceNumberToHex() {
        DecimalType unsignedBigIntType = new DecimalType(20, 0);

        assertEquals("1", convertSequenceNumber(new BigDecimal("1"), unsignedBigIntType));
        assertEquals(
                "8000000000000000",
                convertSequenceNumber(new BigDecimal("9223372036854775808"), unsignedBigIntType));
        assertEquals(
                "FFFFFFFFFFFFFFFF",
                convertSequenceNumber(new BigDecimal("18446744073709551615"), unsignedBigIntType));
    }

    @Test
    void testPreserveEncodedStringSequenceNumber() {
        assertEquals("123", convertSequenceNumber("123", BasicType.STRING_TYPE));
    }

    @Test
    void testAllowMultiPartSequenceNumber() {
        assertEquals("FFF/ABC", convertSequenceNumber("FFF/ABC", BasicType.STRING_TYPE));
    }

    @Test
    void testRejectNullSequenceNumber() {
        assertInvalidSequenceNumber(null, BasicType.LONG_TYPE, "must not be null");
    }

    @Test
    void testRejectNegativeSequenceNumber() {
        assertInvalidSequenceNumber(-1L, BasicType.LONG_TYPE, "must not be negative");
    }

    @Test
    void testRejectNegativeBigDecimalSequenceNumber() {
        assertInvalidSequenceNumber(
                new BigDecimal("-1"), new DecimalType(20, 0), "must not be negative");
    }

    @Test
    void testRejectFractionalBigDecimalSequenceNumber() {
        assertInvalidSequenceNumber(
                new BigDecimal("1.5"),
                new DecimalType(20, 1),
                "must not contain a fractional value");
    }

    @Test
    void testRejectBigDecimalSequenceNumberAboveUnsigned64BitRange() {
        assertInvalidSequenceNumber(
                new BigDecimal("18446744073709551616"),
                new DecimalType(20, 0),
                "must not exceed the unsigned 64-bit range");
    }

    @Test
    void testRejectEmptySequenceNumber() {
        assertInvalidSequenceNumber("", BasicType.STRING_TYPE, "must not be empty");
    }

    @Test
    void testRejectInvalidHexSequenceNumber() {
        assertInvalidSequenceNumber(
                "FFF/XYZ", BasicType.STRING_TYPE, "only hexadecimal characters");
    }

    @Test
    void testRejectSequenceNumberSectionLongerThan16Characters() {
        assertInvalidSequenceNumber(
                "1234567890ABCDEF0", BasicType.STRING_TYPE, "at most 16 characters");
    }

    @Test
    void testRejectSequenceNumberWithMoreThan4Sections() {
        assertInvalidSequenceNumber("1/2/3/4/5", BasicType.STRING_TYPE, "at most 4 sections");
    }

    @Test
    void testRejectEmptySequenceNumberSection() {
        assertInvalidSequenceNumber("1//2", BasicType.STRING_TYPE, "empty section");
    }

    @Test
    void testConvertWithoutSequenceNumberConfig() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice"});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, true);

        assertEquals("UPSERT", result.getString(CHANGE_TYPE));
        assertFalse(result.has(SEQUENCE_NUM));
    }

    @Test
    void testConvertBytesField() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "data",
                                PrimitiveByteArrayType.INSTANCE,
                                null,
                                null,
                                true,
                                null,
                                null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        byte[] originalBytes = "hello".getBytes();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, originalBytes});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, false);

        assertTrue(result.get("data").toString().contains("ByteString"));
        assertTrue(result.has("data"));
    }

    @Test
    void testConvertNullBytesField() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "data",
                                PrimitiveByteArrayType.INSTANCE,
                                null,
                                null,
                                true,
                                null,
                                null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig(null));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, null});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, false);

        assertTrue(result.has("id"));
        assertEquals("null", result.get("data").toString());
    }

    @Test
    void testSequenceColumnNotFoundInSchema() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        assertThrows(
                SeaTunnelRuntimeException.class,
                () -> new BigQuerySerializer(table, createConfig("non_existent_col")));
    }

    private String convertSequenceNumber(Object sequenceNumber, SeaTunnelDataType<?> dataType) {
        BigQuerySerializer serializer = createSequenceSerializer(dataType);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, sequenceNumber});
        row.setRowKind(RowKind.INSERT);
        return serializer.convert(row, true).getString(SEQUENCE_NUM);
    }

    private void assertInvalidSequenceNumber(
            Object sequenceNumber, SeaTunnelDataType<?> dataType, String expectedMessage) {
        SeaTunnelRuntimeException exception =
                assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> convertSequenceNumber(sequenceNumber, dataType));
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    private BigQuerySerializer createSequenceSerializer(SeaTunnelDataType<?> dataType) {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of("sequence", dataType, null, null, true, null, null));
        return new BigQuerySerializer(createCatalogTable(columns), createConfig("sequence"));
    }
}
