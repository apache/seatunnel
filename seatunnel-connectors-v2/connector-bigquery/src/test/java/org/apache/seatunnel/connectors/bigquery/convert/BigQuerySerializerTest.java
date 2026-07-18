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
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

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
    void testConvertWithSequenceNumber() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, null, null, true, null, null),
                        PhysicalColumn.of(
                                "updated_at", BasicType.LONG_TYPE, null, null, true, null, null));
        CatalogTable table = createCatalogTable(columns);
        BigQuerySerializer serializer = new BigQuerySerializer(table, createConfig("updated_at"));

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice", 1700000000L});
        row.setRowKind(RowKind.INSERT);

        JSONObject result = serializer.convert(row, true);

        assertEquals("UPSERT", result.getString(CHANGE_TYPE));
        assertEquals(1700000000L, result.getLong(SEQUENCE_NUM));
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
}
