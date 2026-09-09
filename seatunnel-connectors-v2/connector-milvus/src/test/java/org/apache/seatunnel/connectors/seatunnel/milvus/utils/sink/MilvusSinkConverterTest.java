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

package org.apache.seatunnel.connectors.seatunnel.milvus.utils.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.FieldType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MilvusSinkConverterTest {

    @Test
    void returnsReconvertedTypeWhenSinkTypeNotNull() {
        Column column = columnWithSinkType("col1", "Int64");

        FieldType result = MilvusSinkConverter.convertToFieldType(column, null, null, null, false);

        assertEquals(DataType.Int64, result.getDataType());
    }

    @Test
    void returnsReconvertedTypeWhenSinkTypeIsNull() {
        Column column = PhysicalColumn.of("col1", BasicType.SHORT_TYPE, 0L, true, null, "");
        FieldType result = MilvusSinkConverter.convertToFieldType(column, null, null, null, false);

        assertEquals(DataType.Int16, result.getDataType());
    }

    @Test
    void returnsReconvertedTypeWhenTypesNotNull() {
        Column column = columnWithSinkType("col1", "Int64");
        FieldType result = MilvusSinkConverter.convertToFieldType(column, null, null, null, false);

        assertEquals(DataType.Int64, result.getDataType());
    }

    @Test
    void convertsNullableColumnToNullableMilvusField() {
        Column column =
                PhysicalColumn.of("nullable_col", BasicType.STRING_TYPE, 0L, true, null, "");

        FieldType result = MilvusSinkConverter.convertToFieldType(column, null, null, null, true);

        assertTrue(result.isNullable());
    }

    @Test
    void keepsNotNullableColumnNotNullableWhenNullableFieldEnabled() {
        Column column =
                PhysicalColumn.of("nullable_col", BasicType.STRING_TYPE, 0L, false, null, "");

        FieldType result = MilvusSinkConverter.convertToFieldType(column, null, null, null, true);

        assertFalse(result.isNullable());
    }

    @Test
    void keepsPrimaryKeyAndVectorFieldNotNullableWhenNullableFieldEnabled() {
        Column primaryKeyColumn = PhysicalColumn.of("id", BasicType.LONG_TYPE, 0L, false, null, "");
        FieldType primaryKey =
                MilvusSinkConverter.convertToFieldType(
                        primaryKeyColumn,
                        PrimaryKey.of("id", Collections.singletonList("id")),
                        null,
                        false,
                        true);

        Column vectorColumn =
                PhysicalColumn.of("vector", VectorType.VECTOR_FLOAT_TYPE, 0L, 4, false, null, "");
        FieldType vector =
                MilvusSinkConverter.convertToFieldType(vectorColumn, null, null, null, true);

        assertEquals(false, primaryKey.isNullable());
        assertEquals(false, vector.isNullable());
    }

    @Test
    void keepsPartitionKeyNotNullableWhenNullableFieldEnabled() {
        Column partitionKeyColumn =
                PhysicalColumn.of("partition_col", BasicType.STRING_TYPE, 0L, true, null, "");

        FieldType partitionKey =
                MilvusSinkConverter.convertToFieldType(
                        partitionKeyColumn, null, "partition_col", null, true);

        assertFalse(partitionKey.isNullable());
        assertTrue(partitionKey.isPartitionKey());
    }

    @Test
    void throwsWhenFieldValueIsNullByDefault() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusSinkConverter()
                                        .buildMilvusData(
                                                catalogTable(),
                                                ReadonlyConfig.fromMap(Collections.emptyMap()),
                                                Collections.emptyList(),
                                                null,
                                                row));

        assertEquals("MILVUS-10", exception.getSeaTunnelErrorCode().getCode());
    }

    @Test
    void keepsNullFieldValueInMilvusDataWhenNullableFieldEnabled() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});

        assertEquals(
                JsonNull.INSTANCE,
                new MilvusSinkConverter()
                        .buildMilvusData(
                                catalogTable(),
                                nullableFieldConfig(),
                                Collections.emptyList(),
                                null,
                                row)
                        .get("nullable_col"));
    }

    @Test
    void throwsWhenNotNullableFieldValueIsNullAndNullableFieldEnabled() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusSinkConverter()
                                        .buildMilvusData(
                                                catalogTable(false),
                                                nullableFieldConfig(),
                                                Collections.emptyList(),
                                                null,
                                                row));

        assertEquals("MILVUS-10", exception.getSeaTunnelErrorCode().getCode());
    }

    @Test
    void throwsWhenPartitionKeyValueIsNullAndNullableFieldEnabled() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusSinkConverter()
                                        .buildMilvusData(
                                                catalogTable(),
                                                nullablePartitionKeyConfig(),
                                                Collections.emptyList(),
                                                null,
                                                row));

        assertEquals("MILVUS-10", exception.getSeaTunnelErrorCode().getCode());
    }

    @Test
    void throwsWhenDynamicFieldValueIsNullByDefault() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {null});

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusSinkConverter()
                                        .buildMilvusData(
                                                dynamicCatalogTable(),
                                                ReadonlyConfig.fromMap(Collections.emptyMap()),
                                                Collections.emptyList(),
                                                "dynamic_col",
                                                row));

        assertEquals("MILVUS-10", exception.getSeaTunnelErrorCode().getCode());
    }

    @Test
    void convertsJsonFieldWithObjectRootToJsonObject() {
        Object converted =
                new MilvusSinkConverter()
                        .convertBySeaTunnelType(
                                BasicType.STRING_TYPE, true, "{\"a\":1,\"b\":\"x\"}");

        assertTrue(converted instanceof JsonObject);
        JsonObject object = (JsonObject) converted;
        assertEquals(1, object.get("a").getAsInt());
        assertEquals("x", object.get("b").getAsString());
    }

    @Test
    void convertsJsonFieldWithPrimitiveRootWithoutFailing() {
        // Issue #9677: a Milvus JSON field may hold any JSON root; forcing
        // JsonObject throws "Expected a com.google.gson.JsonObject but was
        // com.google.gson.JsonPrimitive" for non-object values.
        MilvusSinkConverter converter = new MilvusSinkConverter();

        Object stringRoot =
                converter.convertBySeaTunnelType(BasicType.STRING_TYPE, true, "\"abc\"");
        assertTrue(stringRoot instanceof JsonPrimitive);
        assertEquals("abc", ((JsonPrimitive) stringRoot).getAsString());

        Object numberRoot = converter.convertBySeaTunnelType(BasicType.STRING_TYPE, true, "123");
        assertTrue(numberRoot instanceof JsonPrimitive);
        assertEquals(123, ((JsonPrimitive) numberRoot).getAsInt());

        Object boolRoot = converter.convertBySeaTunnelType(BasicType.STRING_TYPE, true, "true");
        assertTrue(boolRoot instanceof JsonPrimitive);
        assertTrue(((JsonPrimitive) boolRoot).isBoolean());
    }

    @Test
    void convertsJsonFieldWithArrayRootWithoutFailing() {
        Object converted =
                new MilvusSinkConverter()
                        .convertBySeaTunnelType(BasicType.STRING_TYPE, true, "[1,2,3]");

        assertTrue(converted instanceof JsonArray);
        assertEquals(3, ((JsonArray) converted).size());
    }

    @Test
    void keepsNonJsonStringAsIs() {
        Object converted =
                new MilvusSinkConverter()
                        .convertBySeaTunnelType(BasicType.STRING_TYPE, false, "plain");

        assertEquals("plain", converted);
    }

    private CatalogTable catalogTable() {
        return catalogTable(true);
    }

    private CatalogTable catalogTable(boolean nullable) {
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "nullable_col",
                                        BasicType.STRING_TYPE,
                                        0L,
                                        nullable,
                                        null,
                                        ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private CatalogTable dynamicCatalogTable() {
        Map<String, Object> options = new HashMap<>();
        options.put(CommonOptions.METADATA.getName(), true);
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                TableSchema.builder()
                        .column(
                                new PhysicalColumn(
                                        "dynamic_col",
                                        BasicType.STRING_TYPE,
                                        0L,
                                        null,
                                        false,
                                        null,
                                        "",
                                        null,
                                        null,
                                        options))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private ReadonlyConfig nullableFieldConfig() {
        Map<String, Object> options = new HashMap<>();
        options.put(MilvusSinkOptions.ENABLE_NULLABLE_FIELD.key(), true);
        return ReadonlyConfig.fromMap(options);
    }

    private ReadonlyConfig nullablePartitionKeyConfig() {
        Map<String, Object> options = new HashMap<>();
        options.put(MilvusSinkOptions.ENABLE_NULLABLE_FIELD.key(), true);
        options.put(MilvusSinkOptions.PARTITION_KEY.key(), "nullable_col");
        return ReadonlyConfig.fromMap(options);
    }

    private Column columnWithSinkType(String name, String sinkType) {
        return new PhysicalColumn(
                name,
                BasicType.SHORT_TYPE,
                0L,
                null,
                true,
                null,
                "",
                sinkType,
                null,
                Collections.emptyMap());
    }
}
