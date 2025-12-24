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
package org.apache.seatunnel.transform;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransform;
import org.apache.seatunnel.transform.jsonpath.JsonPathTransformConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JsonPathTransformTest {

    @Test
    public void testJsonPath() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(), "f1")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);

        CatalogTable outputTable = transform.getProducedCatalogTable();
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f1\": 1}"}));
        Assertions.assertEquals(
                "1", outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("f1")));
    }

    @Test
    public void testErrorHandleWay() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(), "f1")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        CatalogTable outputTable = transform.getProducedCatalogTable();
        final JsonPathTransform finalTransform = transform;
        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> finalTransform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"})));

        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.FAIL.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        JsonPathTransform finalTransform1 = transform;
        Assertions.assertThrows(
                ErrorDataTransformException.class,
                () -> finalTransform1.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"})));

        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNotNull(outputRow);
        Assertions.assertNull(outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("f1")));

        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP_ROW.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNull(outputRow);

        configMap.put(
                TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(),
                ErrorHandleWay.SKIP.name());
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(), "f1")));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNull(outputRow);

        configMap.put(
                TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(),
                ErrorHandleWay.SKIP.name());
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.FAIL.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        try {
            outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
            Assertions.fail("should throw exception");
        } catch (Exception e) {
            // ignore
        }

        configMap.put(
                TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(),
                ErrorHandleWay.FAIL.name());
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNotNull(outputRow);
        Assertions.assertNull(outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("f1")));

        configMap.put(
                TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(),
                ErrorHandleWay.FAIL.name());
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(),
                                "data",
                                JsonPathTransformConfig.PATH.key(),
                                "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                "f1",
                                TransformCommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP_ROW.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNull(outputRow);
    }

    @Test
    public void testOutputColumn() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), "$.f1",
                                JsonPathTransformConfig.DEST_FIELD.key(), "f1")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default", "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "data",
                                                BasicType.STRING_TYPE,
                                                1024,
                                                true,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        CatalogTable outputCatalogTable = transform.getProducedCatalogTable();
        Column f1 = outputCatalogTable.getTableSchema().getColumn("f1");
        Assertions.assertEquals(BasicType.STRING_TYPE, f1.getDataType());
        Assertions.assertEquals(1024, f1.getColumnLength());

        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f1\": 1}"}));
        Assertions.assertNotNull(outputRow);
    }

    @Test
    public void testBatchFieldsValidation() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), Arrays.asList("$.id", "$.name"),
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                        Arrays.asList("id", "name", "age"),
                                JsonPathTransformConfig.DEST_TYPE.key(),
                                        Arrays.asList("bigint", "string"))));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    JsonPathTransformConfig.of(config, table);
                });
    }

    @Test
    public void testBatchFields() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), Arrays.asList("$.id", "$.name"),
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                        Arrays.asList("id", "name"),
                                JsonPathTransformConfig.DEST_TYPE.key(),
                                        Arrays.asList("bigint", "string")),
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(), "$.status",
                                JsonPathTransformConfig.DEST_FIELD.key(), "status",
                                JsonPathTransformConfig.DEST_TYPE.key(), "int")));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);

        CatalogTable outputTable = transform.getProducedCatalogTable();
        SeaTunnelRow outputRow =
                transform.map(
                        new SeaTunnelRow(
                                new Object[] {
                                    "{\"id\": 1001, \"name\": \"John\", \"status\": 1}"
                                }));

        Assertions.assertEquals(
                1001L, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("id")));
        Assertions.assertEquals(
                "John", outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("name")));
        Assertions.assertEquals(
                1, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("status")));
    }

    @Test
    public void testBatchFieldsWithNestedJson() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(),
                                        Arrays.asList(
                                                "$.user.profile.name",
                                                "$.user.profile.age",
                                                "$.user.settings.theme"),
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                        Arrays.asList("user_name", "user_age", "user_theme"),
                                JsonPathTransformConfig.DEST_TYPE.key(),
                                        Arrays.asList("string", "int", "string"))));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);

        CatalogTable outputTable = transform.getProducedCatalogTable();
        String jsonData =
                "{\"user\":{\"profile\":{\"name\":\"Alice\",\"age\":25},\"settings\":{\"theme\":\"dark\"}}}";
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {jsonData}));
        Assertions.assertEquals(
                "Alice",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_name")));
        Assertions.assertEquals(
                25, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_age")));
        Assertions.assertEquals(
                "dark",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_theme")));
    }

    @Test
    public void testBatchFieldsWithArrays() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(),
                                        Arrays.asList(
                                                "$.orders[0].id",
                                                "$.orders[0].amount",
                                                "$.orders[1].id"),
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                        Arrays.asList(
                                                "first_order_id",
                                                "first_amount",
                                                "second_order_id"),
                                JsonPathTransformConfig.DEST_TYPE.key(),
                                        Arrays.asList("int", "double", "int"))));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);

        CatalogTable outputTable = transform.getProducedCatalogTable();
        String jsonData =
                "{\"orders\":[{\"id\":101,\"amount\":50.5},{\"id\":102,\"amount\":75.8}]}";
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {jsonData}));
        Assertions.assertEquals(
                101,
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("first_order_id")));
        Assertions.assertEquals(
                50.5,
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("first_amount")));
        Assertions.assertEquals(
                102,
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("second_order_id")));
    }

    @Test
    public void testAllFieldsInSingleBatchConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                JsonPathTransformConfig.COLUMNS.key(),
                Arrays.asList(
                        ImmutableMap.of(
                                JsonPathTransformConfig.SRC_FIELD.key(), "data",
                                JsonPathTransformConfig.PATH.key(),
                                        Arrays.asList(
                                                "$.id",
                                                "$.name",
                                                "$.status",
                                                "$.user.profile.age",
                                                "$.user.profile.email",
                                                "$.user.settings.theme",
                                                "$.orders[0].id",
                                                "$.orders[0].amount",
                                                "$.orders[1].id",
                                                "$.metadata.created_at",
                                                "$.total"),
                                JsonPathTransformConfig.DEST_FIELD.key(),
                                        Arrays.asList(
                                                "id",
                                                "name",
                                                "status",
                                                "user_age",
                                                "user_email",
                                                "user_theme",
                                                "order1_id",
                                                "order1_amount",
                                                "order2_id",
                                                "created_at",
                                                "total"),
                                JsonPathTransformConfig.DEST_TYPE.key(),
                                        Arrays.asList(
                                                "bigint", "string", "int", "int", "string",
                                                "string", "int", "double", "int", "string",
                                                "double"))));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"data"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
        JsonPathTransform transform =
                new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);

        String allTypesJsonData =
                "{"
                        + "\"id\": 1001,"
                        + "\"name\": \"CompleteTest\","
                        + "\"status\": 1,"
                        + "\"total\": 599.99,"
                        + "\"user\": {"
                        + "  \"profile\": {"
                        + "    \"age\": 30,"
                        + "    \"email\": \"test@example.com\""
                        + "  },"
                        + "  \"settings\": {"
                        + "    \"theme\": \"light\""
                        + "  }"
                        + "},"
                        + "\"orders\": ["
                        + "  {\"id\": 201, \"amount\": 299.99},"
                        + "  {\"id\": 202, \"amount\": 300.00}"
                        + "],"
                        + "\"metadata\": {"
                        + "  \"created_at\": \"2023-10-30T12:00:00Z\""
                        + "}"
                        + "}";

        CatalogTable outputTable = transform.getProducedCatalogTable();
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {allTypesJsonData}));

        String[] fieldNames = outputTable.getSeaTunnelRowType().getFieldNames();
        Assertions.assertEquals(12, fieldNames.length);
        Assertions.assertEquals(
                1001L, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("id")));
        Assertions.assertEquals(
                "CompleteTest",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("name")));
        Assertions.assertEquals(
                1, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("status")));
        Assertions.assertEquals(
                599.99, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("total")));

        Assertions.assertEquals(
                30, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_age")));
        Assertions.assertEquals(
                "test@example.com",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_email")));
        Assertions.assertEquals(
                "light",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("user_theme")));

        Assertions.assertEquals(
                201, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("order1_id")));
        Assertions.assertEquals(
                299.99,
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("order1_amount")));
        Assertions.assertEquals(
                202, outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("order2_id")));
        Assertions.assertEquals(
                "2023-10-30T12:00:00Z",
                outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("created_at")));
    }
}
