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
import org.apache.seatunnel.transform.common.CommonOptions;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP_ROW.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNull(outputRow);

        configMap.put(CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), ErrorHandleWay.SKIP.name());
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

        configMap.put(CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), ErrorHandleWay.SKIP.name());
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
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

        configMap.put(CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), ErrorHandleWay.FAIL.name());
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
                                ErrorHandleWay.SKIP.name())));
        config = ReadonlyConfig.fromMap(configMap);
        transform = new JsonPathTransform(JsonPathTransformConfig.of(config, table), table);
        outputTable = transform.getProducedCatalogTable();
        outputRow = transform.map(new SeaTunnelRow(new Object[] {"{\"f2\": 1}"}));
        Assertions.assertNotNull(outputRow);
        Assertions.assertNull(outputRow.getField(outputTable.getSeaTunnelRowType().indexOf("f1")));

        configMap.put(CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), ErrorHandleWay.FAIL.name());
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
                                CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key(),
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
}
