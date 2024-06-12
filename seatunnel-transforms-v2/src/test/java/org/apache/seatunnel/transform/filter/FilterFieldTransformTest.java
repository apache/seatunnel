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

package org.apache.seatunnel.transform.filter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FilterFieldTransformTest {

    static List<String> filterKeys = Arrays.asList("key3", "key2");
    static CatalogTable catalogTable;
    static Object[] values;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values = new Object[] {"value1", "value2", "value3", "value4", "value5"};
        SeaTunnelRow inputRow = new SeaTunnelRow(values);
    }

    @Test
    void testConfig() {
        // test both not set
        try {
            new FilterFieldTransform(ReadonlyConfig.fromMap(new HashMap<>()), catalogTable);
        } catch (Exception e) {
            Assertions.assertEquals(
                    "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('include_fields', 'exclude_fields') are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    e.getMessage());
        }

        // test both include and exclude set
        try {
            new FilterFieldTransform(
                    ReadonlyConfig.fromMap(
                            new HashMap<String, Object>() {
                                {
                                    put(
                                            FilterFieldTransformConfig.INCLUDE_FIELDS.key(),
                                            filterKeys);
                                    put(
                                            FilterFieldTransformConfig.EXCLUDE_FIELDS.key(),
                                            filterKeys);
                                }
                            }),
                    catalogTable);
        } catch (Exception e) {
            Assertions.assertEquals(
                    "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('include_fields', 'exclude_fields') are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    e.getMessage());
        }

        // not exception should be thrown now
        new FilterFieldTransform(
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(FilterFieldTransformConfig.INCLUDE_FIELDS.key(), filterKeys);
                            }
                        }),
                catalogTable);

        new FilterFieldTransform(
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(FilterFieldTransformConfig.EXCLUDE_FIELDS.key(), filterKeys);
                            }
                        }),
                catalogTable);
    }

    @Test
    void testInclude() {
        // default include
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FilterFieldTransformConfig.INCLUDE_FIELDS.key(), filterKeys);

        FilterFieldTransform filterFieldTransform =
                new FilterFieldTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        // test output schema
        TableSchema resultSchema = filterFieldTransform.transformTableSchema();
        Assertions.assertNotNull(resultSchema);
        Assertions.assertEquals(filterKeys.size(), resultSchema.getColumns().size());
        for (int i = 0; i < resultSchema.getColumns().size(); i++) {
            Assertions.assertEquals(filterKeys.get(i), resultSchema.getColumns().get(i).getName());
        }

        // test output row
        SeaTunnelRow input = new SeaTunnelRow(values);
        SeaTunnelRow output = filterFieldTransform.transformRow(input);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(filterKeys.size(), output.getFields().length);
        for (int i = 0; i < resultSchema.getFieldNames().length; i++) {
            Integer originalIndex =
                    catalogTable
                            .getTableSchema()
                            .toPhysicalRowDataType()
                            .indexOf(resultSchema.getFieldNames()[i]);
            // test the row's field value
            Assertions.assertEquals(input.getFields()[originalIndex], output.getFields()[i]);
        }
    }

    @Test
    void testExclude() {
        // exclude
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FilterFieldTransformConfig.EXCLUDE_FIELDS.key(), filterKeys);
        FilterFieldTransform filterFieldTransform =
                new FilterFieldTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        // test output schema
        TableSchema resultSchema = filterFieldTransform.transformTableSchema();
        Assertions.assertNotNull(resultSchema);
        Assertions.assertEquals(
                catalogTable.getTableSchema().getColumns().size() - filterKeys.size(),
                resultSchema.getColumns().size());
        for (int i = 0; i < catalogTable.getTableSchema().getFieldNames().length; i++) {
            if (!filterKeys.contains(catalogTable.getTableSchema().getFieldNames()[i])) {
                int finalI = i;
                Assertions.assertTrue(
                        resultSchema.getColumns().stream()
                                .anyMatch(
                                        column ->
                                                column.getName()
                                                        .equals(
                                                                catalogTable.getTableSchema()
                                                                        .getFieldNames()[finalI])));
            }
        }

        // test output row
        SeaTunnelRow input = new SeaTunnelRow(values);
        SeaTunnelRow output = filterFieldTransform.transformRow(input);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(
                catalogTable.getTableSchema().getColumns().size() - filterKeys.size(),
                output.getFields().length);
        for (int i = 0; i < output.getFields().length; i++) {
            if (!filterKeys.contains(catalogTable.getTableSchema().getFieldNames()[i])) {
                Integer originalIndex =
                        catalogTable
                                .getTableSchema()
                                .toPhysicalRowDataType()
                                .indexOf(catalogTable.getTableSchema().getFieldNames()[i]);
                // test the row's field value
                Assertions.assertEquals(input.getFields()[originalIndex], output.getFields()[i]);
            }
        }
    }
}
