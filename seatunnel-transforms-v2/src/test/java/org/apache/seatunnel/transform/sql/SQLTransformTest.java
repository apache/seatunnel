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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class SQLTransformTest {

    private static final String TEST_NAME = "test";
    private static final String TIMESTAMP_FILEDNAME = "create_time";
    private static final String[] FILED_NAMES =
            new String[] {"id", "name", "age", TIMESTAMP_FILEDNAME};
    private static final String GENERATE_PARTITION_KEY = "dt";
    private static final ReadonlyConfig READONLY_CONFIG =
            ReadonlyConfig.fromMap(
                    new HashMap<String, Object>() {
                        {
                            put(
                                    "query",
                                    "select *,FORMATDATETIME(create_time,'yyyy-MM-dd HH:mm') as dt from dual");
                        }
                    });

    @Test
    public void testScaleSupport() {
        SQLTransform sqlTransform = new SQLTransform(READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (column.getName().equals(TIMESTAMP_FILEDNAME)) {
                                Assertions.assertEquals(9, column.getScale());
                            } else if (column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertTrue(Objects.isNull(column.getScale()));
                            } else {
                                Assertions.assertEquals(3, column.getColumnLength());
                            }
                        });
    }

    @Test
    public void testQueryWithAnyTable() {
        SQLTransform sqlTransform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("query", "select * from dual");
                                    }
                                }),
                        getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        Assertions.assertEquals(4, tableSchema.getColumns().size());
    }

    @Test
    public void testNotLoseSourceTypeAndOptions() {
        SQLTransform sqlTransform = new SQLTransform(READONLY_CONFIG, getCatalogTable());
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (!column.getName().equals(GENERATE_PARTITION_KEY)) {
                                Assertions.assertEquals(
                                        "source_" + column.getDataType(), column.getSourceType());
                                Assertions.assertEquals(
                                        "testInSQL", column.getOptions().get("context"));
                            }
                        });
    }

    private CatalogTable getCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        FILED_NAMES,
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Integer scale = null;
            Long columnLength = null;
            if (rowType.getFieldName(i).equals(TIMESTAMP_FILEDNAME)) {
                scale = 9;
            } else {
                columnLength = 3L;
            }
            PhysicalColumn column =
                    new PhysicalColumn(
                            rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            columnLength,
                            scale,
                            true,
                            null,
                            null,
                            "source_" + rowType.getFieldType(i),
                            new HashMap<String, Object>() {
                                {
                                    put("context", "testInSQL");
                                }
                            });
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of(TEST_NAME, TEST_NAME, null, TEST_NAME),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It has column information.");
    }

    @Test
    public void testEscapeIdentifier() {
        String tableName = "test";
        String[] fields = new String[] {"id", "apply"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select `id`, trim(`apply`) as `apply` from dual where `apply` = 'a'"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        TableSchema tableSchema = sqlTransform.transformTableSchema();
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("a")}));
        Assertions.assertEquals("id", tableSchema.getFieldNames()[0]);
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals("a", result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("b")}));
        Assertions.assertNull(result);

        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, IFNULL(`apply`, '1') as `apply` from dual  where `apply` = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), String.valueOf("a")}));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE}));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, `apply` + 1 as `apply` from dual where `apply` > 0"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), Long.valueOf(1)}));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(BasicType.LONG_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals(Long.valueOf(2), result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), Long.valueOf(0)}));
        Assertions.assertNull(result);

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    new MapType<String, String>(
                                            BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                                }));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, `apply`.k1 as `apply` from dual where `apply`.k1 = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("k1", "a")
                                }));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("k1", "b")
                                }));
        Assertions.assertNull(result);

        table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                new String[] {"id", "map"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    new MapType<String, String>(
                                            BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                                }));
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, map.`apply` as `apply` from dual where map.`apply` = 'a'"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();
        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(
                                new Object[] {
                                    Integer.valueOf(1), Collections.singletonMap("apply", "a")
                                }));
        Assertions.assertEquals("apply", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());
        Assertions.assertEquals("a", result.get(0).getField(1));
    }
}
