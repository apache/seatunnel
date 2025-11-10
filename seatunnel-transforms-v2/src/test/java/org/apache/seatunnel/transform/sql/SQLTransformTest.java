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
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class SQLTransformTest {

    private static final String TEST_NAME = "test";
    private static final String TIMESTAMP_FIELDNAME = "create_time";
    private static final String[] FIELD_NAMES =
            new String[] {"id", "name", "age", TIMESTAMP_FIELDNAME};
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
                            if (column.getName().equals(TIMESTAMP_FIELDNAME)) {
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
                        FIELD_NAMES,
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
            if (rowType.getFieldName(i).equals(TIMESTAMP_FIELDNAME)) {
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

    @Test
    public void tesCaseWhenClausesWithBooleanField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "bool"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select `id`, `bool`, case when bool then 1 else 2 end as bool_1 from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), true}));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(true, result.get(0).getField(1));
        Assertions.assertEquals(1, result.get(0).getField(2));

        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), false}));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(false, result.get(0).getField(1));
        Assertions.assertEquals(2, result.get(0).getField(2));
    }

    @Test
    public void tesCaseWhenBooleanClausesWithField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select `id`, `int`, (case when `int` = 1 then true else false end) as bool_1 , `string`, (case when `string` = 'true' then true else false end) as bool_2 from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 1, "true"}));

        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(1, result.get(0).getField(1));
        Assertions.assertEquals(true, result.get(0).getField(2));
        Assertions.assertEquals("true", result.get(0).getField(3));
        Assertions.assertEquals(true, result.get(0).getField(4));

        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 0, "false"}));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(0, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
        Assertions.assertEquals("false", result.get(0).getField(3));
        Assertions.assertEquals(false, result.get(0).getField(4));
    }

    @Test
    public void tesCastBooleanClausesWithField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select `id`, `int`, cast(`int` as boolean) as bool_1 , `string`, cast(`string` as boolean) as bool_2 from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), 1, "true"}));

        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(1, result.get(0).getField(1));
        Assertions.assertEquals(true, result.get(0).getField(2));
        Assertions.assertEquals("true", result.get(0).getField(3));
        Assertions.assertEquals(true, result.get(0).getField(4));

        result =
                sqlTransform.transformRow(
                        new SeaTunnelRow(new Object[] {Integer.valueOf(1), 0, "false"}));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(0, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
        Assertions.assertEquals("false", result.get(0).getField(3));
        Assertions.assertEquals(false, result.get(0).getField(4));

        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 3, "false"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[TRANSFORM_COMMON-06], ErrorDescription:[The expression 'cast(`int` AS boolean)' of SQL transform execute failed]",
                                e.getMessage());
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST AS Boolean: 3",
                                e.getCause().getMessage());
                        throw e;
                    }
                });

        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {Integer.valueOf(1), 0, "false333"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[TRANSFORM_COMMON-06], ErrorDescription:[The expression 'cast(`string` AS boolean)' of SQL transform execute failed]",
                                e.getMessage());
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST AS Boolean: false333",
                                e.getCause().getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void tesBooleanField() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query", "select `id`, true as bool_1, false as bool_2 from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, 1, "true"}));
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals(true, result.get(0).getField(1));
        Assertions.assertEquals(false, result.get(0).getField(2));
    }

    @Test
    public void testExpressionErrorField() {
        String tableName = "test";
        String[] fields = new String[] {"FIELD1", "FIELD2", "FIELD3"};
        SeaTunnelDataType[] fieldTypes =
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE, BasicType.DOUBLE_TYPE, BasicType.STRING_TYPE
                };
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName, new SeaTunnelRowType(fields, fieldTypes));
        String sqlQuery =
                "select "
                        + "CAST(`FIELD1` AS STRING) AS FIELD1, "
                        + "CAST(`FIELD1` AS decimal(22,4)) AS FIELD2, "
                        + "CAST(`FIELD3` AS decimal(22,0)) AS FIELD3 "
                        + "from dual";

        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", sqlQuery));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformRow(
                                new SeaTunnelRow(new Object[] {1, 123.123, "true"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[TRANSFORM_COMMON-06], ErrorDescription:[The expression 'CAST(`FIELD3` AS decimal (22, 0))' of SQL transform execute failed]",
                                e.getMessage());
                        throw e;
                    }
                });
        sqlQuery = "select * from dual where FIELD1/0 > 10";
        config = ReadonlyConfig.fromMap(Collections.singletonMap("query", sqlQuery));
        SQLTransform sqlTransform2 = new SQLTransform(config, table);
        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform2.transformRow(
                                new SeaTunnelRow(new Object[] {1, 123.123, "true"}));
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[TRANSFORM_COMMON-07], ErrorDescription:[The where statement 'FIELD1 / 0 > 10' of SQL transform execute failed]",
                                e.getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void testCoalesceTypeConversion() {
        String tableName = "test";
        String[] fields = new String[] {"id", "stringField", "intField", "doubleField"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.DOUBLE_TYPE
                                }));

        // The first parameter to test COALESCE is the string type, followed by the integer type
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, COALESCE(stringField, intField) as result from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        TableSchema tableSchema = sqlTransform.transformTableSchema();

        // Verify that the field type is STRING
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());

        // The first field is not null, and the value of the first field should be directly returned
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals("test", result.get(0).getField(1));

        // The first field is null, and the value converted to the string should be returned.
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, null, 123, 123.45}));
        Assertions.assertEquals("123", result.get(0).getField(1));
        // Make sure the return value is a string type rather than an integer type
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof String,
                "The result should be a string type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // The first parameter to test COALESCE is the integer type, followed by the floating point
        // type
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, COALESCE(intField, doubleField) as result from dual"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();

        // Verify that the field type is INT
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(BasicType.INT_TYPE, tableSchema.getColumns().get(1).getDataType());

        // The first field is not null, and the value of the first field should be directly
        // returned
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals(123, result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof Integer,
                "The result should be an integer type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // The first field is null, and the value converted to an integer should be returned.
        result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", null, 456.78}));
        Assertions.assertEquals(456, result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof Integer,
                "The result should be an integer type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // Test COALESCE with null as first argument
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, COALESCE(null, stringField, intField) as result from dual"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();

        // Verify that the result field type is STRING (since stringField is the first non-null
        // parameter)
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());

        // Test with both stringField and intField having values
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals("test", result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof String,
                "The result should be a string type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // Test with stringField being null, should return intField as string
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, null, 123, 123.45}));
        Assertions.assertEquals("123", result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof String,
                "The result should be a string type, but is actually "
                        + result.get(0).getField(1).getClass().getName());
    }

    @Test
    public void testIfNullTypeConversion() {
        String tableName = "test";
        String[] fields = new String[] {"id", "stringField", "intField", "doubleField"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.DOUBLE_TYPE
                                }));

        // Test IFNULL with string field as first parameter and integer as second
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, IFNULL(stringField, intField) as result from dual"));
        SQLTransform sqlTransform = new SQLTransform(config, table);
        TableSchema tableSchema = sqlTransform.transformTableSchema();

        // Verify that the field type is STRING
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());

        // The first field is not null, and the value of the first field should be directly returned
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals("test", result.get(0).getField(1));

        // The first field is null, and the value converted to the string should be returned.
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, null, 123, 123.45}));
        Assertions.assertEquals("123", result.get(0).getField(1));
        // Make sure the return value is a string type rather than an integer type
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof String,
                "The result should be a string type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // Test IFNULL with integer field as first parameter and double as second
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, IFNULL(intField, doubleField) as result from dual"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();

        // Verify that the field type is INT
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(BasicType.INT_TYPE, tableSchema.getColumns().get(1).getDataType());

        // The first field is not null, and the value of the first field should be directly
        // returned
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals(123, result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof Integer,
                "The result should be an integer type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // The first field is null, and the value converted to an integer should be returned.
        result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", null, 456.78}));
        Assertions.assertEquals(456, result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof Integer,
                "The result should be an integer type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // Test IFNULL with null literal as first argument
        config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select id, IFNULL(null, stringField) as result from dual"));
        sqlTransform = new SQLTransform(config, table);
        tableSchema = sqlTransform.transformTableSchema();

        // Verify that the result field type is STRING
        Assertions.assertEquals("result", tableSchema.getFieldNames()[1]);
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tableSchema.getColumns().get(1).getDataType());

        // Test with stringField having a value
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, "test", 123, 123.45}));
        Assertions.assertEquals("test", result.get(0).getField(1));
        Assertions.assertTrue(
                result.get(0).getField(1) instanceof String,
                "The result should be a string type, but is actually "
                        + result.get(0).getField(1).getClass().getName());

        // Test with stringField being null, should return null
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {1, null, 123, 123.45}));
        Assertions.assertNull(result.get(0).getField(1));
    }

    public void testCastTimestampValidate() {
        String querySql = "select CAST(`id` AS TIMESTAMP) AS idStr, name AS name from dual";
        SQLTransform sqlTransform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("query", querySql);
                                    }
                                }),
                        getCatalogTable());
        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformTableSchema();
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST FROM INT AS type: TIMESTAMP",
                                e.getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void testCastIntValidate() {
        String querySql =
                "select id AS id, name AS name, CAST(create_time AS INT) AS timeInt from dual";
        SQLTransform sqlTransform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("query", querySql);
                                    }
                                }),
                        getCatalogTable());
        Assertions.assertThrows(
                TransformException.class,
                () -> {
                    try {
                        sqlTransform.transformTableSchema();
                    } catch (Exception e) {
                        Assertions.assertEquals(
                                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Unsupported CAST FROM TIMESTAMP AS type: INT",
                                e.getMessage());
                        throw e;
                    }
                });
    }

    @Test
    public void testTrimWithCastExpression() {
        // Test TRIM(CAST(id AS VARCHAR)) - fix for ClassCastException bug
        String tableName = "test";
        String[] fields = new String[] {"id", "name"};
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
                                "select id, TRIM(CAST(id AS VARCHAR)) as id_str, name from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {123, "test"}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(123, result.get(0).getField(0));
        Assertions.assertEquals("123", result.get(0).getField(1));
        Assertions.assertEquals("test", result.get(0).getField(2));
    }

    @Test
    public void testTrimWithMultipleCastExpressions() {
        // Test multiple TRIM(CAST(...)) in one query
        String tableName = "test";
        String[] fields = new String[] {"int_val", "long_val", "double_val"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.LONG_TYPE, BasicType.DOUBLE_TYPE
                                }));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select "
                                        + "TRIM(CAST(int_val AS VARCHAR)) as int_str, "
                                        + "TRIM(CAST(long_val AS VARCHAR)) as long_str, "
                                        + "TRIM(CAST(double_val AS VARCHAR)) as double_str "
                                        + "from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {123, 456L, 789.12}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("123", result.get(0).getField(0));
        Assertions.assertEquals("456", result.get(0).getField(1));
        Assertions.assertEquals("789.12", result.get(0).getField(2));
    }

    @Test
    public void testTrimWithNestedFunctions() {
        // Test TRIM with nested CAST and other functions
        String tableName = "test";
        String[] fields = new String[] {"id", "name"};
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
                                "select id, UPPER(TRIM(CAST(id AS VARCHAR))) as id_upper from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {123, "test"}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(123, result.get(0).getField(0));
        Assertions.assertEquals("123", result.get(0).getField(1));
    }

    @Test
    public void testTrimWithCastInWhereClause() {
        // Test TRIM(CAST(...)) in WHERE clause
        String tableName = "test";
        String[] fields = new String[] {"id", "name"};
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
                                "select id, name from dual where TRIM(CAST(id AS VARCHAR)) = '123'"));

        SQLTransform sqlTransform = new SQLTransform(config, table);

        // Should match
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {123, "test"}));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(123, result.get(0).getField(0));
        Assertions.assertEquals("test", result.get(0).getField(1));

        // Should not match
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {456, "test2"}));
        Assertions.assertNull(result);
    }

    @Test
    public void testTrimWithCastNull() {
        // Test TRIM(CAST(NULL AS VARCHAR))
        String tableName = "test";
        String[] fields = new String[] {"id", "name"};
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
                                "select id, TRIM(CAST(id AS VARCHAR)) as id_str from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {null, "test"}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertNull(result.get(0).getField(0));
        Assertions.assertNull(result.get(0).getField(1)); // TRIM(CAST(NULL)) should be NULL
    }

    @Test
    public void testTrimWithConcatFunction() {
        // Test TRIM(CONCAT(...)) - function inside TRIM
        String tableName = "test";
        String[] fields = new String[] {"first_name", "last_name"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select TRIM(CONCAT(first_name, ' ', last_name)) as full_name from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {"John", "Doe"}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("John Doe", result.get(0).getField(0));
    }

    @Test
    public void testTrimWithSubstringFunction() {
        // Test TRIM(SUBSTRING(...)) - another function inside TRIM
        String tableName = "test";
        String[] fields = new String[] {"text"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields, new SeaTunnelDataType[] {BasicType.STRING_TYPE}));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select TRIM(SUBSTRING(text, 1, 5)) as trimmed from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {"  Hello World  "}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Hel", result.get(0).getField(0));
    }

    @Test
    public void testTrimWithReplaceFunction() {
        // Test TRIM(REPLACE(...)) - yet another function inside TRIM
        String tableName = "test";
        String[] fields = new String[] {"text"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields, new SeaTunnelDataType[] {BasicType.STRING_TYPE}));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select TRIM(REPLACE(text, 'old', 'new')) as replaced from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {" old text "}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("new text", result.get(0).getField(0));
    }

    @Test
    public void testTrimWithArithmeticExpression() {
        // Test TRIM with arithmetic expression (id + 100)
        String tableName = "test";
        String[] fields = new String[] {"id", "name"};
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
                                "select TRIM(CAST(id + 100 AS VARCHAR)) as result from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {23, "test"}));

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("123", result.get(0).getField(0));
    }

    @Test
    public void testTrimWithCoalesceFunction() {
        // Test TRIM(COALESCE(...)) - system function inside TRIM
        String tableName = "test";
        String[] fields = new String[] {"name", "default_name"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE, BasicType.STRING_TYPE
                                }));

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select TRIM(COALESCE(name, default_name)) as result from dual"));

        SQLTransform sqlTransform = new SQLTransform(config, table);

        // Test with non-null name
        List<SeaTunnelRow> result =
                sqlTransform.transformRow(new SeaTunnelRow(new Object[] {" John ", "Default"}));
        Assertions.assertEquals("John", result.get(0).getField(0));

        // Test with null name
        result = sqlTransform.transformRow(new SeaTunnelRow(new Object[] {null, " Default "}));
        Assertions.assertEquals("Default", result.get(0).getField(0));
    }
}
