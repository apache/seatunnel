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

package org.apache.seatunnel.api.metalake.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class GravitinoTableSchemaConvertorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final GravitinoTableSchemaConvertor CONVERTOR =
            new GravitinoTableSchemaConvertor();

    @Test
    void testBooleanType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"bool_col\",\"type\":\"boolean\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("bool_col", column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertTrue(column.isNullable());
    }

    @Test
    void testByteType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"byte_col\",\"type\":\"byte\",\"nullable\":false}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("byte_col", column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertFalse(column.isNullable());
    }

    @Test
    void testByteUnsignedType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"byte_unsigned_col\",\"type\":\"byte unsigned\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("byte_unsigned_col", column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
    }

    @Test
    void testShortType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"short_col\",\"type\":\"short\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("short_col", column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
    }

    @Test
    void testShortUnsignedType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"short_unsigned_col\",\"type\":\"short unsigned\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("short_unsigned_col", column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
    }

    @Test
    void testIntegerType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"int_col\",\"type\":\"integer\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("int_col", column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
    }

    @Test
    void testIntegerUnsignedType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"int_unsigned_col\",\"type\":\"integer unsigned\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("int_unsigned_col", column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
    }

    @Test
    void testLongType() throws Exception {
        String json = "{\"columns\":[{\"name\":\"long_col\",\"type\":\"long\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("long_col", column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }

    @Test
    void testLongUnsignedType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"long_unsigned_col\",\"type\":\"long unsigned\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("long_unsigned_col", column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }

    @Test
    void testFloatType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"float_col\",\"type\":\"float\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("float_col", column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
    }

    @Test
    void testDoubleType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"double_col\",\"type\":\"double\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("double_col", column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
    }

    @Test
    void testStringType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"str_col\",\"type\":\"string\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("str_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    void testVarcharType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"varchar_col\",\"type\":\"varchar(255)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("varchar_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(Long.valueOf(255), column.getColumnLength());
    }

    @Test
    void testCharType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"char_col\",\"type\":\"char(10)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("char_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(Long.valueOf(10), column.getColumnLength());
    }

    @Test
    void testUuidType() throws Exception {
        String json = "{\"columns\":[{\"name\":\"uuid_col\",\"type\":\"uuid\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("uuid_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    void testIntervalYearType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"interval_year_col\",\"type\":\"interval_year\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("interval_year_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    void testIntervalDayType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"interval_day_col\",\"type\":\"interval_day\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("interval_day_col", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    void testDateType() throws Exception {
        String json = "{\"columns\":[{\"name\":\"date_col\",\"type\":\"date\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("date_col", column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
    }

    @Test
    void testTimeType() throws Exception {
        String json = "{\"columns\":[{\"name\":\"time_col\",\"type\":\"time\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("time_col", column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
    }

    @Test
    void testTimestampType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"timestamp_col\",\"type\":\"timestamp\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("timestamp_col", column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    void testTimestampTzType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"timestamp_tz_col\",\"type\":\"timestamp_tz\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("timestamp_tz_col", column.getName());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    void testBinaryType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"binary_col\",\"type\":\"binary\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        Column column = columns.get(0);
        Assertions.assertEquals("binary_col", column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
    }

    @Test
    void testFixedType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"fixed_col\",\"type\":\"fixed(16)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("fixed_col", column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(Long.valueOf(16), column.getColumnLength());
    }

    @Test
    void testDecimalType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"decimal_col\",\"type\":\"decimal(10,2)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("decimal_col", column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(Integer.valueOf(2), column.getScale());
    }

    @Test
    void testDecimalTypeWithDifferentPrecision() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"decimal_col\",\"type\":\"decimal(38,18)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("decimal_col", column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(Integer.valueOf(18), column.getScale());
    }

    @Test
    void testDecimalTypeUpperCase() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"decimal_col\",\"type\":\"DECIMAL(20,5)\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("decimal_col", column.getName());
        Assertions.assertEquals(new DecimalType(20, 5), column.getDataType());
        Assertions.assertEquals(Integer.valueOf(5), column.getScale());
    }

    @Test
    void testDecimalTypeWithSpaces() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"decimal_col\",\"type\":\"decimal( 10 , 2 )\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        PhysicalColumn column = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals("decimal_col", column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(Integer.valueOf(2), column.getScale());
    }

    @Test
    void testListTypeWithSimpleElementType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"list_col\",\"type\":{\"type\":\"list\",\"elementType\":\"integer\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("list_col", columns.get(0).getName());
        Assertions.assertEquals(BasicType.INT_TYPE, arrayType.getElementType());
    }

    @Test
    void testListTypeWithStringElementType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"list_col\",\"type\":{\"type\":\"list\",\"elementType\":\"string\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("list_col", columns.get(0).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, arrayType.getElementType());
    }

    @Test
    void testListTypeWithDecimalElementType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"list_col\",\"type\":{\"type\":\"list\",\"elementType\":\"decimal(10,2)\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("list_col", columns.get(0).getName());
        Assertions.assertEquals(new DecimalType(10, 2), arrayType.getElementType());
    }

    @Test
    void testMapTypeWithStringKeyIntValue() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"map_col\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        MapType<?, ?> mapType = (MapType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("map_col", columns.get(0).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, mapType.getValueType());
    }

    @Test
    void testMapTypeWithIntKeyLongValue() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"map_col\",\"type\":{\"type\":\"map\",\"keyType\":\"integer\",\"valueType\":\"long\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        MapType<?, ?> mapType = (MapType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("map_col", columns.get(0).getName());
        Assertions.assertEquals(BasicType.INT_TYPE, mapType.getKeyType());
        Assertions.assertEquals(BasicType.LONG_TYPE, mapType.getValueType());
    }

    @Test
    void testMapTypeWithComplexTypes() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"map_col\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"decimal(10,2)\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(1, columns.size());
        MapType<?, ?> mapType = (MapType<?, ?>) columns.get(0).getDataType();
        Assertions.assertEquals("map_col", columns.get(0).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, mapType.getKeyType());
        Assertions.assertEquals(new DecimalType(10, 2), mapType.getValueType());
    }

    @Test
    void testUnsupportedStructType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"struct_col\",\"type\":{\"type\":\"struct\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("struct"),
                "Error message should mention unsupported type 'struct'");
        Assertions.assertTrue(exception.getMessage().contains("struct_col"));
    }

    @Test
    void testUnsupportedUnionType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"union_col\",\"type\":{\"type\":\"union\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("union"),
                "Error message should mention unsupported type 'union'");
        Assertions.assertTrue(exception.getMessage().contains("union_col"));
    }

    @Test
    void testUnsupportedUnknownType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"unknown_col\",\"type\":\"unsupported_type\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("unsupported_type"),
                "Error message should mention unsupported type 'unsupported_type'");
        Assertions.assertTrue(exception.getMessage().contains("unknown_col"));
    }

    @Test
    void testListWithoutElementType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"list_col\",\"type\":{\"type\":\"list\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("list without elementType"),
                "Error message should mention missing elementType");
        Assertions.assertTrue(exception.getMessage().contains("list_col"));
    }

    @Test
    void testMapWithoutKeyOrValueType() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"map_col\",\"type\":{\"type\":\"map\",\"keyType\":\"string\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("map without keyType or valueType"),
                "Error message should mention missing keyType or valueType");
        Assertions.assertTrue(exception.getMessage().contains("map_col"));
    }

    @Test
    void testNullType() throws Exception {
        String json = "{\"columns\":[{\"name\":\"null_col\",\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("null"), "Error message should mention null type");
        Assertions.assertTrue(exception.getMessage().contains("null_col"));
    }

    @Test
    void testComplexTypeWithoutTypeField() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"invalid_col\",\"type\":{\"elementType\":\"integer\"},\"nullable\":true}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> CONVERTOR.convertor(metaInfo));
        Assertions.assertTrue(
                exception.getMessage().contains("invalid_col"),
                "Error message should mention column name");
    }

    @Test
    void testPrimaryKey() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"pk\",\"indexType\":\"PRIMARY_KEY\",\"fieldNames\":[[\"id\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        PrimaryKey primaryKey = schema.getPrimaryKey();
        Assertions.assertNotNull(primaryKey);
        Assertions.assertEquals("pk", primaryKey.getPrimaryKey());
        Assertions.assertEquals(1, primaryKey.getColumnNames().size());
        Assertions.assertEquals("id", primaryKey.getColumnNames().get(0));
    }

    @Test
    void testPrimaryKeyWithMultipleColumns() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id1\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"id2\",\"type\":\"string\",\"nullable\":false},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"pk\",\"indexType\":\"PRIMARY_KEY\",\"fieldNames\":[[\"id1\"],[\"id2\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        PrimaryKey primaryKey = schema.getPrimaryKey();
        Assertions.assertNotNull(primaryKey);
        Assertions.assertEquals("pk", primaryKey.getPrimaryKey());
        Assertions.assertEquals(2, primaryKey.getColumnNames().size());
        Assertions.assertEquals("id1", primaryKey.getColumnNames().get(0));
        Assertions.assertEquals("id2", primaryKey.getColumnNames().get(1));
    }

    @Test
    void testUniqueKey() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"uk_email\",\"indexType\":\"UNIQUE_KEY\",\"fieldNames\":[[\"email\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<ConstraintKey> constraintKeys = schema.getConstraintKeys();
        Assertions.assertEquals(1, constraintKeys.size());
        ConstraintKey uniqueKey = constraintKeys.get(0);
        Assertions.assertEquals("uk_email", uniqueKey.getConstraintName());
        Assertions.assertEquals(
                ConstraintKey.ConstraintType.UNIQUE_KEY, uniqueKey.getConstraintType());
        Assertions.assertEquals(1, uniqueKey.getColumnNames().size());
        Assertions.assertEquals("email", uniqueKey.getColumnNames().get(0).getColumnName());
    }

    @Test
    void testMultipleUniqueKeys() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true},{\"name\":\"username\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"uk_email\",\"indexType\":\"UNIQUE_KEY\",\"fieldNames\":[[\"email\"]]},{\"name\":\"uk_username\",\"indexType\":\"UNIQUE_KEY\",\"fieldNames\":[[\"username\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<ConstraintKey> constraintKeys = schema.getConstraintKeys();
        Assertions.assertEquals(2, constraintKeys.size());
        Assertions.assertEquals("uk_email", constraintKeys.get(0).getConstraintName());
        Assertions.assertEquals("uk_username", constraintKeys.get(1).getConstraintName());
    }

    @Test
    void testPrimaryKeyAndUniqueKey() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"pk\",\"indexType\":\"PRIMARY_KEY\",\"fieldNames\":[[\"id\"]]},{\"name\":\"uk_email\",\"indexType\":\"UNIQUE_KEY\",\"fieldNames\":[[\"email\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        PrimaryKey primaryKey = schema.getPrimaryKey();
        Assertions.assertNotNull(primaryKey);
        Assertions.assertEquals("pk", primaryKey.getPrimaryKey());
        List<ConstraintKey> constraintKeys = schema.getConstraintKeys();
        Assertions.assertEquals(1, constraintKeys.size());
        Assertions.assertEquals("uk_email", constraintKeys.get(0).getConstraintName());
    }

    @Test
    void testMultipleColumnsOfDifferentTypes() throws Exception {
        String json =
                "{"
                        + "\"columns\":["
                        + "{\"name\":\"id\",\"type\":\"long\",\"nullable\":false},"
                        + "{\"name\":\"name\",\"type\":\"varchar(100)\",\"nullable\":true},"
                        + "{\"name\":\"amount\",\"type\":\"decimal(10,2)\",\"nullable\":true},"
                        + "{\"name\":\"created_at\",\"type\":\"timestamp\",\"nullable\":true},"
                        + "{\"name\":\"is_active\",\"type\":\"boolean\",\"nullable\":true},"
                        + "{\"name\":\"data\",\"type\":\"binary\",\"nullable\":true},"
                        + "{\"name\":\"tags\",\"type\":{\"type\":\"list\",\"elementType\":\"string\"},\"nullable\":true},"
                        + "{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\"},\"nullable\":true}"
                        + "],"
                        + "\"indexes\":["
                        + "{\"name\":\"pk\",\"indexType\":\"PRIMARY_KEY\",\"fieldNames\":[[\"id\"]]}"
                        + "]"
                        + "}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);

        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(8, columns.size());

        Assertions.assertEquals("id", columns.get(0).getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, columns.get(0).getDataType());

        Assertions.assertEquals("name", columns.get(1).getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(1).getDataType());
        Assertions.assertEquals(
                Long.valueOf(100), ((PhysicalColumn) columns.get(1)).getColumnLength());

        Assertions.assertEquals("amount", columns.get(2).getName());
        Assertions.assertEquals(new DecimalType(10, 2), columns.get(2).getDataType());
        Assertions.assertEquals(Integer.valueOf(2), ((PhysicalColumn) columns.get(2)).getScale());

        Assertions.assertEquals("created_at", columns.get(3).getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, columns.get(3).getDataType());

        Assertions.assertEquals("is_active", columns.get(4).getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, columns.get(4).getDataType());

        Assertions.assertEquals("data", columns.get(5).getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, columns.get(5).getDataType());

        Assertions.assertEquals("tags", columns.get(6).getName());
        ArrayType<?, ?> tagsType = (ArrayType<?, ?>) columns.get(6).getDataType();
        Assertions.assertEquals(BasicType.STRING_TYPE, tagsType.getElementType());

        Assertions.assertEquals("metadata", columns.get(7).getName());
        MapType<?, ?> metadataType = (MapType<?, ?>) columns.get(7).getDataType();
        Assertions.assertEquals(BasicType.STRING_TYPE, metadataType.getKeyType());
        Assertions.assertEquals(BasicType.STRING_TYPE, metadataType.getValueType());

        PrimaryKey primaryKey = schema.getPrimaryKey();
        Assertions.assertNotNull(primaryKey);
        Assertions.assertEquals("pk", primaryKey.getPrimaryKey());
    }

    // ==================== BuildCatalogTable Tests ====================

    @Test
    void testBuildCatalogTable() throws Exception {
        String json =
                "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true}],"
                        + "\"indexes\":[{\"name\":\"pk\",\"indexType\":\"PRIMARY_KEY\",\"fieldNames\":[[\"id\"]]}]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema tableSchema = CONVERTOR.convertor(metaInfo);
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        CatalogTable catalogTable =
                CONVERTOR.buildCatalogTable("test_catalog", tablePath, tableSchema);
        Assertions.assertEquals("test_catalog", catalogTable.getCatalogName());
        Assertions.assertEquals("test_catalog", catalogTable.getTableId().getCatalogName());
        Assertions.assertEquals("test_database", catalogTable.getTableId().getDatabaseName());
        Assertions.assertEquals("test_schema", catalogTable.getTableId().getSchemaName());
        Assertions.assertEquals("test_table", catalogTable.getTableId().getTableName());
        Assertions.assertEquals(tableSchema, catalogTable.getTableSchema());
    }

    @Test
    void testEmptyColumns() throws Exception {
        String json = "{\"columns\":[]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertTrue(columns.isEmpty());
    }

    @Test
    void testNoColumnsField() throws Exception {
        String json = "{\"indexes\":[]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertTrue(columns.isEmpty());
    }

    @Test
    void testCaseInsensitiveTypeMatching() throws Exception {
        String json =
                "{\"columns\":["
                        + "{\"name\":\"col1\",\"type\":\"BOOLEAN\",\"nullable\":true},"
                        + "{\"name\":\"col2\",\"type\":\"INTEGER\",\"nullable\":true},"
                        + "{\"name\":\"col3\",\"type\":\"STRING\",\"nullable\":true},"
                        + "{\"name\":\"col4\",\"type\":\"DOUBLE\",\"nullable\":true}"
                        + "]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);
        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(4, columns.size());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, columns.get(0).getDataType());
        Assertions.assertEquals(BasicType.INT_TYPE, columns.get(1).getDataType());
        Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(2).getDataType());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, columns.get(3).getDataType());
    }

    @Test
    void testMixedCaseTypeWithParameters() throws Exception {
        String json =
                "{\"columns\":["
                        + "{\"name\":\"col1\",\"type\":\"VARCHAR(100)\",\"nullable\":true},"
                        + "{\"name\":\"col2\",\"type\":\"CHAR(10)\",\"nullable\":true},"
                        + "{\"name\":\"col3\",\"type\":\"DECIMAL(20,5)\",\"nullable\":true},"
                        + "{\"name\":\"col4\",\"type\":\"Fixed(8)\",\"nullable\":true}"
                        + "]}";
        JsonNode metaInfo = OBJECT_MAPPER.readTree(json);
        TableSchema schema = CONVERTOR.convertor(metaInfo);

        List<Column> columns = schema.getColumns();
        Assertions.assertEquals(4, columns.size());

        PhysicalColumn col1 = (PhysicalColumn) columns.get(0);
        Assertions.assertEquals(BasicType.STRING_TYPE, col1.getDataType());
        Assertions.assertEquals(Long.valueOf(100), col1.getColumnLength());

        PhysicalColumn col2 = (PhysicalColumn) columns.get(1);
        Assertions.assertEquals(BasicType.STRING_TYPE, col2.getDataType());
        Assertions.assertEquals(Long.valueOf(10), col2.getColumnLength());

        PhysicalColumn col3 = (PhysicalColumn) columns.get(2);
        Assertions.assertEquals(new DecimalType(20, 5), col3.getDataType());
        Assertions.assertEquals(Integer.valueOf(5), col3.getScale());

        PhysicalColumn col4 = (PhysicalColumn) columns.get(3);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, col4.getDataType());
        Assertions.assertEquals(Long.valueOf(8), col4.getColumnLength());
    }
}
