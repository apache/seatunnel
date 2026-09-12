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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DuckDBTypeConverterTest {

    @Test
    void testConvertBoolean() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("f_boolean")
                        .columnType("boolean")
                        .dataType("boolean")
                        .nullable(true)
                        .defaultValue(true)
                        .comment("flag")
                        .build();
        Column column = DuckDBTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals("f_boolean", column.getName());
        Assertions.assertEquals(true, column.getDefaultValue());
        Assertions.assertEquals("flag", column.getComment());
    }

    @Test
    void testConvertTinyint() {
        Assertions.assertEquals(BasicType.BYTE_TYPE, convert("f_tinyint", "tinyint").getDataType());
    }

    @Test
    void testConvertUnsignedTinyint() {
        Assertions.assertEquals(
                BasicType.BYTE_TYPE, convert("f_utinyint", "utinyint").getDataType());
    }

    @Test
    void testConvertSmallint() {
        Assertions.assertEquals(
                BasicType.SHORT_TYPE, convert("f_smallint", "smallint").getDataType());
    }

    @Test
    void testConvertUnsignedSmallint() {
        Assertions.assertEquals(
                BasicType.SHORT_TYPE, convert("f_usmallint", "usmallint").getDataType());
    }

    @Test
    void testConvertInteger() {
        Assertions.assertEquals(BasicType.INT_TYPE, convert("f_integer", "integer").getDataType());
    }

    @Test
    void testConvertUnsignedInteger() {
        Assertions.assertEquals(
                BasicType.INT_TYPE, convert("f_uinteger", "uinteger").getDataType());
    }

    @Test
    void testConvertBigint() {
        Assertions.assertEquals(BasicType.LONG_TYPE, convert("f_bigint", "bigint").getDataType());
    }

    @Test
    void testConvertUnsignedBigint() {
        Assertions.assertEquals(BasicType.LONG_TYPE, convert("f_ubigint", "ubigint").getDataType());
    }

    @Test
    void testConvertHugeint() {
        Column column = convert("f_hugeint", "hugeint");
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
    }

    @Test
    void testConvertUnsignedHugeint() {
        Column column = convert("f_uhugeint", "uhugeint");
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
    }

    @Test
    void testConvertBignum() {
        Column column = convert("f_bignum", "bignum");
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
    }

    @Test
    void testConvertFloat() {
        Assertions.assertEquals(BasicType.FLOAT_TYPE, convert("f_float", "float").getDataType());
    }

    @Test
    void testConvertDouble() {
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, convert("f_double", "double").getDataType());
    }

    @Test
    void testConvertDecimal() {
        Column column = convertDecimal("f_decimal", 10L, 2);
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
    }

    @Test
    void testConvertDecimalWithDefaults() {
        Column column = convertDecimal("f_decimal_default", null, null);
        Assertions.assertEquals(
                new DecimalType(
                        DuckDBTypeConverter.DEFAULT_PRECISION, DuckDBTypeConverter.DEFAULT_SCALE),
                column.getDataType());
        Assertions.assertEquals(DuckDBTypeConverter.DEFAULT_PRECISION, column.getColumnLength());
        Assertions.assertEquals(DuckDBTypeConverter.DEFAULT_SCALE, column.getScale());
    }

    @Test
    void testConvertDecimalTruncatesPrecisionAndScale() {
        Column column = convertDecimal("f_decimal_truncate", 50L, 50);
        Assertions.assertEquals(new DecimalType(38, 38), column.getDataType());
        Assertions.assertEquals(DuckDBTypeConverter.MAX_PRECISION, column.getColumnLength());
        Assertions.assertEquals(DuckDBTypeConverter.MAX_SCALE, column.getScale());
    }

    @Test
    void testConvertVarchar() {
        Column column = convert("f_varchar", "varchar", 200L);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(200L, column.getColumnLength());
    }

    @Test
    void testConvertText() {
        Column column = convert("f_text", "text");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertNull(column.getColumnLength());
    }

    @Test
    void testConvertChar() {
        Column column = convert("f_char", "char", 10L);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(10L, column.getColumnLength());
    }

    @Test
    void testConvertBpchar() {
        Column column = convert("f_bpchar", "bpchar", 5L);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(5L, column.getColumnLength());
    }

    @Test
    void testConvertStringAlias() {
        Column column = convert("f_string", "string");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    void testConvertBit() {
        Column column = convert("f_bit", "bit", 8L);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8L, column.getColumnLength());
    }

    @Test
    void testConvertBitUsesDefaultLengthWhenMissing() {
        Column column = convert("f_bit_default", "bit");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(1L, column.getColumnLength());
    }

    @Test
    void testConvertUuid() {
        Column column = convert("f_uuid", "uuid");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(255L, column.getColumnLength());
    }

    @Test
    void testConvertJson() {
        Column column = convert("f_json", "json");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(255L, column.getColumnLength());
    }

    @Test
    void testConvertBlob() {
        Column column = convert("f_blob", "blob", 128L);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(128L, column.getColumnLength());
    }

    @Test
    void testConvertDate() {
        Column column = convert("f_date", "date");
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
    }

    @Test
    void testConvertTime() {
        Column column = convert("f_time", "time");
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
    }

    @Test
    void testConvertTimestamp() {
        Column column = convert("f_timestamp", "timestamp");
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    void testConvertTimestampWithTimezone() {
        Column column = convert("f_timestamp_tz", "timestamp with time zone");
        // TIMESTAMP WITH TIME ZONE is LTZ → must map to OFFSET_DATE_TIME_TYPE
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    void testConvertInterval() {
        Column column = convert("f_interval", "interval");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(50L, column.getColumnLength());
    }

    @Test
    void testConvertArray() {
        Column column = convert("f_array", "array");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(65535L, column.getColumnLength());
    }

    @Test
    void testConvertStruct() {
        Column column = convert("f_struct", "struct");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(65535L, column.getColumnLength());
    }

    @Test
    void testConvertMap() {
        Column column = convert("f_map", "map");
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(65535L, column.getColumnLength());
    }

    @Test
    void testConvertUnsupportedTypeFallsBackToString() {
        Column column = convert("f_unknown", "geography", 64L);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(64L, column.getColumnLength());
    }

    @Test
    void testReconvertBoolean() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_boolean")
                                .dataType(BasicType.BOOLEAN_TYPE)
                                .nullable(false)
                                .defaultValue(false)
                                .comment("flag")
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(false, typeDefine.getDefaultValue());
        Assertions.assertEquals("flag", typeDefine.getComment());
    }

    @Test
    void testReconvertTinyint() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_tinyint")
                                .dataType(BasicType.BYTE_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TINYINT, typeDefine.getDataType());
    }

    @Test
    void testReconvertSmallint() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_smallint")
                                .dataType(BasicType.SHORT_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_SMALLINT, typeDefine.getDataType());
    }

    @Test
    void testReconvertInteger() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_integer")
                                .dataType(BasicType.INT_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_INTEGER, typeDefine.getDataType());
    }

    @Test
    void testReconvertBigint() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_bigint")
                                .dataType(BasicType.LONG_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BIGINT, typeDefine.getDataType());
    }

    @Test
    void testReconvertFloat() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_float")
                                .dataType(BasicType.FLOAT_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_FLOAT, typeDefine.getDataType());
    }

    @Test
    void testReconvertDouble() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_double")
                                .dataType(BasicType.DOUBLE_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DOUBLE, typeDefine.getDataType());
    }

    @Test
    void testReconvertDecimal() {
        DecimalType decimalType = new DecimalType(20, 4);
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_decimal")
                                .dataType(decimalType)
                                .columnLength(20L)
                                .scale(4)
                                .build());
        Assertions.assertEquals("DECIMAL(20,4)", typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DECIMAL, typeDefine.getDataType());
        Assertions.assertEquals(20L, typeDefine.getPrecision());
        Assertions.assertEquals(4, typeDefine.getScale());
    }

    @Test
    void testReconvertDecimalTruncatesPrecisionAndScale() {
        DecimalType decimalType = new DecimalType(50, 50);
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_decimal_large")
                                .dataType(decimalType)
                                .columnLength(50L)
                                .scale(50)
                                .build());
        Assertions.assertEquals("DECIMAL(38,38)", typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DECIMAL, typeDefine.getDataType());
        Assertions.assertEquals(DuckDBTypeConverter.MAX_PRECISION, typeDefine.getPrecision());
        Assertions.assertEquals(DuckDBTypeConverter.MAX_SCALE, typeDefine.getScale());
    }

    @Test
    void testReconvertString() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_string")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(128L)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_VARCHAR, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_VARCHAR, typeDefine.getDataType());
        Assertions.assertEquals(128L, typeDefine.getLength());
    }

    @Test
    void testReconvertBytes() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_bytes")
                                .dataType(PrimitiveByteArrayType.INSTANCE)
                                .columnLength(64L)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BLOB, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BLOB, typeDefine.getDataType());
        Assertions.assertEquals(64L, typeDefine.getLength());
    }

    @Test
    void testReconvertDate() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_date")
                                .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DATE, typeDefine.getDataType());
    }

    @Test
    void testReconvertTime() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_time")
                                .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIME, typeDefine.getDataType());
    }

    @Test
    void testReconvertTimestamp() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_timestamp")
                                .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIMESTAMP, typeDefine.getDataType());
    }

    @Test
    void testReconvertTimestampTz() {
        BasicTypeDefine<?> typeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_timestamp_tz")
                                .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                                .build());
        // OFFSET_DATE_TIME_TYPE → DUCKDB_TIMESTAMP_WITH_TZ
        Assertions.assertEquals(
                DuckDBTypeConverter.DUCKDB_TIMESTAMP_WITH_TZ, typeDefine.getColumnType());
        Assertions.assertEquals(
                DuckDBTypeConverter.DUCKDB_TIMESTAMP_WITH_TZ, typeDefine.getDataType());
    }

    @Test
    void testReconvertUnsupportedType() {
        Column mapColumn =
                PhysicalColumn.builder()
                        .name("f_map")
                        .dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE))
                        .build();
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> DuckDBTypeConverter.INSTANCE.reconvert(mapColumn));
    }

    private Column convert(String name, String dataType) {
        return DuckDBTypeConverter.INSTANCE.convert(
                BasicTypeDefine.builder()
                        .name(name)
                        .columnType(dataType)
                        .dataType(dataType)
                        .build());
    }

    private Column convert(String name, String dataType, Long length) {
        return DuckDBTypeConverter.INSTANCE.convert(
                BasicTypeDefine.builder()
                        .name(name)
                        .columnType(dataType)
                        .dataType(dataType)
                        .length(length)
                        .build());
    }

    private Column convertDecimal(String name, Long precision, Integer scale) {
        BasicTypeDefine.BasicTypeDefineBuilder<Object> builder =
                BasicTypeDefine.builder().name(name).columnType("decimal").dataType("decimal");
        if (precision != null) {
            builder.precision(precision);
        }
        if (scale != null) {
            builder.scale(scale);
        }
        return DuckDBTypeConverter.INSTANCE.convert(builder.build());
    }
}
