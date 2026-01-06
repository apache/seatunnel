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
    void testConvertIntegralTypes() {
        BasicTypeDefine<Object> booleanDefine =
                BasicTypeDefine.builder()
                        .name("f_boolean")
                        .columnType("boolean")
                        .dataType("boolean")
                        .nullable(true)
                        .defaultValue(true)
                        .comment("flag")
                        .build();
        Column booleanColumn = DuckDBTypeConverter.INSTANCE.convert(booleanDefine);
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, booleanColumn.getDataType());
        Assertions.assertEquals(booleanDefine.getName(), booleanColumn.getName());
        Assertions.assertEquals(booleanDefine.getDefaultValue(), booleanColumn.getDefaultValue());
        Assertions.assertEquals(booleanDefine.getComment(), booleanColumn.getComment());

        Assertions.assertEquals(
                BasicType.BYTE_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_tiny")
                                        .columnType("tinyint")
                                        .dataType("tinyint")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.BYTE_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_utiny")
                                        .columnType("utinyint")
                                        .dataType("utinyint")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.SHORT_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_small")
                                        .columnType("smallint")
                                        .dataType("smallint")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.SHORT_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_usmall")
                                        .columnType("usmallint")
                                        .dataType("usmallint")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_int")
                                        .columnType("integer")
                                        .dataType("integer")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.INT_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_uint")
                                        .columnType("uinteger")
                                        .dataType("uinteger")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_big")
                                        .columnType("bigint")
                                        .dataType("bigint")
                                        .build())
                        .getDataType());
        Assertions.assertEquals(
                BasicType.LONG_TYPE,
                DuckDBTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("f_ubig")
                                        .columnType("ubigint")
                                        .dataType("ubigint")
                                        .build())
                        .getDataType());
    }

    @Test
    void testConvertFloatingAndDecimalTypes() {
        Column floatColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_float")
                                .columnType("float")
                                .dataType("float")
                                .build());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, floatColumn.getDataType());

        Column doubleColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_double")
                                .columnType("double")
                                .dataType("double")
                                .build());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, doubleColumn.getDataType());

        Column decimalColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_decimal")
                                .columnType("decimal(10,2)")
                                .dataType("decimal")
                                .precision(10L)
                                .scale(2)
                                .build());
        Assertions.assertEquals(new DecimalType(10, 2), decimalColumn.getDataType());
        Assertions.assertEquals(10L, decimalColumn.getColumnLength());
        Assertions.assertEquals(2, decimalColumn.getScale());

        Column defaultDecimal =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_decimal_default")
                                .columnType("decimal")
                                .dataType("decimal")
                                .build());
        Assertions.assertEquals(new DecimalType(38, 0), defaultDecimal.getDataType());
        Assertions.assertEquals(
                DuckDBTypeConverter.DEFAULT_PRECISION, defaultDecimal.getColumnLength());
        Assertions.assertEquals(DuckDBTypeConverter.DEFAULT_SCALE, defaultDecimal.getScale());

        Column truncatedDecimal =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_decimal_large")
                                .columnType("decimal(50,50)")
                                .dataType("decimal")
                                .precision(50L)
                                .scale(50)
                                .build());
        Assertions.assertEquals(new DecimalType(38, 38), truncatedDecimal.getDataType());
        Assertions.assertEquals(
                DuckDBTypeConverter.MAX_PRECISION, truncatedDecimal.getColumnLength());
        Assertions.assertEquals(DuckDBTypeConverter.MAX_SCALE, truncatedDecimal.getScale());
    }

    @Test
    void testConvertStringBinaryAndSpecialTypes() {
        Column varcharColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_varchar")
                                .columnType("varchar")
                                .dataType("varchar")
                                .length(200L)
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, varcharColumn.getDataType());
        Assertions.assertEquals(200L, varcharColumn.getColumnLength());

        Column textColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_text")
                                .columnType("text")
                                .dataType("text")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, textColumn.getDataType());
        Assertions.assertNull(textColumn.getColumnLength());

        Column jsonColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_json")
                                .columnType("json")
                                .dataType("json")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, jsonColumn.getDataType());
        Assertions.assertEquals(255L, jsonColumn.getColumnLength());

        Column uuidColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_uuid")
                                .columnType("uuid")
                                .dataType("uuid")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, uuidColumn.getDataType());
        Assertions.assertEquals(255L, uuidColumn.getColumnLength());

        Column blobColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_blob")
                                .columnType("blob")
                                .dataType("blob")
                                .length(128L)
                                .build());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, blobColumn.getDataType());
        Assertions.assertEquals(128L, blobColumn.getColumnLength());

        Column intervalColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_interval")
                                .columnType("interval")
                                .dataType("interval")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, intervalColumn.getDataType());
        Assertions.assertEquals(50L, intervalColumn.getColumnLength());

        Column hugeintColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_hugeint")
                                .columnType("hugeint")
                                .dataType("hugeint")
                                .build());
        Assertions.assertEquals(new DecimalType(38, 0), hugeintColumn.getDataType());
        Assertions.assertEquals(38L, hugeintColumn.getColumnLength());

        Column arrayFallback =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_array")
                                .columnType("integer[]")
                                .dataType("array")
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, arrayFallback.getDataType());
        Assertions.assertEquals(65535L, arrayFallback.getColumnLength());

        Column unsupportedFallback =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_unknown")
                                .columnType("geography")
                                .dataType("geography")
                                .length(64L)
                                .build());
        Assertions.assertEquals(BasicType.STRING_TYPE, unsupportedFallback.getDataType());
        Assertions.assertEquals(64L, unsupportedFallback.getColumnLength());
    }

    @Test
    void testConvertTemporalTypes() {
        Column dateColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_date")
                                .columnType("date")
                                .dataType("date")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, dateColumn.getDataType());

        Column timeColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_time")
                                .columnType("time")
                                .dataType("time")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, timeColumn.getDataType());

        Column timestampColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_timestamp")
                                .columnType("timestamp")
                                .dataType("timestamp")
                                .build());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampColumn.getDataType());

        Column timestampTzColumn =
                DuckDBTypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder()
                                .name("f_timestamp_tz")
                                .columnType("timestamp with time zone")
                                .dataType("timestamp with time zone")
                                .build());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE, timestampTzColumn.getDataType());
    }

    @Test
    void testReconvertSupportedTypes() {
        Column booleanColumn =
                PhysicalColumn.builder()
                        .name("f_boolean")
                        .dataType(BasicType.BOOLEAN_TYPE)
                        .nullable(false)
                        .defaultValue(false)
                        .comment("flag")
                        .build();
        BasicTypeDefine<?> booleanDefine = DuckDBTypeConverter.INSTANCE.reconvert(booleanColumn);
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BOOLEAN, booleanDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BOOLEAN, booleanDefine.getDataType());
        Assertions.assertEquals(booleanColumn.getDefaultValue(), booleanDefine.getDefaultValue());
        Assertions.assertEquals(booleanColumn.getComment(), booleanDefine.getComment());

        BasicTypeDefine<?> intDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_int")
                                .dataType(BasicType.INT_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_INTEGER, intDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_INTEGER, intDefine.getDataType());

        DecimalType decimalType = new DecimalType(20, 4);
        BasicTypeDefine<?> decimalDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_decimal")
                                .dataType(decimalType)
                                .columnLength(20L)
                                .scale(4)
                                .build());
        Assertions.assertEquals(
                String.format("%s(%d,%d)", DuckDBTypeConverter.DUCKDB_DECIMAL, 20, 4),
                decimalDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DECIMAL, decimalDefine.getDataType());
        Assertions.assertEquals(20L, decimalDefine.getPrecision());
        Assertions.assertEquals(4, decimalDefine.getScale());

        BasicTypeDefine<?> stringDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_string")
                                .dataType(BasicType.STRING_TYPE)
                                .columnLength(128L)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_VARCHAR, stringDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_VARCHAR, stringDefine.getDataType());
        Assertions.assertEquals(128L, stringDefine.getLength());

        BasicTypeDefine<?> bytesDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_bytes")
                                .dataType(PrimitiveByteArrayType.INSTANCE)
                                .columnLength(64L)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BLOB, bytesDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_BLOB, bytesDefine.getDataType());
        Assertions.assertEquals(64L, bytesDefine.getLength());
    }

    @Test
    void testReconvertTemporalTypes() {
        BasicTypeDefine<?> dateDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_date")
                                .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DATE, dateDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_DATE, dateDefine.getDataType());

        BasicTypeDefine<?> timeDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_time")
                                .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                .build());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIME, timeDefine.getColumnType());
        Assertions.assertEquals(DuckDBTypeConverter.DUCKDB_TIME, timeDefine.getDataType());

        BasicTypeDefine<?> timestampDefine =
                DuckDBTypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("f_timestamp")
                                .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                                .build());
        Assertions.assertEquals(
                DuckDBTypeConverter.DUCKDB_TIMESTAMP, timestampDefine.getColumnType());
        Assertions.assertEquals(
                DuckDBTypeConverter.DUCKDB_TIMESTAMP, timestampDefine.getDataType());
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
}
