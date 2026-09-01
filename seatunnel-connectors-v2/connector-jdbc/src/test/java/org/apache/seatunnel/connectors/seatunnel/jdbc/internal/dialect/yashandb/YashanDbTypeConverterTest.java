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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.BYTES_4GB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_JSON_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_PRECISION;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_RAW_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_ROWID_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_SCALE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_TIMESTAMP_SCALE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_UROWID_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter.MAX_VARCHAR_LENGTH;

public class YashanDbTypeConverterTest {

    private static final YashanDbTypeConverter INSTANCE = new YashanDbTypeConverter();

    // ============================ convert() tests ============================

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // expected
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertIntegerTypes() {
        // TINYINT -> BYTE_TYPE
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TINYINT")
                        .dataType("TINYINT")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals("test", column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());

        // SMALLINT -> SHORT_TYPE
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SMALLINT")
                        .dataType("SMALLINT")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());

        // INT -> INT_TYPE
        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("INT").dataType("INT").build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());

        // INTEGER -> INT_TYPE
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTEGER")
                        .dataType("INTEGER")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());

        // BIGINT -> LONG_TYPE
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }

    @Test
    public void testConvertNumber() {
        // NUMBER without precision -> DecimalType(38, 18)
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMBER")
                        .dataType("NUMBER")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
        Assertions.assertEquals(18, column.getScale());

        // NUMBER(10, 2) -> DecimalType(10, 2)
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMBER(10,2)")
                        .dataType("NUMBER")
                        .precision(10L)
                        .scale(2)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());

        // NUMBER(38, 0) -> DecimalType(38, 0)
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMBER(38,0)")
                        .dataType("NUMBER")
                        .precision(38L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());

        // NUMBER with precision > 38 should still use provided precision (convert does not cap)
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMBER(39,0)")
                        .dataType("NUMBER")
                        .precision(39L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(new DecimalType(39, 0), column.getDataType());
    }

    @Test
    public void testConvertFloatDouble() {
        // FLOAT -> FLOAT_TYPE
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("FLOAT")
                        .dataType("FLOAT")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());

        // DOUBLE -> DOUBLE_TYPE
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DOUBLE")
                        .dataType("DOUBLE")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BOOLEAN")
                        .dataType("BOOLEAN")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
    }

    @Test
    public void testConvertBit() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("BIT").dataType("BIT").build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }

    @Test
    public void testConvertCharTypes() {
        // CHAR(10) -> STRING with columnLength = 10 * 4
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHAR(10)")
                        .dataType("CHAR")
                        .length(10L)
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(40L, column.getColumnLength());

        // VARCHAR(100) -> STRING with columnLength = 100 * 4
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARCHAR(100)")
                        .dataType("VARCHAR")
                        .length(100L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(400L, column.getColumnLength());

        // NCHAR(10) -> STRING with columnLength = 10 * 2
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NCHAR(10)")
                        .dataType("NCHAR")
                        .length(10L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(20L, column.getColumnLength());

        // NVARCHAR(100) -> STRING with columnLength = 100 * 2
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NVARCHAR(100)")
                        .dataType("NVARCHAR")
                        .length(100L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(200L, column.getColumnLength());

        // VARCHAR2(100) -> STRING with columnLength = 100 * 4 (byte-length semantics, same as
        // VARCHAR)
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARCHAR2(100)")
                        .dataType("VARCHAR2")
                        .length(100L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(400L, column.getColumnLength());

        // NVARCHAR2(100) -> STRING with columnLength = 100 * 2 (char-length semantics, same as
        // NVARCHAR)
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NVARCHAR2(100)")
                        .dataType("NVARCHAR2")
                        .length(100L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(200L, column.getColumnLength());
    }

    @Test
    public void testConvertRowidTypes() {
        // ROWID -> STRING with columnLength = 18
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("ROWID")
                        .dataType("ROWID")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(MAX_ROWID_LENGTH, column.getColumnLength());

        // UROWID without length -> STRING with columnLength = MAX_UROWID_LENGTH
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("UROWID")
                        .dataType("UROWID")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(MAX_UROWID_LENGTH, column.getColumnLength());

        // UROWID(2000) -> STRING with columnLength = 2000
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("UROWID(2000)")
                        .dataType("UROWID")
                        .length(2000L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2000L, column.getColumnLength());
    }

    @Test
    public void testConvertLobTypes() {
        // CLOB -> STRING with columnLength = 4GB-1
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("CLOB").dataType("CLOB").build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(BYTES_4GB - 1, column.getColumnLength());

        // NCLOB -> STRING with columnLength = 4GB-1
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NCLOB")
                        .dataType("NCLOB")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(BYTES_4GB - 1, column.getColumnLength());

        // BLOB -> BYTE_ARRAY
        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("BLOB").dataType("BLOB").build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());

        // RAW without length -> BYTE_ARRAY with columnLength = MAX_RAW_LENGTH
        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("RAW").dataType("RAW").build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(MAX_RAW_LENGTH, column.getColumnLength());

        // RAW(100) -> BYTE_ARRAY with columnLength = 100
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("RAW(100)")
                        .dataType("RAW")
                        .length(100L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(100L, column.getColumnLength());
    }

    @Test
    public void testConvertDateTimeTypes() {
        // DATE -> LOCAL_DATE_TIME
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("DATE").dataType("DATE").build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());

        // TIME -> LOCAL_TIME
        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("TIME").dataType("TIME").build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());

        // TIMESTAMP -> LOCAL_DATE_TIME with default scale 6
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP")
                        .dataType("TIMESTAMP")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(6, column.getScale());

        // TIMESTAMP(3) -> LOCAL_DATE_TIME with scale 3
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP(3)")
                        .dataType("TIMESTAMP")
                        .scale(3)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());

        // TIMESTAMP WITH TIME ZONE -> LOCAL_DATE_TIME
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP WITH TIME ZONE")
                        .dataType("TIMESTAMP WITH TIME ZONE")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());

        // TIMESTAMP WITH LOCAL TIME ZONE -> LOCAL_DATE_TIME
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP WITH LOCAL TIME ZONE")
                        .dataType("TIMESTAMP WITH LOCAL TIME ZONE")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    public void testConvertIntervalTypes() {
        // INTERVAL YEAR TO MONTH -> STRING
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTERVAL YEAR TO MONTH")
                        .dataType("INTERVAL YEAR TO MONTH")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());

        // INTERVAL DAY TO SECOND -> STRING
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTERVAL DAY TO SECOND")
                        .dataType("INTERVAL DAY TO SECOND")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    @Test
    public void testConvertJsonType() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("JSON").dataType("JSON").build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals((long) MAX_JSON_LENGTH, column.getColumnLength());
    }

    @Test
    public void testConvertXmlType() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("XMLTYPE")
                        .dataType("XMLTYPE")
                        .length(1000L)
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(1000L, column.getColumnLength());
    }

    @Test
    public void testConvertVectorType() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VECTOR(3)")
                        .dataType("VECTOR")
                        .scale(3)
                        .length(3L)
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VECTOR(3,FLOAT64)")
                        .dataType("VECTOR")
                        .scale(3)
                        .length(3L)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());
    }

    // ============================ reconvert() tests ============================

    @Test
    public void testReconvertUnsupported() {
        Column column =
                PhysicalColumn.of(
                        "test",
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        (Long) null,
                        true,
                        null,
                        null);
        try {
            INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.of("test", BasicType.BOOLEAN_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("BOOLEAN", typeDefine.getColumnType());
        Assertions.assertEquals("BOOLEAN", typeDefine.getDataType());
    }

    @Test
    public void testReconvertByteTypes() {
        // TINYINT
        Column column =
                PhysicalColumn.of("test", BasicType.BYTE_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TINYINT", typeDefine.getColumnType());
        Assertions.assertEquals("TINYINT", typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.of("test", BasicType.SHORT_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("SMALLINT", typeDefine.getColumnType());
        Assertions.assertEquals("SMALLINT", typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column =
                PhysicalColumn.of("test", BasicType.INT_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("INT", typeDefine.getColumnType());
        Assertions.assertEquals("INT", typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column =
                PhysicalColumn.of("test", BasicType.LONG_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("BIGINT", typeDefine.getColumnType());
        Assertions.assertEquals("BIGINT", typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.of("test", BasicType.FLOAT_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("FLOAT", typeDefine.getColumnType());
        Assertions.assertEquals("FLOAT", typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.of("test", BasicType.DOUBLE_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("DOUBLE", typeDefine.getColumnType());
        Assertions.assertEquals("DOUBLE", typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        // Default decimal(38, 18)
        Column column =
                PhysicalColumn.of("test", new DecimalType(38, 18), (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("NUMBER(38,18)", typeDefine.getColumnType());
        Assertions.assertEquals("NUMBER", typeDefine.getDataType());
        Assertions.assertEquals(38L, typeDefine.getPrecision());
        Assertions.assertEquals(18, typeDefine.getScale());

        // Specific decimal(10, 2)
        column = PhysicalColumn.of("test", new DecimalType(10, 2), (Long) null, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("NUMBER(10,2)", typeDefine.getColumnType());
        Assertions.assertEquals(10L, typeDefine.getPrecision());
        Assertions.assertEquals(2, typeDefine.getScale());

        // Decimal with precision > MAX_PRECISION -> capped at 38
        column = PhysicalColumn.of("test", new DecimalType(50, 20), (Long) null, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(MAX_PRECISION, typeDefine.getPrecision());

        // Decimal with negative precision -> uses default
        column = PhysicalColumn.of("test", new DecimalType(-1, 2), (Long) null, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(38L, typeDefine.getPrecision());
        Assertions.assertEquals(18, typeDefine.getScale());

        // Decimal with negative scale -> capped at 0
        column = PhysicalColumn.of("test", new DecimalType(10, -5), (Long) null, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(10L, typeDefine.getPrecision());
        Assertions.assertEquals(0, typeDefine.getScale());

        // Decimal with scale > MAX_SCALE -> capped at MAX_SCALE
        column = PhysicalColumn.of("test", new DecimalType(10, 200), (Long) null, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(MAX_SCALE, typeDefine.getScale());
    }

    @Test
    public void testReconvertBytes() {
        // BYTES -> BLOB always
        Column column =
                PhysicalColumn.of(
                        "test", PrimitiveByteArrayType.INSTANCE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("BLOB", typeDefine.getColumnType());
        Assertions.assertEquals("BLOB", typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        // Null length -> VARCHAR(MAX_VARCHAR_LENGTH)
        Column column =
                PhysicalColumn.of("test", BasicType.STRING_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(
                String.format("VARCHAR(%s)", MAX_VARCHAR_LENGTH), typeDefine.getColumnType());

        // Length 0 -> VARCHAR(MAX_VARCHAR_LENGTH)
        column = PhysicalColumn.of("test", BasicType.STRING_TYPE, 0L, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(
                String.format("VARCHAR(%s)", MAX_VARCHAR_LENGTH), typeDefine.getColumnType());

        // Length within VARCHAR range
        column = PhysicalColumn.of("test", BasicType.STRING_TYPE, 1000L, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("VARCHAR(1000)", typeDefine.getColumnType());

        // Length at MAX_VARCHAR_LENGTH boundary
        column =
                PhysicalColumn.of(
                        "test", BasicType.STRING_TYPE, MAX_VARCHAR_LENGTH, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(
                String.format("VARCHAR(%s)", MAX_VARCHAR_LENGTH), typeDefine.getColumnType());

        // Length exceeding MAX_VARCHAR_LENGTH -> CLOB
        column =
                PhysicalColumn.of(
                        "test", BasicType.STRING_TYPE, MAX_VARCHAR_LENGTH + 1, true, null, null);
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("CLOB", typeDefine.getColumnType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.of(
                        "test", LocalTimeType.LOCAL_DATE_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("DATE", typeDefine.getColumnType());
        Assertions.assertEquals("DATE", typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.of(
                        "test", LocalTimeType.LOCAL_TIME_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TIME", typeDefine.getColumnType());
        Assertions.assertEquals("TIME", typeDefine.getDataType());
    }

    @Test
    public void testReconvertTimestamp() {
        // TIMESTAMP without scale -> TIMESTAMP
        Column column =
                PhysicalColumn.of(
                        "test", LocalTimeType.LOCAL_DATE_TIME_TYPE, (Long) null, true, null, null);
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TIMESTAMP", typeDefine.getColumnType());
        Assertions.assertEquals("TIMESTAMP", typeDefine.getDataType());

        // TIMESTAMP(3) -> TIMESTAMP(3)
        column =
                PhysicalColumn.of("test", LocalTimeType.LOCAL_DATE_TIME_TYPE, 3L, true, null, null);
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .nullable(true)
                        .build();
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TIMESTAMP(3)", typeDefine.getColumnType());
        Assertions.assertEquals(3, typeDefine.getScale());

        // TIMESTAMP with scale > MAX_TIMESTAMP_SCALE -> capped
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(MAX_TIMESTAMP_SCALE + 5)
                        .nullable(true)
                        .build();
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(
                String.format("TIMESTAMP(%s)", MAX_TIMESTAMP_SCALE), typeDefine.getColumnType());
        Assertions.assertEquals(MAX_TIMESTAMP_SCALE, typeDefine.getScale());
    }

    @Test
    public void testReconvertTimestampTz() {
        // TIMESTAMP_TZ without scale -> TIMESTAMP WITH TIME ZONE
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .nullable(true)
                        .build();
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TIMESTAMP WITH TIME ZONE", typeDefine.getColumnType());

        // TIMESTAMP_TZ(3) -> TIMESTAMP(3) WITH TIME ZONE
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .scale(3)
                        .nullable(true)
                        .build();
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("TIMESTAMP(3) WITH TIME ZONE", typeDefine.getColumnType());
        Assertions.assertEquals(3, typeDefine.getScale());
    }

    @Test
    public void testReconvertVectorTypes() {
        // FLOAT_VECTOR -> VECTOR(n)
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                        .scale(3)
                        .nullable(true)
                        .build();
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("VECTOR(3)", typeDefine.getColumnType());
        Assertions.assertEquals("VECTOR", typeDefine.getDataType());
    }

    @Test
    public void testReconvertArrayToVector() {
        // ARRAY<INT> -> VECTOR FLOAT32
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.INT_ARRAY_TYPE)
                        .columnLength(10L)
                        .nullable(true)
                        .build();
        BasicTypeDefine<?> typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("VECTOR(10,FLOAT32)", typeDefine.getColumnType());
        Assertions.assertEquals("VECTOR", typeDefine.getDataType());

        // ARRAY<DOUBLE> -> VECTOR FLOAT64
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.DOUBLE_ARRAY_TYPE)
                        .columnLength(10L)
                        .nullable(true)
                        .build();
        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals("VECTOR(10,FLOAT64)", typeDefine.getColumnType());
        Assertions.assertEquals("VECTOR", typeDefine.getDataType());

        // ARRAY<STRING> -> unsupported
        Column stringArrayColumn =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                        .columnLength(10L)
                        .nullable(true)
                        .build();
        try {
            INSTANCE.reconvert(stringArrayColumn);
            Assertions.fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testConvertCaseInsensitive() {
        // lowercase type names should work
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar")
                        .dataType("varchar")
                        .length(50L)
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint")
                        .dataType("bigint")
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
    }
}
