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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

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

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.BYTES_2GB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.BYTES_4GB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.MAX_RAW_LENGTH;

public class OracleTypeConverterTest {

    private static final OracleTypeConverter INSTANCE = new OracleTypeConverter();

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertNumberWithoutDecimalTypeNarrowing() {
        OracleTypeConverter converter = new OracleTypeConverter(false);

        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number")
                        .dataType("number")
                        .build();
        Column column = converter.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(38,127)")
                        .dataType("number")
                        .precision(38L)
                        .scale(127)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number")
                        .dataType("number")
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(1,0)")
                        .dataType("number")
                        .precision(1L)
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(1, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(8,0)")
                        .dataType("number")
                        .precision(8L)
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(8, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(18,0)")
                        .dataType("number")
                        .precision(18L)
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(18, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(38,0)")
                        .dataType("number")
                        .precision(38L)
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(39,0)")
                        .dataType("number")
                        .precision(39L)
                        .scale(0)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInteger() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("integer")
                        .dataType("integer")
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        // generated by int/smallint type in oracle create table sql
        BasicTypeDefine<Object> numberTypeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number")
                        .dataType("number")
                        .precision(null)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(numberTypeDefine);
        Assertions.assertEquals(numberTypeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(numberTypeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertNumber() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number")
                        .dataType("number")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(38,127)")
                        .dataType("number")
                        .precision(38L)
                        .scale(127)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number")
                        .dataType("number")
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(1,0)")
                        .dataType("number")
                        .precision(1L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(8,0)")
                        .dataType("number")
                        .precision(8L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(18,0)")
                        .dataType("number")
                        .precision(18L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(38,0)")
                        .dataType("number")
                        .precision(38L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(39,0)")
                        .dataType("number")
                        .precision(39L)
                        .scale(0)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("binary_float")
                        .dataType("binary_float")
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("real").dataType("real").build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("binary_double")
                        .dataType("binary_double")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("char(1)")
                        .dataType("char")
                        .length(1L)
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength() * 4, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nchar(1)")
                        .dataType("nchar")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar(1)")
                        .dataType("varchar")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength() * 4, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar2(1)")
                        .dataType("varchar2")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength() * 4, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nvarchar2(1)")
                        .dataType("nvarchar2")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("rowid")
                        .dataType("rowid")
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(18, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("xmltype")
                        .dataType("xmltype")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("sys.xmltype")
                        .dataType("sys.xmltype")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("long")
                        .dataType("long")
                        .length(1L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(BYTES_2GB - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("clob").dataType("clob").build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(BYTES_4GB - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nclob")
                        .dataType("nclob")
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(BYTES_4GB - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBytes() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("blob").dataType("blob").build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(BYTES_4GB - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("raw").dataType("raw").build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(MAX_RAW_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("raw(10)")
                        .dataType("raw")
                        .length(10L)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("long raw")
                        .dataType("long raw")
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(BYTES_2GB - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertNull(column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp(6)")
                        .dataType("timestamp")
                        .scale(6)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(6, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp(6) with time zone")
                        .dataType("timestamp with time zone")
                        .scale(6)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(6, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp(6) with local time zone")
                        .dataType("timestamp with local time zone")
                        .scale(6)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(6, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

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
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", OracleTypeConverter.ORACLE_NUMBER, 1),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_NUMBER, typeDefine.getDataType());
        Assertions.assertEquals(1, typeDefine.getLength());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                OracleTypeConverter.ORACLE_BINARY_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BINARY_FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                OracleTypeConverter.ORACLE_BINARY_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BINARY_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        OracleTypeConverter.ORACLE_NUMBER,
                        OracleTypeConverter.DEFAULT_PRECISION,
                        OracleTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_NUMBER, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", OracleTypeConverter.ORACLE_NUMBER, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_NUMBER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(2000L)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", OracleTypeConverter.ORACLE_RAW, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_RAW, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(BYTES_2GB)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(BYTES_2GB + 1)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_BLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("VARCHAR2(4000)", typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_VARCHAR2, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(2000L)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", OracleTypeConverter.ORACLE_VARCHAR2, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_VARCHAR2, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(4000L)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", OracleTypeConverter.ORACLE_VARCHAR2, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_VARCHAR2, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(40001L)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_CLOB, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_CLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(OracleTypeConverter.ORACLE_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                OracleTypeConverter.ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                typeDefine.getColumnType());
        Assertions.assertEquals(
                OracleTypeConverter.ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("TIMESTAMP(%s) WITH LOCAL TIME ZONE", column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                OracleTypeConverter.ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testNumberWithNegativeScale() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(38,-1)")
                        .dataType("number")
                        .precision(38L)
                        .scale(-1)
                        .build();
        Column column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(5,-2)")
                        .dataType("number")
                        .precision(5L)
                        .scale(-2)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(9,-2)")
                        .dataType("number")
                        .precision(9L)
                        .scale(-2)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(14,-11)")
                        .dataType("number")
                        .precision(14L)
                        .scale(-11)
                        .build();
        column = INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(25, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }
}
