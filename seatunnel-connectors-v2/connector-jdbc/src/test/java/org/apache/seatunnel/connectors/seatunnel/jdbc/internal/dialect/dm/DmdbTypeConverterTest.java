/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

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

public class DmdbTypeConverterTest {
    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            DmdbTypeConverter.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
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
            DmdbTypeConverter.INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertBit() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("bit").dataType("bit").build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint")
                        .dataType("tinyint")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("byte").dataType("byte").build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallint")
                        .dataType("smallint")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int").dataType("int").build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("integer")
                        .dataType("integer")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("pls_integer")
                        .dataType("pls_integer")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint")
                        .dataType("bigint")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertReal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("real").dataType("real").build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("double")
                        .dataType("double")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("double precision")
                        .dataType("double precision")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal")
                        .dataType("decimal")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(38, column.getColumnLength());
        Assertions.assertEquals(18, column.getScale());
        Assertions.assertEquals("DECIMAL(38,18)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal(10,2)")
                        .dataType("decimal")
                        .precision(10L)
                        .scale(2)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(
                String.format("DECIMAL(%s,%s)", typeDefine.getPrecision(), typeDefine.getScale()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("numeric(10,2)")
                        .dataType("numeric")
                        .precision(10L)
                        .scale(2)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(
                String.format("DECIMAL(%s,%s)", typeDefine.getPrecision(), typeDefine.getScale()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("number(10,2)")
                        .dataType("number")
                        .precision(10L)
                        .scale(2)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(
                String.format("DECIMAL(%s,%s)", typeDefine.getPrecision(), typeDefine.getScale()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("dec(10,2)")
                        .dataType("dec")
                        .precision(10L)
                        .scale(2)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 2), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(
                String.format("DECIMAL(%s,%s)", typeDefine.getPrecision(), typeDefine.getScale()),
                column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("char(2)")
                        .dataType("char")
                        .length(2L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("character(2)")
                        .dataType("character")
                        .length(2L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(
                String.format("char(%s)", typeDefine.getLength()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar(2)")
                        .dataType("varchar")
                        .length(2L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(
                String.format("varchar2(%s)", typeDefine.getLength()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar2(2)")
                        .dataType("varchar2")
                        .length(2L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testNvarchar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nvarchar(2)")
                        .dataType("nvarchar")
                        .length(2L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertText() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("text")
                        .dataType("text")
                        .length(2147483647L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("long")
                        .dataType("long")
                        .length(2147483647L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("longvarchar")
                        .dataType("longvarchar")
                        .length(2147483647L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("clob")
                        .dataType("clob")
                        .length(2147483647L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBinary() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("binary(1)")
                        .dataType("binary")
                        .length(1L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(1L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varbinary(1)")
                        .dataType("varbinary")
                        .length(1L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(1L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("longvarbinary")
                        .dataType("longvarbinary")
                        .length(2147483647L)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(2147483647L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBlob() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("blob")
                        .dataType("blob")
                        .length(2147483647L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(2147483647L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertImage() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("image")
                        .dataType("image")
                        .length(2147483647L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(2147483647L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBfile() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bfile")
                        .dataType("bfile")
                        .length(2147483647L)
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2147483647L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("time").dataType("time").build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("time(3)")
                        .dataType("time")
                        .scale(3)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("time with time zone")
                        .dataType("time with time zone")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("time(3) with time zone")
                        .dataType("time with time zone")
                        .scale(3)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime")
                        .dataType("datetime")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime(3)")
                        .dataType("datetime")
                        .scale(3)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime with time zone")
                        .dataType("datetime with time zone")
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime(3) with time zone")
                        .dataType("datetime with time zone")
                        .scale(3)
                        .build();
        column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTimestamp() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp")
                        .dataType("timestamp")
                        .build();
        Column column = DmdbTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_BIT, typeDefine.getColumnType());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_INT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_REAL, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_REAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        DmdbTypeConverter.DM_DECIMAL,
                        DmdbTypeConverter.DEFAULT_PRECISION,
                        DmdbTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", DmdbTypeConverter.DM_DECIMAL, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_DECIMAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_LONGVARBINARY, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_LONGVARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(255L)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DmdbTypeConverter.DM_VARBINARY, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(1901L)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_LONGVARBINARY, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_LONGVARBINARY, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(1900L)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DmdbTypeConverter.DM_VARCHAR2, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_VARCHAR2, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(1901L)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TEXT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DmdbTypeConverter.DM_TIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIMESTAMP, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DmdbTypeConverter.DM_TIMESTAMP, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DmdbTypeConverter.DM_TIMESTAMP, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }
}
