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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

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

public class SqlServerTypeConverterTest {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            SqlServerTypeConverter.INSTANCE.convert(typeDefine);
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
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bit")
                        .dataType("bit")
                        .nullable(true)
                        .defaultValue("1")
                        .comment("test")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint")
                        .dataType("tinyint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTinyintIdentity() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint identity")
                        .dataType("tinyint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_TINYINT, column.getSourceType());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallint")
                        .dataType("smallint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertSmallintIdentity() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallint identity")
                        .dataType("smallint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_SMALLINT, column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int").dataType("int").build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("integer")
                        .dataType("integer")
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals("int", column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBigintIdentity() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint identity")
                        .dataType("bigint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_BIGINT, column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint")
                        .dataType("bigint")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("real").dataType("real").build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .precision(24L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_REAL, column.getSourceType().toUpperCase());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .precision(25L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
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
                        .precision(38L)
                        .scale(2)
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(38, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals("DECIMAL(38,2)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("numeric")
                        .dataType("numeric")
                        .precision(38L)
                        .scale(2)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(38, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals("DECIMAL(38,2)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("money")
                        .dataType("money")
                        .precision(19L)
                        .scale(4)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(19, 4), column.getDataType());
        Assertions.assertEquals(19, column.getColumnLength());
        Assertions.assertEquals(4, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallmoney")
                        .dataType("smallmoney")
                        .precision(10L)
                        .scale(4)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(10, 4), column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(4, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("char")
                        .dataType("char")
                        .length(2L)
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("CHAR(2)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nchar")
                        .dataType("nchar")
                        .length(2L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals("NCHAR(2)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar")
                        .dataType("varchar")
                        .length(-1L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                (SqlServerTypeConverter.POWER_2_31 - 1) * 2, column.getColumnLength());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_VARCHAR, column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar")
                        .dataType("varchar")
                        .length(10L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(20, column.getColumnLength());
        Assertions.assertEquals("VARCHAR(10)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nvarchar")
                        .dataType("nvarchar")
                        .length(-1L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                (SqlServerTypeConverter.POWER_2_31 - 1) * 2, column.getColumnLength());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_NVARCHAR, column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("nvarchar")
                        .dataType("nvarchar")
                        .length(10L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(20, column.getColumnLength());
        Assertions.assertEquals("NVARCHAR(10)", column.getSourceType());
    }

    @Test
    public void testConvertText() {
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder().name("test").columnType("text").dataType("text").build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.POWER_2_31 - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("ntext")
                        .dataType("ntext")
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.POWER_2_30 - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertXml() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("xml").dataType("xml").build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.POWER_2_31 - 1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBinary() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("binary")
                        .dataType("binary")
                        .length(1L)
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(1, column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", typeDefine.getDataType(), typeDefine.getLength()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varbinary")
                        .dataType("varbinary")
                        .length(-1L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.POWER_2_31 - 1, column.getColumnLength());
        Assertions.assertEquals("VARBINARY(MAX)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varbinary")
                        .dataType("varbinary")
                        .length(10L)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", typeDefine.getDataType(), typeDefine.getLength()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("image")
                        .dataType("image")
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(SqlServerTypeConverter.POWER_2_31 - 1, column.getColumnLength());
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
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("time")
                        .dataType("time")
                        .scale(3)
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(
                String.format("%s(%s)", typeDefine.getDataType(), typeDefine.getScale()),
                column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime")
                        .dataType("datetime")
                        .build();
        Column column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime2")
                        .dataType("datetime2")
                        .scale(3)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(
                String.format("%s(%s)", typeDefine.getDataType(), typeDefine.getScale()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetimeoffset")
                        .dataType("datetimeoffset")
                        .scale(3)
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(
                String.format("%s(%s)", typeDefine.getDataType(), typeDefine.getScale()),
                column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smalldatetime")
                        .dataType("smalldatetime")
                        .build();
        column = SqlServerTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
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
            SqlServerTypeConverter.INSTANCE.reconvert(column);
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
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.BOOLEAN_TYPE)
                        .nullable(true)
                        .defaultValue(true)
                        .comment("test")
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_BIT, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_BIT, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_INT, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_REAL, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_REAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        SqlServerTypeConverter.SQLSERVER_DECIMAL,
                        SqlServerTypeConverter.DEFAULT_PRECISION,
                        SqlServerTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", SqlServerTypeConverter.SQLSERVER_DECIMAL, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_DECIMAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_VARBINARY, typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(8000L)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        SqlServerTypeConverter.SQLSERVER_VARBINARY, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(8001L)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_VARBINARY, typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_VARBINARY, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_NVARCHAR, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_NVARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(4000L)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        SqlServerTypeConverter.SQLSERVER_NVARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_NVARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(4001L)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_NVARCHAR, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.MAX_NVARCHAR, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_TIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SqlServerTypeConverter.SQLSERVER_TIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(SqlServerTypeConverter.SQLSERVER_TIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_DATETIME2, typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_DATETIME2, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", SqlServerTypeConverter.SQLSERVER_DATETIME2, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_DATETIME2, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(9)
                        .build();

        typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SqlServerTypeConverter.SQLSERVER_DATETIME2, 7),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                SqlServerTypeConverter.SQLSERVER_DATETIME2, typeDefine.getDataType());
        Assertions.assertEquals(7, typeDefine.getScale());
    }
}
