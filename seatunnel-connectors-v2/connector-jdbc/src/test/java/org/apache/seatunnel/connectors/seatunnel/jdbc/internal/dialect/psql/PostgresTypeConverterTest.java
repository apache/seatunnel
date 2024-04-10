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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostgresTypeConverterTest {
    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            PostgresTypeConverter.INSTANCE.convert(typeDefine);
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
            PostgresTypeConverter.INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bool")
                        .dataType("bool")
                        .nullable(true)
                        .defaultValue("1")
                        .comment("test")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int2").dataType("int2").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int4").dataType("int4").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int8").dataType("int8").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float4")
                        .dataType("float4")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float8")
                        .dataType("float8")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("numeric(38,2)")
                        .dataType("numeric")
                        .precision(38L)
                        .scale(2)
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("numeric")
                        .dataType("numeric")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bpchar")
                        .dataType("bpchar")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bpchar(10)")
                        .dataType("bpchar")
                        .length(10L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(40, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertVarchar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar")
                        .dataType("varchar")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(null, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar(10)")
                        .dataType("varchar")
                        .length(10L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(40, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertOtherString() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("text").dataType("text").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(null, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("json").dataType("json").build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(null, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("jsonb")
                        .dataType("jsonb")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(null, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("xml").dataType("xml").build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(null, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBinary() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bytea")
                        .dataType("bytea")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertTime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("time").dataType("time").build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("time(3)")
                        .dataType("time")
                        .length(3L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timetz")
                        .dataType("timetz")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timetz(3)")
                        .dataType("timetz")
                        .length(3L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
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
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp(3)")
                        .dataType("timestamp")
                        .length(3L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamptz")
                        .dataType("timestamptz")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamptz(3)")
                        .dataType("timestamptz")
                        .length(3L)
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());
    }

    @Test
    public void testConvertArray() {
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_bool")
                        .dataType("_bool")
                        .build();
        Column column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.BOOLEAN_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_int2")
                        .dataType("_int2")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.SHORT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_int4")
                        .dataType("_int4")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_int8")
                        .dataType("_int8")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LONG_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType().toLowerCase());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_float4")
                        .dataType("_float4")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, column.getDataType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_float8")
                        .dataType("_float8")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.DOUBLE_ARRAY_TYPE, column.getDataType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_bpchar")
                        .dataType("_bpchar")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_varchar")
                        .dataType("_varchar")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("_text")
                        .dataType("_text")
                        .build();
        column = PostgresTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.STRING_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
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

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_REAL, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_REAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_DOUBLE_PRECISION, typeDefine.getColumnType());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_DOUBLE_PRECISION, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        PostgresTypeConverter.PG_NUMERIC,
                        PostgresTypeConverter.DEFAULT_PRECISION,
                        PostgresTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_NUMERIC, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", PostgresTypeConverter.PG_NUMERIC, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_NUMERIC, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_BYTEA, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_BYTEA, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(1L)
                        .build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", PostgresTypeConverter.PG_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(10485761L)
                        .build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", PostgresTypeConverter.PG_TIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIMESTAMP, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", PostgresTypeConverter.PG_TIMESTAMP, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TIMESTAMP, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(9)
                        .build();

        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", PostgresTypeConverter.PG_TIMESTAMP, 6),
                typeDefine.getColumnType());
    }

    @Test
    public void testReconvertArray() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.BOOLEAN_ARRAY_TYPE)
                        .build();

        BasicTypeDefine typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_BOOLEAN_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_BOOLEAN_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.BYTE_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.SHORT_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.INT_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_INTEGER_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_INTEGER_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.LONG_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_BIGINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_BIGINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.FLOAT_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_REAL_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_REAL_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.DOUBLE_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_DOUBLE_PRECISION_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_DOUBLE_PRECISION_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.STRING_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_TEXT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.BYTE_ARRAY_TYPE).build();
        typeDefine = PostgresTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(PostgresTypeConverter.PG_SMALLINT_ARRAY, typeDefine.getDataType());
    }
}
