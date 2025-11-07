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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.redshift;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedshiftTypeConverterTest {
    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            RedshiftTypeConverter.INSTANCE.convert(typeDefine);
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
                        .columnType("BOOLEAN")
                        .dataType("BOOLEAN")
                        .nullable(true)
                        .defaultValue("1")
                        .comment("test")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SMALLINT")
                        .dataType("SMALLINT")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTEGER")
                        .dataType("INTEGER")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("REAL").dataType("REAL").build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DOUBLE PRECISION")
                        .dataType("DOUBLE PRECISION")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("NUMERIC(38,2)")
                        .dataType("NUMERIC")
                        .precision(38L)
                        .scale(2)
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("numeric")
                        .dataType("numeric")
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 18), column.getDataType());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        RedshiftTypeConverter.REDSHIFT_NUMERIC,
                        RedshiftTypeConverter.DEFAULT_PRECISION,
                        RedshiftTypeConverter.DEFAULT_SCALE),
                column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER")
                        .dataType("CHARACTER")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                RedshiftTypeConverter.MAX_CHARACTER_LENGTH, column.getColumnLength());
        Assertions.assertEquals("CHARACTER(4096)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER(10)")
                        .dataType("CHARACTER")
                        .length(10L)
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertVarchar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER VARYING")
                        .dataType("CHARACTER VARYING")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                RedshiftTypeConverter.MAX_CHARACTER_VARYING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING,
                        RedshiftTypeConverter.MAX_CHARACTER_VARYING_LENGTH),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER VARYING(10)")
                        .dataType("CHARACTER VARYING")
                        .length(10L)
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(10, column.getColumnLength());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING, typeDefine.getLength()),
                column.getSourceType());
    }

    @Test
    public void testConvertOtherString() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("HLLSKETCH")
                        .dataType("HLLSKETCH")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                RedshiftTypeConverter.MAX_HLLSKETCH_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SUPER")
                        .dataType("SUPER")
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_SUPER_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBinary() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARBYTE")
                        .dataType("VARBYTE")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(
                RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_VARBYTE,
                        RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BINARY VARYING")
                        .dataType("BINARY VARYING")
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(
                RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_BINARY_VARYING,
                        RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH),
                column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIME WITHOUT TIME ZONE")
                        .dataType("TIME WITHOUT TIME ZONE")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIME_SCALE, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIME WITH TIME ZONE")
                        .dataType("TIME WITH TIME ZONE")
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIME_SCALE, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTimestamp() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP WITHOUT TIME ZONE")
                        .dataType("TIMESTAMP WITHOUT TIME ZONE")
                        .build();
        Column column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIMESTAMP_SCALE, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP WITH TIME ZONE")
                        .dataType("TIMESTAMP WITH TIME ZONE")
                        .build();
        column = RedshiftTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIMESTAMP_SCALE, column.getScale());
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

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_INTEGER, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_INTEGER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_REAL, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_REAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_DOUBLE_PRECISION, typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_DOUBLE_PRECISION, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        RedshiftTypeConverter.REDSHIFT_NUMERIC,
                        RedshiftTypeConverter.DEFAULT_PRECISION,
                        RedshiftTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_NUMERIC, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", RedshiftTypeConverter.REDSHIFT_NUMERIC, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_NUMERIC, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%d)",
                        RedshiftTypeConverter.REDSHIFT_BINARY_VARYING,
                        RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_BINARY_VARYING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH)
                        .build();
        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%d)",
                        RedshiftTypeConverter.REDSHIFT_BINARY_VARYING, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_BINARY_VARYING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH + 1)
                        .build();
        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%d)",
                        RedshiftTypeConverter.REDSHIFT_BINARY_VARYING,
                        RedshiftTypeConverter.MAX_BINARY_VARYING_LENGTH),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_BINARY_VARYING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING,
                        RedshiftTypeConverter.MAX_CHARACTER_VARYING_LENGTH),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength((long) RedshiftTypeConverter.MAX_CHARACTER_VARYING_LENGTH)
                        .build();

        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_CHARACTER_VARYING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(
                                (long) (RedshiftTypeConverter.MAX_CHARACTER_VARYING_LENGTH + 1))
                        .build();

        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_SUPER, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_SUPER, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.PG_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.PG_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(9)
                        .build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_TIME, typeDefine.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIME_SCALE, typeDefine.getScale());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_TIMESTAMP, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(9)
                        .build();

        typeDefine = RedshiftTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                RedshiftTypeConverter.REDSHIFT_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(RedshiftTypeConverter.REDSHIFT_TIMESTAMP, typeDefine.getDataType());
        Assertions.assertEquals(RedshiftTypeConverter.MAX_TIMESTAMP_SCALE, typeDefine.getScale());
    }
}
