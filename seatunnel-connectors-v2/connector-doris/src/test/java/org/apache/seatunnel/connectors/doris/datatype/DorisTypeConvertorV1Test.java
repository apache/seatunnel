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

package org.apache.seatunnel.connectors.doris.datatype;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class DorisTypeConvertorV1Test {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            DorisTypeConverterV1.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertNull() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("null")
                        .dataType("null")
                        .nullable(true)
                        .defaultValue("null")
                        .comment("null")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.VOID_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint(1)")
                        .dataType("tinyint")
                        .length(1L)
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint(2)")
                        .dataType("tinyint")
                        .length(2L)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint")
                        .dataType("tinyint")
                        .unsigned(false)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallint")
                        .dataType("smallint")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int").dataType("int").build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint(1)")
                        .dataType("tinyint")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint")
                        .dataType("bigint")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertLargeint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("largeint")
                        .dataType("bigint unsigned")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(20, 0), column.getDataType());
        Assertions.assertEquals(20, column.getColumnLength());
        Assertions.assertEquals(0, column.getScale());
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
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("double")
                        .dataType("double")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal(18,9)")
                        .dataType("decimal")
                        .precision(27L)
                        .scale(9)
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(27, 9), column.getDataType());
        Assertions.assertEquals(27, column.getColumnLength());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimalv3")
                        .dataType("decimal")
                        .precision(9L)
                        .scale(2)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(9, 2), column.getDataType());
        Assertions.assertEquals(9L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimalv3(36,2)")
                        .dataType("decimal")
                        .precision(38L)
                        .scale(2)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
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
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toLowerCase(Locale.ROOT));

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar(2)")
                        .dataType("varchar")
                        .length(2L)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toLowerCase(Locale.ROOT));
    }

    @Test
    public void testConvertString() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("string")
                        .dataType("varchar")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(DorisTypeConverterV1.MAX_STRING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertJson() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("json").dataType("json").build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(DorisTypeConverterV1.MAX_STRING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("json").dataType("json").build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(DorisTypeConverterV1.MAX_STRING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDateV2() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datev2")
                        .dataType("date")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime")
                        .dataType("datetime")
                        .build();
        Column column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetimev2(3)")
                        .dataType("datetime")
                        .scale(3)
                        .build();
        column = DorisTypeConverterV1.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testStringTooLong() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(4294967295L)
                        .build();
        BasicTypeDefine reconvert = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(AbstractDorisTypeConverter.DORIS_STRING, reconvert.getColumnType());
    }

    @Test
    public void testReconvertNull() {
        Column column =
                PhysicalColumn.of("test", BasicType.VOID_TYPE, (Long) null, true, "null", "null");

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_NULL, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_NULL, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(1, typeDefine.getLength());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_INT, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        DorisTypeConverterV1.DORIS_DECIMALV3,
                        DorisTypeConverterV1.MAX_PRECISION,
                        DorisTypeConverterV1.MAX_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DECIMALV3, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DECIMALV3, typeDefine.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", DorisTypeConverterV1.DORIS_DECIMALV3, 10, 2),
                typeDefine.getColumnType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(40, 2)).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_VARCHAR, typeDefine.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s)", DorisTypeConverterV1.DORIS_VARCHAR, 200),
                typeDefine.getColumnType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(255L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(65535L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(4294967295L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(DorisTypeConverterV1.DORIS_JSON)
                        .build();

        BasicTypeDefine typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_JSONB, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_JSON, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(DorisTypeConverterV1.DORIS_JSON)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_JSONB, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_JSON, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DorisTypeConverterV1.DORIS_CHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_CHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .sourceType("VARCHAR(255)")
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", DorisTypeConverterV1.DORIS_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(65533L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", DorisTypeConverterV1.DORIS_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DATEV2, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DATEV2, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DorisTypeConverterV1.DORIS_VARCHAR, 8),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DorisTypeConverterV1.DORIS_VARCHAR, 8),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_VARCHAR, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        DorisTypeConverterV1.DORIS_DATETIMEV2,
                        AbstractDorisTypeConverter.MAX_DATETIME_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DATETIMEV2, typeDefine.getDataType());
        Assertions.assertEquals(
                AbstractDorisTypeConverter.MAX_DATETIME_SCALE, typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DorisTypeConverterV1.DORIS_DATETIMEV2, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DATETIMEV2, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(10)
                        .build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)",
                        DorisTypeConverterV1.DORIS_DATETIMEV2,
                        AbstractDorisTypeConverter.MAX_DATETIME_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DATETIMEV2, typeDefine.getDataType());
        Assertions.assertEquals(
                AbstractDorisTypeConverter.MAX_DATETIME_SCALE, typeDefine.getScale());
    }

    @Test
    public void testReconvertArray() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.BOOLEAN_ARRAY_TYPE)
                        .build();

        BasicTypeDefine<?> typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_BOOLEAN_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BOOLEAN_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.BYTE_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_TINYINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_TINYINT_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.STRING_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_STRING_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_STRING_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.SHORT_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_SMALLINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_SMALLINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.INT_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_INT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_INT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.LONG_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_BIGINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_BIGINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.FLOAT_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_FLOAT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_FLOAT_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.DOUBLE_ARRAY_TYPE).build();

        typeDefine = DorisTypeConverterV1.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV1.DORIS_DOUBLE_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV1.DORIS_DOUBLE_ARRAY, typeDefine.getDataType());

        typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV2.DORIS_DOUBLE_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV2.DORIS_DOUBLE_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE)
                        .build();

        typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV2.DORIS_DATEV2_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(DorisTypeConverterV2.DORIS_DATEV2_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE)
                        .build();

        typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                DorisTypeConverterV2.DORIS_DATETIMEV2_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(
                DorisTypeConverterV2.DORIS_DATETIMEV2_ARRAY, typeDefine.getDataType());

        DecimalArrayType decimalArrayType = new DecimalArrayType(new DecimalType(10, 2));
        column = PhysicalColumn.builder().name("test").dataType(decimalArrayType).build();

        typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DECIMALV3(10, 2)>", typeDefine.getColumnType());
        Assertions.assertEquals("ARRAY<DECIMALV3>", typeDefine.getDataType());

        decimalArrayType = new DecimalArrayType(new DecimalType(20, 0));
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(decimalArrayType)
                        .sourceType(AbstractDorisTypeConverter.DORIS_LARGEINT_ARRAY)
                        .build();
        typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DECIMALV3(20, 0)>", typeDefine.getColumnType());
        Assertions.assertEquals("ARRAY<DECIMALV3>", typeDefine.getDataType());
    }
}
