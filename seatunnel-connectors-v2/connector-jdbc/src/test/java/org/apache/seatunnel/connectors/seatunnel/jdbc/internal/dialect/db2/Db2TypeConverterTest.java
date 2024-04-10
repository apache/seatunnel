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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2;

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

public class Db2TypeConverterTest {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            DB2TypeConverter.INSTANCE.convert(typeDefine);
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
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("SMALLINT")
                        .dataType("SMALLINT")
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInteger() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("INTEGER")
                        .dataType("INTEGER")
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(DB2TypeConverter.DB2_INT, column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BIGINT")
                        .dataType("BIGINT")
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertReal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("REAL").dataType("REAL").build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DOUBLE")
                        .dataType("DOUBLE")
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecFloat() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DECFLOAT")
                        .dataType("DECFLOAT")
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DECIMAL")
                        .dataType("DECIMAL")
                        .precision(31L)
                        .scale(1)
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(31, 1), column.getDataType());
        Assertions.assertEquals(31, column.getColumnLength());
        Assertions.assertEquals(1, column.getScale());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        DB2TypeConverter.DB2_DECIMAL,
                        typeDefine.getPrecision(),
                        typeDefine.getScale()),
                column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CHARACTER")
                        .dataType("CHARACTER")
                        .length(1L)
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals("CHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARCHAR")
                        .dataType("VARCHAR")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals("VARCHAR(1)", column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("LONG VARCHAR")
                        .dataType("LONG VARCHAR")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("CLOB")
                        .dataType("CLOB")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_CLOB, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("GRAPHIC")
                        .dataType("GRAPHIC")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_GRAPHIC, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARGRAPHIC")
                        .dataType("VARGRAPHIC")
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_VARGRAPHIC, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DBCLOB")
                        .dataType("DBCLOB")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4, column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_DBCLOB, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("XML").dataType("XML").build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(Integer.MAX_VALUE, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBytes() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BINARY")
                        .dataType("BINARY")
                        .length(1L)
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_BINARY, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("VARBINARY")
                        .dataType("VARBINARY")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getLength(), column.getColumnLength());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_VARBINARY, typeDefine.getLength()),
                column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("BLOB")
                        .dataType("BLOB")
                        .length(1L)
                        .build();
        column = DB2TypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_BLOB, typeDefine.getLength()),
                column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("DATE").dataType("DATE").build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTime() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("TIME").dataType("TIME").build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTimestamp() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp")
                        .dataType("timestamp")
                        .scale(6)
                        .build();
        Column column = DB2TypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_TIMESTAMP, typeDefine.getScale()),
                column.getSourceType());
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
            DB2TypeConverter.INSTANCE.reconvert(column);
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

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_BOOLEAN, typeDefine.getDataType());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_INT, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_REAL, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_REAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        DB2TypeConverter.DB2_DECIMAL, DB2TypeConverter.DEFAULT_PRECISION, 0),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", DB2TypeConverter.DB2_DECIMAL, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(32, 31)).build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", DB2TypeConverter.DB2_DECIMAL, 31, 30),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_DECIMAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("VARBINARY(32672)", typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(DB2TypeConverter.MAX_BINARY_LENGTH)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_BINARY, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_BINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(DB2TypeConverter.MAX_VARBINARY_LENGTH)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_VARBINARY, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(DB2TypeConverter.MAX_VARBINARY_LENGTH + 1)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_BLOB, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_BLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("VARCHAR(32672)", typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(DB2TypeConverter.MAX_CHAR_LENGTH)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_CHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_CHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(DB2TypeConverter.MAX_VARCHAR_LENGTH)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(DB2TypeConverter.MAX_VARCHAR_LENGTH + 1)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_CLOB, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_CLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_TIME, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTimestamp() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(DB2TypeConverter.DB2_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_TIMESTAMP, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", DB2TypeConverter.DB2_TIMESTAMP, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(DB2TypeConverter.DB2_TIMESTAMP, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }
}
