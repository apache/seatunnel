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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

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

import com.mysql.cj.MysqlType;

public class MySqlTypeConverterTest {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("aaa").dataType("aaa").build();
        try {
            MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.VOID_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertBit() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bit(1)")
                        .dataType("bit")
                        .length(1L)
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bit(9)")
                        .dataType("bit")
                        .length(9L)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
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
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint unsigned")
                        .dataType("tinyint unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyint")
                        .dataType("tinyint")
                        .unsigned(true)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("smallint unsigned")
                        .dataType("smallint unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertMediumint() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("mediumint")
                        .dataType("mediumint")
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("mediumint unsigned")
                        .dataType("mediumint unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("int").dataType("int").build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("integer")
                        .dataType("integer")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("int unsigned")
                        .dataType("int unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("integer unsigned")
                        .dataType("integer unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint unsigned")
                        .dataType("bigint unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(20, 0), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("bigint unsigned zerofill")
                        .dataType("bigint unsigned zerofill")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(20, 0), column.getDataType());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("float unsigned")
                        .dataType("float unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("double unsigned")
                        .dataType("double unsigned")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal(38,2)")
                        .dataType("decimal")
                        .precision(38L)
                        .scale(2)
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(38, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal(39,2)")
                        .dataType("decimal")
                        .precision(39L)
                        .scale(2)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(
                new DecimalType(
                        MySqlTypeConverter.DEFAULT_PRECISION, MySqlTypeConverter.DEFAULT_SCALE),
                column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("decimal(38,2) unsigned")
                        .dataType("decimal unsigned")
                        .precision(38L)
                        .scale(2)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(39, 2), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertEnum() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("enum('aaa','bbb')")
                        .dataType("enum")
                        .length(3L)
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getColumnLength());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varchar(2)")
                        .dataType("varchar")
                        .length(2L)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertText() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinytext")
                        .dataType("tinytext")
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(255, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("text").dataType("text").build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(65535, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("mediumtext")
                        .dataType("mediumtext")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(16777215, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("longtext")
                        .dataType("longtext")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(4294967295L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertJson() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("json").dataType("json").build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
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
                        .columnType("binary(1)")
                        .dataType("binary")
                        .length(1L)
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("varbinary(1)")
                        .dataType("varbinary")
                        .length(1L)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(1, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBlob() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("tinyblob")
                        .dataType("tinyblob")
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(255, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder().name("test").columnType("blob").dataType("blob").build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(65535, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("mediumblob")
                        .dataType("mediumblob")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(16777215, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("longblob")
                        .dataType("longblob")
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(4294967295L, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertGeometry() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("geometry")
                        .dataType("geometry")
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder().name("test").columnType("date").dataType("date").build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
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
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("datetime(3)")
                        .dataType("datetime")
                        .scale(3)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTimestamp() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp")
                        .dataType("timestamp")
                        .build();
        Column column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("timestamp(3)")
                        .dataType("timestamp")
                        .scale(3)
                        .build();
        column = MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getScale(), column.getScale());
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
            MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testReconvertNull() {
        Column column =
                PhysicalColumn.of("test", BasicType.VOID_TYPE, (Long) null, true, "null", "null");

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.NULL, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_NULL, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_NULL, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.BOOLEAN, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format("%s(%s)", MySqlTypeConverter.MYSQL_TINYINT, 1),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TINYINT, typeDefine.getDataType());
        Assertions.assertEquals(1, typeDefine.getLength());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TINYINT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.SMALLINT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.INT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_INT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.BIGINT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.FLOAT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DOUBLE, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DECIMAL, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        MySqlTypeConverter.MYSQL_DECIMAL,
                        MySqlTypeConverter.DEFAULT_PRECISION,
                        MySqlTypeConverter.DEFAULT_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DECIMAL, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DECIMAL, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", MySqlTypeConverter.MYSQL_DECIMAL, 10, 2),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DECIMAL, typeDefine.getDataType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.VARBINARY, typeDefine.getNativeType());
        Assertions.assertEquals("VARBINARY(32766)", typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(255L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.VARBINARY, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format(
                        "%s(%s)", MySqlTypeConverter.MYSQL_VARBINARY, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_VARBINARY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(65535L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.MEDIUMBLOB, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMBLOB, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMBLOB, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.MEDIUMBLOB, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMBLOB, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMBLOB, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(4294967295L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.LONGBLOB, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGBLOB, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGBLOB, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.LONGTEXT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGTEXT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGTEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.VARCHAR, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format("%s(%s)", MySqlTypeConverter.MYSQL_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(65535L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TEXT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TEXT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.MEDIUMTEXT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMTEXT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_MEDIUMTEXT, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(4294967295L)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.LONGTEXT, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGTEXT, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_LONGTEXT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DATE, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TIME, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format("%s(%s)", MySqlTypeConverter.MYSQL_TIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testReconvertTimeForV55() {
        MySqlTypeConverter typeConverter = new MySqlTypeConverter(MySqlVersion.V_5_5);
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine = typeConverter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = typeConverter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.TIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_TIME, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DATETIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DATETIME, typeDefine.getNativeType());
        Assertions.assertEquals(
                String.format("%s(%s)", MySqlTypeConverter.MYSQL_DATETIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());
    }

    @Test
    public void testReconvertDatetimeForV55() {
        MySqlTypeConverter typeConverter = new MySqlTypeConverter(MySqlVersion.V_5_5);
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine = typeConverter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DATETIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = typeConverter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MysqlType.DATETIME, typeDefine.getNativeType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getColumnType());
        Assertions.assertEquals(MySqlTypeConverter.MYSQL_DATETIME, typeDefine.getDataType());
    }
}
