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
package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BIGINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BIGINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BOOLEAN;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_BOOLEAN_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATETIME;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATETIMEV2_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DATEV2_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DECIMALV3;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_DOUBLE_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_FLOAT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_FLOAT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_INT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_INT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_JSON;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_LARGEINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_MAP_COLUMN_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_NULL;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_SMALLINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_SMALLINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_STRING;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_STRING_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_TINYINT;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_TINYINT_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType.SR_VARCHAR;

public class StarRocksTypeConverterTest {

    private StarRocksTypeConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StarRocksTypeConverter();
    }

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("aaa")
                        .dataType("aaa")
                        .build();
        try {
            converter.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertNull() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("null")
                        .dataType("null")
                        .nullable(true)
                        .defaultValue("null")
                        .comment("null")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.VOID_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
        Assertions.assertEquals(typeDefine.isNullable(), column.isNullable());
        Assertions.assertEquals(typeDefine.getDefaultValue(), column.getDefaultValue());
        Assertions.assertEquals(typeDefine.getComment(), column.getComment());
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("tinyint(1)")
                        .dataType("tinyint")
                        .length(1L)
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("tinyint(2)")
                        .dataType("tinyint")
                        .length(2L)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("tinyint")
                        .dataType("tinyint")
                        .unsigned(false)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("smallint")
                        .dataType("smallint")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("int")
                        .dataType("int")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("tinyint(1)")
                        .dataType("tinyint")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("bigint")
                        .dataType("bigint")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertLargeint() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("largeint")
                        .dataType("bigint unsigned")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(20, 0), column.getDataType());
        Assertions.assertEquals(20, column.getColumnLength());
        Assertions.assertEquals(0, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("float")
                        .dataType("float")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("double")
                        .dataType("double")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("decimalv3")
                        .dataType("decimal")
                        .precision(9L)
                        .scale(2)
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(9, 2), column.getDataType());
        Assertions.assertEquals(9L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("decimalv3(36,2)")
                        .dataType("decimal")
                        .precision(38L)
                        .scale(2)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(38, 2), column.getDataType());
        Assertions.assertEquals(38L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("char(2)")
                        .dataType("char")
                        .length(2L)
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toLowerCase(Locale.ROOT));

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("varchar(2)")
                        .dataType("varchar")
                        .length(2L)
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(2, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toLowerCase(Locale.ROOT));
    }

    @Test
    public void testConvertString() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("string")
                        .dataType("varchar")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(StarRocksTypeConverter.MAX_STRING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertJson() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("json")
                        .dataType("json")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(StarRocksTypeConverter.MAX_STRING_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("date")
                        .dataType("date")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("datetime")
                        .dataType("datetime")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertArray() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<tinyint(1)>")
                        .dataType("ARRAY")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.BOOLEAN_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<tinyint(4)>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.BYTE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<smallint(6)>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.SHORT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<int(11)>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<bigint(20)>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LONG_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<largeint>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalArrayType(new DecimalType(20, 0)), column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<float>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<double>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.DOUBLE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<decimalv3(10, 2)>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        DecimalArrayType decimalArrayType = new DecimalArrayType(new DecimalType(10, 2));
        Assertions.assertEquals(decimalArrayType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<date>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LOCAL_DATE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("array<datetime>")
                        .dataType("ARRAY")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertMap() {
        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<varchar(65533),tinyint(1)>")
                        .dataType("MAP")
                        .build();
        Column column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        MapType mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<char(1),tinyint(4)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.BYTE_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<string,smallint(6)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.SHORT_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<int(11),int(11)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.INT_TYPE, BasicType.INT_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<tinyint(4),bigint(20)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.BYTE_TYPE, BasicType.LONG_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<smallint(6),largeint>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.SHORT_TYPE, new DecimalType(20, 0));
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<bigint(20),float>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.LONG_TYPE, BasicType.FLOAT_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<largeint,double>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(new DecimalType(20, 0), BasicType.DOUBLE_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<string,decimalv3(10, 2)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.STRING_TYPE, new DecimalType(10, 2));
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<decimalv3(10, 2),date>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(new DecimalType(10, 2), LocalTimeType.LOCAL_DATE_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<date,datetime>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<datetime,char(20)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(LocalTimeType.LOCAL_DATE_TIME_TYPE, BasicType.STRING_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<char(20),varchar(255)>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name("test")
                        .columnType("map<varchar(255),string>")
                        .dataType("MAP")
                        .build();
        column = converter.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
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
        BasicTypeDefine<StarRocksType> reconvert = converter.reconvert(column);
        Assertions.assertEquals(SR_STRING, reconvert.getColumnType());
    }

    @Test
    public void testReconvertNull() {
        Column column =
                PhysicalColumn.of("test", BasicType.VOID_TYPE, (Long) null, true, "null", "null");

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_NULL, typeDefine.getColumnType());
        Assertions.assertEquals(SR_NULL, typeDefine.getDataType());
        Assertions.assertEquals(column.isNullable(), typeDefine.isNullable());
        Assertions.assertEquals(column.getDefaultValue(), typeDefine.getDefaultValue());
        Assertions.assertEquals(column.getComment(), typeDefine.getComment());
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.BOOLEAN_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(SR_BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(1, typeDefine.getLength());
    }

    @Test
    public void testReconvertByte() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.BYTE_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(SR_TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.SHORT_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(SR_SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.INT_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_INT, typeDefine.getColumnType());
        Assertions.assertEquals(SR_INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column = PhysicalColumn.builder().name("test").dataType(BasicType.LONG_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(SR_BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.FLOAT_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(SR_FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.DOUBLE_TYPE).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(SR_DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(new DecimalType(0, 0)).build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        SR_DECIMALV3,
                        StarRocksTypeConverter.MAX_PRECISION,
                        StarRocksTypeConverter.MAX_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_DECIMALV3, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(10, 2)).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DECIMALV3, typeDefine.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", SR_DECIMALV3, 10, 2), typeDefine.getColumnType());

        column = PhysicalColumn.builder().name("test").dataType(new DecimalType(40, 2)).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_VARCHAR, typeDefine.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_VARCHAR, 200), typeDefine.getColumnType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(255L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(65535L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(4294967295L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(SR_JSON)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_JSON, typeDefine.getColumnType());
        Assertions.assertEquals(SR_JSON, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(SR_JSON)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_JSON, typeDefine.getColumnType());
        Assertions.assertEquals(SR_JSON, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_CHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_CHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .sourceType("VARCHAR(255)")
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(65533L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DATE, typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(String.format("%s(%s)", SR_VARCHAR, 8), typeDefine.getColumnType());
        Assertions.assertEquals(SR_VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(String.format("%s(%s)", SR_VARCHAR, 8), typeDefine.getColumnType());
        Assertions.assertEquals(SR_VARCHAR, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_DATETIME, StarRocksTypeConverter.MAX_DATETIME_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATETIME, typeDefine.getDataType());
        Assertions.assertEquals(StarRocksTypeConverter.MAX_DATETIME_SCALE, typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_DATETIME, column.getScale()),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATETIME, typeDefine.getDataType());
        Assertions.assertEquals(column.getScale(), typeDefine.getScale());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(10)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", SR_DATETIME, StarRocksTypeConverter.MAX_DATETIME_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATETIME, typeDefine.getDataType());
        Assertions.assertEquals(StarRocksTypeConverter.MAX_DATETIME_SCALE, typeDefine.getScale());
    }

    @Test
    public void testReconvertArray() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.BOOLEAN_ARRAY_TYPE)
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_BOOLEAN_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_BOOLEAN_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.BYTE_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_TINYINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_TINYINT_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.STRING_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_STRING_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_STRING_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.SHORT_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_SMALLINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_SMALLINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.INT_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_INT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_INT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.LONG_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_BIGINT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_BIGINT_ARRAY, typeDefine.getDataType());

        column = PhysicalColumn.builder().name("test").dataType(ArrayType.FLOAT_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_FLOAT_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_FLOAT_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder().name("test").dataType(ArrayType.DOUBLE_ARRAY_TYPE).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DOUBLE_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_DOUBLE_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DATEV2_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATEV2_ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE)
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(SR_DATETIMEV2_ARRAY, typeDefine.getColumnType());
        Assertions.assertEquals(SR_DATETIMEV2_ARRAY, typeDefine.getDataType());

        DecimalArrayType decimalArrayType = new DecimalArrayType(new DecimalType(10, 2));
        column = PhysicalColumn.builder().name("test").dataType(decimalArrayType).build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DECIMALV3(10, 2)>", typeDefine.getColumnType());
        Assertions.assertEquals("ARRAY<DECIMALV3>", typeDefine.getDataType());

        decimalArrayType = new DecimalArrayType(new DecimalType(20, 0));
        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(decimalArrayType)
                        .sourceType(SR_LARGEINT_ARRAY)
                        .build();
        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DECIMALV3(20, 0)>", typeDefine.getColumnType());
        Assertions.assertEquals("ARRAY<DECIMALV3>", typeDefine.getDataType());
    }

    @Test
    public void testReconvertMap() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE))
                        .build();

        BasicTypeDefine<StarRocksType> typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(SR_MAP_COLUMN_TYPE, "STRING", "STRING"), typeDefine.getColumnType());
        Assertions.assertEquals(
                String.format(SR_MAP_COLUMN_TYPE, "STRING", "STRING"), typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.BYTE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<TINYINT, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<TINYINT, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.SHORT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<SMALLINT, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<SMALLINT, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<INT, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<INT, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.LONG_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<BIGINT, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<BIGINT, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.FLOAT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<FLOAT, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<FLOAT, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.DOUBLE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DOUBLE, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<DOUBLE, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(new MapType<>(new DecimalType(10, 2), BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DECIMALV3(10,2), STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<DECIMALV3(10,2), STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(
                                new MapType<>(LocalTimeType.LOCAL_DATE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DATE, STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<DATE, STRING>", typeDefine.getDataType());

        column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(
                                new MapType<>(
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = converter.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DATETIME(6), STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP<DATETIME(6), STRING>", typeDefine.getDataType());
    }
}
