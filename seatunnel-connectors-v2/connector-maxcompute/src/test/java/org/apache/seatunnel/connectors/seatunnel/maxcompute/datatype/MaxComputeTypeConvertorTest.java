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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.datatype;

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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

import java.util.Locale;

public class MaxComputeTypeConvertorTest {

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .columnType("aaa")
                        .dataType("aaa")
                        .build();
        try {
            MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
            Assertions.fail();
        } catch (SeaTunnelRuntimeException e) {
            // ignore
        } catch (Throwable e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertTinyint() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT)
                                        .getTypeName())
                        .dataType(OdpsType.TINYINT.name())
                        .length(1L)
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BYTE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertSmallint() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT)
                                        .getTypeName())
                        .dataType(OdpsType.SMALLINT.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.SHORT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertInt() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT).getTypeName())
                        .dataType(OdpsType.INT.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBoolean() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN)
                                        .getTypeName())
                        .dataType(OdpsType.BOOLEAN.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertBigint() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT).getTypeName())
                        .dataType(OdpsType.BIGINT.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.LONG_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertFloat() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT).getTypeName())
                        .dataType(OdpsType.FLOAT.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.FLOAT_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDouble() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE).getTypeName())
                        .dataType(OdpsType.DOUBLE.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDecimal() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getDecimalTypeInfo(9, 2))
                        .columnType(TypeInfoFactory.getDecimalTypeInfo(9, 2).getTypeName())
                        .dataType(OdpsType.DECIMAL.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(new DecimalType(9, 2), column.getDataType());
        Assertions.assertEquals(9L, column.getColumnLength());
        Assertions.assertEquals(2, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertChar() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getCharTypeInfo(2))
                        .columnType(TypeInfoFactory.getCharTypeInfo(2).getTypeName())
                        .dataType(OdpsType.CHAR.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toUpperCase(Locale.ROOT));

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getVarcharTypeInfo(2))
                        .columnType(TypeInfoFactory.getVarcharTypeInfo(2).getTypeName())
                        .dataType(OdpsType.VARCHAR.name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(8, column.getColumnLength());
        Assertions.assertEquals(
                typeDefine.getColumnType(), column.getSourceType().toUpperCase(Locale.ROOT));
    }

    @Test
    public void testConvertString() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING).getTypeName())
                        .dataType(OdpsType.STRING.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(
                MaxComputeTypeConverter.MAX_VARBINARY_LENGTH, column.getColumnLength());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertJson() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.JSON))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.JSON).getTypeName())
                        .dataType(OdpsType.JSON.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDate() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE).getTypeName())
                        .dataType(OdpsType.DATE.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertDatetime() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME)
                                        .getTypeName())
                        .dataType(OdpsType.DATETIME.name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP)
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP)
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP_NTZ))
                        .columnType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP_NTZ)
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP_NTZ)
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertArray() {
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.BOOLEAN))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.BOOLEAN))
                                        .getOdpsType()
                                        .name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.BOOLEAN_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<BOOLEAN>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.TINYINT))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.TINYINT))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.BYTE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<TINYINT>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.SMALLINT))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.SMALLINT))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.SHORT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<SMALLINT>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.INT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<INT>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.BIGINT))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.BIGINT))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LONG_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<BIGINT>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.FLOAT))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.FLOAT))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.FLOAT_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<FLOAT>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.DOUBLE))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.DOUBLE))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.DOUBLE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<DOUBLE>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LOCAL_DATE_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<DATE>", column.getSourceType());

        typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME)))
                        .columnType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.DATETIME))
                                        .getTypeName())
                        .dataType(
                                TypeInfoFactory.getArrayTypeInfo(
                                                TypeInfoFactory.getPrimitiveTypeInfo(
                                                        OdpsType.DATETIME))
                                        .getOdpsType()
                                        .name())
                        .build();
        column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE, column.getDataType());
        Assertions.assertEquals("ARRAY<DATETIME>", column.getSourceType());
    }

    @Test
    public void testConvertMap() {
        TypeInfo typeInfo =
                TypeInfoFactory.getMapTypeInfo(
                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING),
                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN));
        BasicTypeDefine<TypeInfo> typeDefine =
                BasicTypeDefine.<TypeInfo>builder()
                        .name("test")
                        .nativeType(typeInfo)
                        .columnType(typeInfo.getTypeName())
                        .dataType(typeInfo.getOdpsType().name())
                        .build();
        Column column = MaxComputeTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        MapType mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.BOOLEAN_TYPE);
        Assertions.assertEquals(mapType, column.getDataType());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testReconvertBoolean() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.BOOLEAN_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.BOOLEAN, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.BOOLEAN, typeDefine.getDataType());
        Assertions.assertEquals(1, typeDefine.getLength());
    }

    @Test
    public void testReconvertByte() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.BYTE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.TINYINT, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.TINYINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertShort() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.SHORT_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.SMALLINT, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.SMALLINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertInt() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.INT_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.INT, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.INT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertLong() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.LONG_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.BIGINT, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.BIGINT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertFloat() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.FLOAT_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.FLOAT, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.FLOAT, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDouble() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.DOUBLE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.DOUBLE, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.DOUBLE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDecimal() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new DecimalType(0, 0))
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format(
                        "%s(%s,%s)",
                        MaxComputeTypeConverter.DECIMAL,
                        MaxComputeTypeConverter.MAX_PRECISION,
                        MaxComputeTypeConverter.MAX_SCALE),
                typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.DECIMAL, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new DecimalType(10, 2))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.DECIMAL, typeDefine.getDataType());
        Assertions.assertEquals(
                String.format("%s(%s,%s)", MaxComputeTypeConverter.DECIMAL, 10, 2),
                typeDefine.getColumnType());
    }

    @Test
    public void testReconvertBytes() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(PrimitiveByteArrayType.INSTANCE)
                        .columnLength(null)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.BINARY, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.BINARY, typeDefine.getDataType());
    }

    @Test
    public void testReconvertString() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(MaxComputeTypeConverter.JSON)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(null)
                        .sourceType(MaxComputeTypeConverter.JSON)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", MaxComputeTypeConverter.CHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.CHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(255L)
                        .sourceType("VARCHAR(255)")
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", MaxComputeTypeConverter.CHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.CHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(65533L)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(
                String.format("%s(%s)", MaxComputeTypeConverter.VARCHAR, column.getColumnLength()),
                typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.VARCHAR, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(BasicType.STRING_TYPE)
                        .columnLength(16777215L)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.STRING, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDate() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.DATE, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.DATE, typeDefine.getDataType());
    }

    @Test
    public void testReconvertTime() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                        .build();

        Exception exception =
                Assertions.assertThrows(
                        Exception.class, () -> MaxComputeTypeConverter.INSTANCE.reconvert(column));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "ErrorCode:[COMMON-19], ErrorDescription:['Maxcompute' unsupported convert SeaTunnel data type 'TIME' of 'test' to connector data type.]"));
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.DATETIME, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(3)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.DATETIME, typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.DATETIME, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .scale(10)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals(MaxComputeTypeConverter.TIMESTAMP, typeDefine.getDataType());
    }

    @Test
    public void testReconvertArray() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.BOOLEAN_ARRAY_TYPE)
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<BOOLEAN>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.BYTE_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<TINYINT>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<STRING>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.SHORT_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<SMALLINT>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.INT_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<INT>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.LONG_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<BIGINT>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.FLOAT_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<FLOAT>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.DOUBLE_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DOUBLE>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DATE>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE)
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DATETIME>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.ARRAY, typeDefine.getDataType());

        DecimalArrayType decimalArrayType = new DecimalArrayType(new DecimalType(10, 2));
        column = PhysicalColumn.<TypeInfo>builder().name("test").dataType(decimalArrayType).build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("ARRAY<DECIMAL(10,2)>", typeDefine.getColumnType());
        Assertions.assertEquals("ARRAY", typeDefine.getDataType());
    }

    @Test
    public void testReconvertMap() {
        Column column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE))
                        .build();

        BasicTypeDefine typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<STRING,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals(MaxComputeTypeConverter.MAP, typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.BYTE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<TINYINT,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.SHORT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<SMALLINT,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<INT,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.LONG_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<BIGINT,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.FLOAT_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<FLOAT,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(BasicType.DOUBLE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DOUBLE,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(new MapType<>(new DecimalType(10, 2), BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DECIMAL(10,2),STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(
                                new MapType<>(LocalTimeType.LOCAL_DATE_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DATE,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());

        column =
                PhysicalColumn.<TypeInfo>builder()
                        .name("test")
                        .dataType(
                                new MapType<>(
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE, BasicType.STRING_TYPE))
                        .build();

        typeDefine = MaxComputeTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("MAP<DATETIME,STRING>", typeDefine.getColumnType());
        Assertions.assertEquals("MAP", typeDefine.getDataType());
    }
}
