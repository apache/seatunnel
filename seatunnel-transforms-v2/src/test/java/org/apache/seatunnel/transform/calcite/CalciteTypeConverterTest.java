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

package org.apache.seatunnel.transform.calcite;

import org.apache.seatunnel.shade.org.apache.calcite.avatica.util.TimeUnit;
import org.apache.seatunnel.shade.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.seatunnel.shade.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.seatunnel.shade.org.apache.calcite.sql.type.SqlTypeName;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.transform.calcite.type.CalciteTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CalciteTypeConverterTest {

    private static RelDataTypeFactory typeFactory;

    @BeforeAll
    static void setUp() {
        typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }

    @Test
    void testBooleanRoundTrip() {
        assertRoundTrip(BasicType.BOOLEAN_TYPE, SqlTypeName.BOOLEAN);
    }

    @Test
    void testByteRoundTrip() {
        assertRoundTrip(BasicType.BYTE_TYPE, SqlTypeName.TINYINT);
    }

    @Test
    void testShortRoundTrip() {
        assertRoundTrip(BasicType.SHORT_TYPE, SqlTypeName.SMALLINT);
    }

    @Test
    void testIntRoundTrip() {
        assertRoundTrip(BasicType.INT_TYPE, SqlTypeName.INTEGER);
    }

    @Test
    void testLongRoundTrip() {
        assertRoundTrip(BasicType.LONG_TYPE, SqlTypeName.BIGINT);
    }

    @Test
    void testFloatRoundTrip() {
        assertRoundTrip(BasicType.FLOAT_TYPE, SqlTypeName.REAL);
    }

    @Test
    void testDoubleRoundTrip() {
        assertRoundTrip(BasicType.DOUBLE_TYPE, SqlTypeName.DOUBLE);
    }

    @Test
    void testStringRoundTrip() {
        assertRoundTrip(BasicType.STRING_TYPE, SqlTypeName.VARCHAR);
    }

    @Test
    void testDecimalType() {
        DecimalType decimal = new DecimalType(18, 6);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, decimal);
        Assertions.assertEquals(SqlTypeName.DECIMAL, calciteType.getSqlTypeName());
        Assertions.assertEquals(18, calciteType.getPrecision());
        Assertions.assertEquals(6, calciteType.getScale());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(DecimalType.class, back);
        Assertions.assertEquals(18, ((DecimalType) back).getPrecision());
        Assertions.assertEquals(6, ((DecimalType) back).getScale());
    }

    @Test
    void testDecimalSmallPrecision() {
        DecimalType smallDecimal = new DecimalType(5, 2);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, smallDecimal);
        Assertions.assertEquals(5, calciteType.getPrecision());
        Assertions.assertEquals(2, calciteType.getScale());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(5, ((DecimalType) back).getPrecision());
        Assertions.assertEquals(2, ((DecimalType) back).getScale());
    }

    @Test
    void testDecimalMaxCalcitePrecision() {
        DecimalType bigDecimal = new DecimalType(19, 10);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, bigDecimal);
        Assertions.assertEquals(19, calciteType.getPrecision());
        Assertions.assertEquals(10, calciteType.getScale());
    }

    @Test
    void testDecimalZeroScale() {
        DecimalType intDecimal = new DecimalType(10, 0);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, intDecimal);
        Assertions.assertEquals(10, calciteType.getPrecision());
        Assertions.assertEquals(0, calciteType.getScale());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(0, ((DecimalType) back).getScale());
    }

    @Test
    void testDecimalMinPrecision() {
        DecimalType minDecimal = new DecimalType(1, 0);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, minDecimal);
        Assertions.assertEquals(1, calciteType.getPrecision());
    }

    @Test
    void testDateRoundTrip() {
        assertRoundTrip(LocalTimeType.LOCAL_DATE_TYPE, SqlTypeName.DATE);
    }

    @Test
    void testTimeRoundTrip() {
        assertRoundTrip(LocalTimeType.LOCAL_TIME_TYPE, SqlTypeName.TIME);
    }

    @Test
    void testTimestampRoundTrip() {
        assertRoundTrip(LocalTimeType.LOCAL_DATE_TIME_TYPE, SqlTypeName.TIMESTAMP);
    }

    @Test
    void testTimestampWithTimezone() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(
                        typeFactory, LocalTimeType.OFFSET_DATE_TIME_TYPE);
        Assertions.assertEquals(
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, calciteType.getSqlTypeName());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, back);
    }

    @Test
    void testTimeWithLocalTimezoneReverse() {
        RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, back);
    }

    @Test
    void testBytesType() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, PrimitiveByteArrayType.INSTANCE);
        Assertions.assertEquals(SqlTypeName.VARBINARY, calciteType.getSqlTypeName());
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, back);
    }

    @Test
    void testBinaryReverseMapping() {
        RelDataType binaryType = typeFactory.createSqlType(SqlTypeName.BINARY);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(binaryType);
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, back);
    }

    @Test
    void testCharReverseMapping() {
        RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(charType);
        Assertions.assertEquals(BasicType.STRING_TYPE, back);
    }

    @Test
    void testCharWithPrecisionReverse() {
        RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR, 50);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(charType);
        Assertions.assertEquals(BasicType.STRING_TYPE, back);
    }

    @Test
    void testVarcharWithPrecisionReverse() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(varcharType);
        Assertions.assertEquals(BasicType.STRING_TYPE, back);
    }

    @Test
    void testNullType() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, BasicType.VOID_TYPE);
        Assertions.assertEquals(SqlTypeName.NULL, calciteType.getSqlTypeName());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(BasicType.VOID_TYPE, back);
    }

    @Test
    void testStringArrayForward() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, ArrayType.STRING_ARRAY_TYPE);
        Assertions.assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    }

    @Test
    void testIntArrayRoundTrip() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, ArrayType.INT_ARRAY_TYPE);
        Assertions.assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(ArrayType.class, back);
        Assertions.assertEquals(BasicType.INT_TYPE, ((ArrayType<?, ?>) back).getElementType());
    }

    @Test
    void testBooleanArrayForward() {
        assertArrayForward(ArrayType.BOOLEAN_ARRAY_TYPE);
    }

    @Test
    void testByteArrayForward() {
        assertArrayForward(ArrayType.BYTE_ARRAY_TYPE);
    }

    @Test
    void testShortArrayForward() {
        assertArrayForward(ArrayType.SHORT_ARRAY_TYPE);
    }

    @Test
    void testLongArrayForward() {
        assertArrayForward(ArrayType.LONG_ARRAY_TYPE);
    }

    @Test
    void testFloatArrayForward() {
        assertArrayForward(ArrayType.FLOAT_ARRAY_TYPE);
    }

    @Test
    void testDoubleArrayForward() {
        assertArrayForward(ArrayType.DOUBLE_ARRAY_TYPE);
    }

    @Test
    void testStringArrayRoundTrip() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, ArrayType.STRING_ARRAY_TYPE);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(ArrayType.class, back);
        Assertions.assertEquals(BasicType.STRING_TYPE, ((ArrayType<?, ?>) back).getElementType());
    }

    @Test
    void testLongArrayRoundTrip() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, ArrayType.LONG_ARRAY_TYPE);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(ArrayType.class, back);
        Assertions.assertEquals(BasicType.LONG_TYPE, ((ArrayType<?, ?>) back).getElementType());
    }

    @Test
    void testDoubleArrayRoundTrip() {
        RelDataType calciteType =
                CalciteTypeConverter.toCalciteType(typeFactory, ArrayType.DOUBLE_ARRAY_TYPE);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(ArrayType.class, back);
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, ((ArrayType<?, ?>) back).getElementType());
    }

    @Test
    void testArrayNullComponentReverse() {
        RelDataType arrayType =
                typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(arrayType);
        Assertions.assertInstanceOf(ArrayType.class, back);
    }

    @Test
    void testArrayOfDecimalForward() {
        ArrayType<?, ?> arrayType =
                new ArrayType<>(java.math.BigDecimal.class, new DecimalType(10, 2));
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, arrayType);
        Assertions.assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    }

    @Test
    void testArrayOfDateForward() {
        ArrayType<?, ?> arrayType =
                new ArrayType<>(java.time.LocalDate.class, LocalTimeType.LOCAL_DATE_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, arrayType);
        Assertions.assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    }

    @Test
    void testMapType() {
        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);
        Assertions.assertEquals(SqlTypeName.MAP, calciteType.getSqlTypeName());
    }

    @Test
    void testMapTypeRoundTrip() {
        MapType<String, Integer> mapType = new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(MapType.class, back);
        MapType<?, ?> backMap = (MapType<?, ?>) back;
        Assertions.assertEquals(BasicType.STRING_TYPE, backMap.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, backMap.getValueType());
    }

    @Test
    void testMapIntIntForward() {
        MapType<Integer, Integer> mapType = new MapType<>(BasicType.INT_TYPE, BasicType.INT_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);
        Assertions.assertEquals(SqlTypeName.MAP, calciteType.getSqlTypeName());
    }

    @Test
    void testMapIntIntRoundTrip() {
        MapType<Integer, Integer> mapType = new MapType<>(BasicType.INT_TYPE, BasicType.INT_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        MapType<?, ?> backMap = (MapType<?, ?>) back;
        Assertions.assertEquals(BasicType.INT_TYPE, backMap.getKeyType());
        Assertions.assertEquals(BasicType.INT_TYPE, backMap.getValueType());
    }

    @Test
    void testMapStringDoubleRoundTrip() {
        MapType<String, Double> mapType =
                new MapType<>(BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        MapType<?, ?> backMap = (MapType<?, ?>) back;
        Assertions.assertEquals(BasicType.STRING_TYPE, backMap.getKeyType());
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, backMap.getValueType());
    }

    @Test
    void testMapWithComplexValue() {
        SeaTunnelRowType innerRow =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        MapType<String, ?> mapType = new MapType<>(BasicType.STRING_TYPE, innerRow);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, mapType);
        Assertions.assertEquals(SqlTypeName.MAP, calciteType.getSqlTypeName());
    }

    @Test
    void testRowType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, rowType);
        Assertions.assertTrue(calciteType.isStruct());
        Assertions.assertEquals(2, calciteType.getFieldCount());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertInstanceOf(SeaTunnelRowType.class, back);
        SeaTunnelRowType backRow = (SeaTunnelRowType) back;
        Assertions.assertEquals("name", backRow.getFieldName(0));
        Assertions.assertEquals("age", backRow.getFieldName(1));
    }

    @Test
    void testNestedRowType() {
        SeaTunnelRowType inner =
                new SeaTunnelRowType(
                        new String[] {"city", "zip"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        SeaTunnelRowType outer =
                new SeaTunnelRowType(
                        new String[] {"name", "address"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, inner});

        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, outer);
        Assertions.assertTrue(calciteType.isStruct());
        Assertions.assertTrue(calciteType.getFieldList().get(1).getType().isStruct());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        SeaTunnelRowType backOuter = (SeaTunnelRowType) back;
        SeaTunnelRowType backInner = (SeaTunnelRowType) backOuter.getFieldType(1);
        Assertions.assertEquals("city", backInner.getFieldName(0));
        Assertions.assertEquals("zip", backInner.getFieldName(1));
    }

    @Test
    void testSingleFieldRow() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, rowType);
        Assertions.assertEquals(1, calciteType.getFieldCount());
    }

    @Test
    void testRowWithManyFields() {
        String[] names = new String[10];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[10];
        for (int i = 0; i < 10; i++) {
            names[i] = "field_" + i;
            types[i] = BasicType.INT_TYPE;
        }
        SeaTunnelRowType rowType = new SeaTunnelRowType(names, types);
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, rowType);
        Assertions.assertEquals(10, calciteType.getFieldCount());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        SeaTunnelRowType backRow = (SeaTunnelRowType) back;
        Assertions.assertEquals(10, backRow.getTotalFields());
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals("field_" + i, backRow.getFieldName(i));
        }
    }

    @Test
    void testRowWithAllBasicTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "bool", "byte", "short", "int", "long", "float", "double", "str"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.BOOLEAN_TYPE,
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.STRING_TYPE
                        });
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, rowType);
        Assertions.assertEquals(8, calciteType.getFieldCount());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        SeaTunnelRowType backRow = (SeaTunnelRowType) back;
        Assertions.assertEquals(SqlType.BOOLEAN, backRow.getFieldType(0).getSqlType());
        Assertions.assertEquals(SqlType.TINYINT, backRow.getFieldType(1).getSqlType());
        Assertions.assertEquals(SqlType.SMALLINT, backRow.getFieldType(2).getSqlType());
        Assertions.assertEquals(SqlType.INT, backRow.getFieldType(3).getSqlType());
        Assertions.assertEquals(SqlType.BIGINT, backRow.getFieldType(4).getSqlType());
        Assertions.assertEquals(SqlType.FLOAT, backRow.getFieldType(5).getSqlType());
        Assertions.assertEquals(SqlType.DOUBLE, backRow.getFieldType(6).getSqlType());
        Assertions.assertEquals(SqlType.STRING, backRow.getFieldType(7).getSqlType());
    }

    @Test
    void testDeeplyNestedRow() {
        SeaTunnelRowType level3 =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelRowType level2 =
                new SeaTunnelRowType(new String[] {"inner"}, new SeaTunnelDataType[] {level3});
        SeaTunnelRowType level1 =
                new SeaTunnelRowType(new String[] {"mid"}, new SeaTunnelDataType[] {level2});

        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, level1);
        Assertions.assertTrue(calciteType.isStruct());
        Assertions.assertTrue(
                calciteType
                        .getFieldList()
                        .get(0)
                        .getType()
                        .getFieldList()
                        .get(0)
                        .getType()
                        .isStruct());

        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        SeaTunnelRowType backL1 = (SeaTunnelRowType) back;
        SeaTunnelRowType backL2 = (SeaTunnelRowType) backL1.getFieldType(0);
        SeaTunnelRowType backL3 = (SeaTunnelRowType) backL2.getFieldType(0);
        Assertions.assertEquals(SqlType.INT, backL3.getFieldType(0).getSqlType());
    }

    @Test
    void testVectorBinaryType() {
        assertVectorForward(VectorType.VECTOR_BINARY_TYPE);
    }

    @Test
    void testVectorFloatType() {
        assertVectorForward(VectorType.VECTOR_FLOAT_TYPE);
    }

    @Test
    void testVectorFloat16Type() {
        assertVectorForward(VectorType.VECTOR_FLOAT16_TYPE);
    }

    @Test
    void testVectorBfloat16Type() {
        assertVectorForward(VectorType.VECTOR_BFLOAT16_TYPE);
    }

    @Test
    void testVectorSparseFloatType() {
        assertVectorForward(VectorType.VECTOR_SPARSE_FLOAT_TYPE);
    }

    @Test
    void testIntervalYear() {
        assertIntervalReverse(TimeUnit.YEAR, null);
    }

    @Test
    void testIntervalYearMonth() {
        assertIntervalReverse(TimeUnit.YEAR, TimeUnit.MONTH);
    }

    @Test
    void testIntervalMonth() {
        assertIntervalReverse(TimeUnit.MONTH, null);
    }

    @Test
    void testIntervalDay() {
        assertIntervalReverse(TimeUnit.DAY, null);
    }

    @Test
    void testIntervalDayHour() {
        assertIntervalReverse(TimeUnit.DAY, TimeUnit.HOUR);
    }

    @Test
    void testIntervalDayMinute() {
        assertIntervalReverse(TimeUnit.DAY, TimeUnit.MINUTE);
    }

    @Test
    void testIntervalDaySecond() {
        assertIntervalReverse(TimeUnit.DAY, TimeUnit.SECOND);
    }

    @Test
    void testIntervalHour() {
        assertIntervalReverse(TimeUnit.HOUR, null);
    }

    @Test
    void testIntervalHourMinute() {
        assertIntervalReverse(TimeUnit.HOUR, TimeUnit.MINUTE);
    }

    @Test
    void testIntervalHourSecond() {
        assertIntervalReverse(TimeUnit.HOUR, TimeUnit.SECOND);
    }

    @Test
    void testIntervalMinute() {
        assertIntervalReverse(TimeUnit.MINUTE, null);
    }

    @Test
    void testIntervalMinuteSecond() {
        assertIntervalReverse(TimeUnit.MINUTE, TimeUnit.SECOND);
    }

    @Test
    void testIntervalSecond() {
        assertIntervalReverse(TimeUnit.SECOND, null);
    }

    @Test
    void testAnyReverseMapping() {
        RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(anyType);
        Assertions.assertEquals(BasicType.STRING_TYPE, back);
    }

    @Test
    void testCalciteFloatReverse() {
        RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(floatType);
        Assertions.assertEquals(BasicType.FLOAT_TYPE, back);
    }

    @Test
    void testCalciteRealReverse() {
        RelDataType realType = typeFactory.createSqlType(SqlTypeName.REAL);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(realType);
        Assertions.assertEquals(BasicType.FLOAT_TYPE, back);
    }

    @Test
    void testMultisetReverse() {
        RelDataType multisetType =
                typeFactory.createMultisetType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(multisetType);
        Assertions.assertInstanceOf(ArrayType.class, back);
    }

    @Test
    void testNullableIntegerReverse() {
        RelDataType nullableInt =
                typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(nullableInt);
        Assertions.assertEquals(BasicType.INT_TYPE, back);
    }

    @Test
    void testNonNullableIntegerReverse() {
        RelDataType notNullInt =
                typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(notNullInt);
        Assertions.assertEquals(BasicType.INT_TYPE, back);
    }

    @Test
    void testNullableVarcharReverse() {
        RelDataType nullableVarchar =
                typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(nullableVarchar);
        Assertions.assertEquals(BasicType.STRING_TYPE, back);
    }

    private void assertRoundTrip(SeaTunnelDataType<?> stType, SqlTypeName expectedCalciteName) {
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, stType);
        Assertions.assertEquals(expectedCalciteName, calciteType.getSqlTypeName());
        SeaTunnelDataType<?> roundTripped = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(stType, roundTripped);
    }

    private void assertArrayForward(ArrayType<?, ?> arrayType) {
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, arrayType);
        Assertions.assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    }

    private void assertVectorForward(SeaTunnelDataType<?> vectorType) {
        RelDataType calciteType = CalciteTypeConverter.toCalciteType(typeFactory, vectorType);
        Assertions.assertEquals(SqlTypeName.VARBINARY, calciteType.getSqlTypeName());
    }

    private void assertIntervalReverse(TimeUnit start, TimeUnit end) {
        SqlIntervalQualifier qualifier = new SqlIntervalQualifier(start, end, SqlParserPos.ZERO);
        RelDataType calciteType = typeFactory.createSqlIntervalType(qualifier);
        SeaTunnelDataType<?> back = CalciteTypeConverter.toSeaTunnelType(calciteType);
        Assertions.assertEquals(BasicType.LONG_TYPE, back);
    }
}
