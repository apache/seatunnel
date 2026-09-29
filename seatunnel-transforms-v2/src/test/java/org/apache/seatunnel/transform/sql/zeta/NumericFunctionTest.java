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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;
import org.apache.seatunnel.transform.sql.zeta.functions.NumericFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

public class NumericFunctionTest {

    @Test
    public void testTrimScale() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"int_v", "long_v", "float_v", "double_v", "decimal_v"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE,
                            new DecimalType(20, 10)
                        });

        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {20, -99L, 1.20f, 1.230d, new BigDecimal("1.0000010000")});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select TRIM_SCALE(int_v) as new_int_v, TRIM_SCALE(long_v) as new_long_v, TRIM_SCALE(float_v) as new_float_v, TRIM_SCALE(double_v) as new_double_v, TRIM_SCALE(decimal_v) as new_decimal_v from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals("20", outRow.getField(0));
        Assertions.assertEquals("-99", outRow.getField(1));
        Assertions.assertEquals("1.2", outRow.getField(2));
        Assertions.assertEquals("1.23", outRow.getField(3));
        Assertions.assertEquals("1.000001", outRow.getField(4));

        Assertions.assertEquals("123", NumericFunction.trimScale(Collections.singletonList(123)));
        Assertions.assertEquals(
                "123.45", NumericFunction.trimScale(Collections.singletonList(123.45000)));
        Assertions.assertEquals(
                "123", NumericFunction.trimScale(Collections.singletonList(123.0000)));
        Assertions.assertEquals(
                "-123.4", NumericFunction.trimScale(Collections.singletonList(-123.4000)));
        Assertions.assertEquals(
                "0.1",
                NumericFunction.trimScale(Collections.singletonList(new BigDecimal("0.1000"))));
        Assertions.assertEquals("0", NumericFunction.trimScale(Collections.singletonList(0)));
        Assertions.assertNull(NumericFunction.trimScale(Collections.singletonList((Object) null)));
    }

    @Test
    public void testRoundShortNegativeScale() {
        short shortValue = 123;

        Number result = NumericFunction.round(Arrays.asList(shortValue, -1));

        Assertions.assertEquals(120, result.intValue());
    }

    @Test
    public void testSignNullReturnsNull() {
        Assertions.assertNull(NumericFunction.sign(Collections.singletonList(null)));
    }

    /** BIGINT values outside the {@code int} range must survive CEIL/FLOOR unchanged. */
    @Test
    public void testCeilAndFloorKeepBigIntValue() {
        long value = 9007199254740993L;
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"long_v"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {value});

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select CEIL(long_v) as ceil_v, FLOOR(long_v) as floor_v, TRUNC(long_v) as trunc_v from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        Assertions.assertEquals(BasicType.LONG_TYPE, outRowType.getFieldType(0));
        Assertions.assertEquals(BasicType.LONG_TYPE, outRowType.getFieldType(1));
        Assertions.assertEquals(BasicType.LONG_TYPE, outRowType.getFieldType(2));

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(value, outRow.getField(0));
        Assertions.assertEquals(value, outRow.getField(1));
        Assertions.assertEquals(value, outRow.getField(2));
    }

    /** DOUBLE inputs keep their type through CEIL/FLOOR instead of being narrowed to int. */
    @Test
    public void testCeilAndFloorKeepDoubleType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"double_v"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1.0e18d});

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select CEIL(double_v) as ceil_v, FLOOR(double_v) as floor_v from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, outRowType.getFieldType(0));
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, outRowType.getFieldType(1));

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(1.0e18d, outRow.getField(0));
        Assertions.assertEquals(1.0e18d, outRow.getField(1));
    }

    /** DECIMAL values carry more digits than a double, so ROUND/TRUNC must stay exact. */
    @Test
    public void testRoundAndTruncKeepDecimalPrecision() {
        DecimalType decimalType = new DecimalType(38, 9);
        BigDecimal value = new BigDecimal("12345678901234567890.987654321");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"decimal_v"}, new SeaTunnelDataType[] {decimalType});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {value});

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select ROUND(decimal_v, 2) as round_v, TRUNC(decimal_v, 2) as trunc_v, CEIL(decimal_v) as ceil_v from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        Assertions.assertEquals(decimalType, outRowType.getFieldType(0));
        Assertions.assertEquals(decimalType, outRowType.getFieldType(1));
        Assertions.assertEquals(decimalType, outRowType.getFieldType(2));

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(new BigDecimal("12345678901234567890.99"), outRow.getField(0));
        Assertions.assertEquals(new BigDecimal("12345678901234567890.98"), outRow.getField(1));
        Assertions.assertEquals(new BigDecimal("12345678901234567891"), outRow.getField(2));
    }

    /** MOD must not lose the low-order bits of a BIGINT dividend. */
    @Test
    public void testModKeepsBigIntPrecision() {
        Assertions.assertEquals(1, NumericFunction.mod(Arrays.asList(9007199254740993L, 2)));
        Assertions.assertEquals(1L, NumericFunction.mod(Arrays.asList(9007199254740993L, 2L)));
        Assertions.assertEquals(
                new BigDecimal("0.987654321"),
                NumericFunction.mod(
                        Arrays.asList(
                                new BigDecimal("12345678901234567890.987654321"),
                                new BigDecimal("1"))));
    }
}
