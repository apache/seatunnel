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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    public void testModByZeroThrows() {
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.mod(java.util.Arrays.asList(7, 0)));
    }

    @Test
    public void testAbsForDifferentNumberTypes() {
        Assertions.assertEquals(10, NumericFunction.abs(Collections.singletonList(-10)));
        Assertions.assertEquals(10L, NumericFunction.abs(Collections.singletonList(-10L)));
        Assertions.assertEquals(1.5f, NumericFunction.abs(Collections.singletonList(-1.5f)));
        Assertions.assertEquals(2.5d, NumericFunction.abs(Collections.singletonList(-2.5d)));

        BigDecimal decimal = new BigDecimal("-123.45");
        Assertions.assertEquals(
                new BigDecimal("123.45"), NumericFunction.abs(Collections.singletonList(decimal)));

        Assertions.assertNull(NumericFunction.abs(Collections.singletonList(null)));

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () ->
                        NumericFunction.abs(
                                Collections.singletonList(new java.math.BigInteger("1"))));
    }

    @Test
    public void testBasicTrigonometricFunctionsAndNull() {
        List<Object> oneArg = Collections.singletonList(0.0);
        Assertions.assertEquals(0.0, NumericFunction.sin(oneArg));
        Assertions.assertEquals(0.0, NumericFunction.tan(oneArg));
        Assertions.assertEquals(1.0, NumericFunction.cosh(oneArg));
        Assertions.assertEquals(1.0, NumericFunction.cos(oneArg));

        List<Object> nullArg = Collections.singletonList(null);
        Assertions.assertNull(NumericFunction.sin(nullArg));
        Assertions.assertNull(NumericFunction.asin(nullArg));
        Assertions.assertNull(NumericFunction.atan(nullArg));
        Assertions.assertNull(NumericFunction.acos(nullArg));
    }

    @Test
    public void testCotAndAtan2() {
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.cot(Collections.singletonList(0.0)));

        List<Object> cotArgs = Collections.singletonList(Math.PI / 4);
        Double cot = NumericFunction.cot(cotArgs);
        Assertions.assertEquals(1.0, cot, 1e-9);

        Assertions.assertEquals(0.0, NumericFunction.atan2(Arrays.asList(0.0, 1.0)), 1e-9);

        Assertions.assertNull(NumericFunction.atan2(Arrays.asList(null, 1.0)));
        Assertions.assertNull(NumericFunction.atan2(Arrays.asList(1.0, null)));
    }

    @Test
    public void testModForDifferentResultTypes() {
        Assertions.assertEquals(1, NumericFunction.mod(Arrays.asList(5, 2)));
        Assertions.assertEquals(1L, NumericFunction.mod(Arrays.asList(5L, 2L)));

        Float floatResult = (Float) NumericFunction.mod(Arrays.asList(5.5f, 2.0f));
        Assertions.assertEquals(1.5f, floatResult);

        Double doubleResult = (Double) NumericFunction.mod(Arrays.asList(5.5d, 2.0d));
        Assertions.assertEquals(1.5d, doubleResult);

        BigDecimal bdResult =
                (BigDecimal)
                        NumericFunction.mod(
                                Arrays.asList(new BigDecimal("5.5"), new BigDecimal("2.0")));
        Assertions.assertEquals(new BigDecimal("1.5"), bdResult.stripTrailingZeros());
    }

    @Test
    public void testCeilFloorRoundAndTrunc() {
        Assertions.assertEquals(2, NumericFunction.ceil(Arrays.asList(1.2d)));
        Assertions.assertEquals(-1, NumericFunction.ceil(Arrays.asList(-1.8d)));

        Assertions.assertEquals(1, NumericFunction.floor(Arrays.asList(1.8d)));
        Assertions.assertEquals(-2, NumericFunction.floor(Arrays.asList(-1.2d)));

        Assertions.assertEquals(3L, NumericFunction.round(Arrays.asList(2.6d)).longValue());
        Assertions.assertEquals(2L, NumericFunction.round(Arrays.asList(2.4d)).longValue());

        Assertions.assertEquals(2L, NumericFunction.trunc(Arrays.asList(2.9d)).longValue());
        Assertions.assertEquals(-2L, NumericFunction.trunc(Arrays.asList(-2.9d)).longValue());

        // negative scale for integer rounding
        Assertions.assertEquals(1200, NumericFunction.round(Arrays.asList(1234, -2)).intValue());
    }

    @Test
    public void testExpLnLogAndLog10() {
        Assertions.assertEquals(Math.exp(1.0), NumericFunction.exp(Collections.singletonList(1.0)));

        double lnValue = NumericFunction.ln(Collections.singletonList(Math.E));
        Assertions.assertEquals(1.0, lnValue, 1e-9);

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.ln(Collections.singletonList(0.0)));

        // LOG(base, value)
        Assertions.assertEquals(2.0, NumericFunction.log(Arrays.asList(10.0, 100.0)), 1e-9);

        Assertions.assertEquals(
                2.0, NumericFunction.log(Arrays.asList(Math.E, Math.E * Math.E)), 1e-9);

        Assertions.assertEquals(3.0, NumericFunction.log(Arrays.asList(2.0, 8.0)), 1e-9);

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.log(Arrays.asList(-1.0, 10.0)));
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.log(Arrays.asList(10.0, -1.0)));

        Assertions.assertEquals(2.0, NumericFunction.log10(Collections.singletonList(100.0)), 1e-9);

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.log10(Collections.singletonList(0.0)));
    }

    @Test
    public void testRadiansSqrtPiAndPower() {
        Assertions.assertEquals(
                Math.PI, NumericFunction.radians(Collections.singletonList(180.0)), 1e-9);

        Assertions.assertEquals(3.0, NumericFunction.sqrt(Collections.singletonList(9.0)), 1e-9);

        Assertions.assertEquals(Math.PI, NumericFunction.pi(Collections.emptyList()), 0.0);

        Assertions.assertEquals(8.0, NumericFunction.power(Arrays.asList(2.0, 3.0)), 1e-9);

        Assertions.assertNull(NumericFunction.power(Arrays.asList(null, 3.0)));
        Assertions.assertNull(NumericFunction.power(Arrays.asList(2.0, null)));
    }

    @Test
    public void testRandomDeterministicWithSeed() {
        Double first = NumericFunction.random(Collections.singletonList(123));
        Double second = NumericFunction.random(Collections.singletonList(123));
        Assertions.assertEquals(first, second);

        Double value = NumericFunction.random(Collections.singletonList(42));
        Assertions.assertTrue(value >= 0.0 && value < 1.0);
    }

    @Test
    public void testSignForDifferentTypes() {
        Assertions.assertEquals(1, NumericFunction.sign(Collections.singletonList(10)));
        Assertions.assertEquals(-1, NumericFunction.sign(Collections.singletonList(-10L)));
        Assertions.assertEquals(0, NumericFunction.sign(Collections.singletonList(0)));

        Assertions.assertEquals(
                1, NumericFunction.sign(Collections.singletonList(2.5d)).intValue());
        Assertions.assertEquals(
                -1, NumericFunction.sign(Collections.singletonList(-2.5f)).intValue());

        Assertions.assertEquals(
                0,
                NumericFunction.sign(Collections.singletonList(new BigDecimal("0.0000")))
                        .intValue());
    }
}
