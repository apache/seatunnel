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
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

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
        Assertions.assertEquals(
                (byte) 10, NumericFunction.abs(Collections.singletonList((byte) -10)));
        Assertions.assertEquals(
                (short) 10, NumericFunction.abs(Collections.singletonList((short) -10)));
        Assertions.assertEquals(10, NumericFunction.abs(Collections.singletonList(-10)));
        Assertions.assertEquals(10L, NumericFunction.abs(Collections.singletonList(-10L)));
        Assertions.assertEquals(1.5f, NumericFunction.abs(Collections.singletonList(-1.5f)));
        Assertions.assertEquals(2.5d, NumericFunction.abs(Collections.singletonList(-2.5d)));

        BigDecimal decimal = new BigDecimal("-123.45");
        Assertions.assertEquals(
                new BigDecimal("123.45"), NumericFunction.abs(Collections.singletonList(decimal)));

        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.abs(Collections.singletonList(Byte.MIN_VALUE)));
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.abs(Collections.singletonList(Short.MIN_VALUE)));

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

        // BigDecimal precision is preserved (fix for #11696)
        BigDecimal bigLeft = new BigDecimal("123456789012345678901234567890.123456");
        BigDecimal bigRight = new BigDecimal("9876543210.987654");
        BigDecimal bigMod = (BigDecimal) NumericFunction.mod(Arrays.asList(bigLeft, bigRight));
        Assertions.assertEquals(
                bigLeft.remainder(bigRight).stripTrailingZeros(), bigMod.stripTrailingZeros());

        // Mod by a divisor that underflows to 0.0 in double should not throw (fix for #11696)
        BigDecimal tinyDivisor = new BigDecimal("0.00000000000000000001");
        BigDecimal tinyMod =
                (BigDecimal) NumericFunction.mod(Arrays.asList(new BigDecimal("1"), tinyDivisor));
        Assertions.assertEquals(0, tinyMod.compareTo(BigDecimal.ZERO));
    }

    @Test
    public void testCeilFloorRoundAndTrunc() {
        // CEIL/FLOOR return the type of their argument, so a DOUBLE argument yields a DOUBLE.
        Assertions.assertEquals(2d, NumericFunction.ceil(Arrays.asList(1.2d)));
        Assertions.assertEquals(-1d, NumericFunction.ceil(Arrays.asList(-1.8d)));

        Assertions.assertEquals(1d, NumericFunction.floor(Arrays.asList(1.8d)));
        Assertions.assertEquals(-2d, NumericFunction.floor(Arrays.asList(-1.2d)));

        Assertions.assertEquals(2, NumericFunction.ceil(Arrays.asList(2)));
        Assertions.assertEquals(2L, NumericFunction.floor(Arrays.asList(2L)));

        Assertions.assertEquals(3L, NumericFunction.round(Arrays.asList(2.6d)).longValue());
        Assertions.assertEquals(2L, NumericFunction.round(Arrays.asList(2.4d)).longValue());

        Assertions.assertEquals(2L, NumericFunction.trunc(Arrays.asList(2.9d)).longValue());
        Assertions.assertEquals(-2L, NumericFunction.trunc(Arrays.asList(-2.9d)).longValue());

        // negative scale for integer rounding
        Assertions.assertEquals(1200, NumericFunction.round(Arrays.asList(1234, -2)).intValue());
        Assertions.assertEquals((byte) 40, NumericFunction.round(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals((byte) 50, NumericFunction.ceil(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals((byte) 40, NumericFunction.floor(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals((byte) 40, NumericFunction.trunc(Arrays.asList((byte) 44, -1)));
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () -> NumericFunction.round(Arrays.asList(Byte.MAX_VALUE, -1)));
        Assertions.assertThrows(
                org.apache.seatunnel.transform.exception.TransformException.class,
                () ->
                        NumericFunction.round(
                                Collections.singletonList(new java.math.BigInteger("1"))));

        // Long inputs preserve Long return type (fix for #11696)
        Assertions.assertEquals(10000000000L, NumericFunction.ceil(Arrays.asList(10000000000L)));
        Assertions.assertEquals(10000000000L, NumericFunction.floor(Arrays.asList(10000000000L)));
        Assertions.assertEquals(10000000000L, NumericFunction.trunc(Arrays.asList(10000000000L)));

        // BigDecimal inputs preserve BigDecimal return type (fix for #11696)
        BigDecimal bd = new BigDecimal("12345.6789");
        Assertions.assertEquals(new BigDecimal("12346"), NumericFunction.ceil(Arrays.asList(bd)));
        Assertions.assertEquals(new BigDecimal("12345"), NumericFunction.floor(Arrays.asList(bd)));
        Assertions.assertEquals(new BigDecimal("12345"), NumericFunction.trunc(Arrays.asList(bd)));
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
        Assertions.assertEquals(1, NumericFunction.sign(Collections.singletonList((byte) 10)));
        Assertions.assertEquals(-1, NumericFunction.sign(Collections.singletonList((short) -10)));
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
        Assertions.assertEquals(
                1,
                NumericFunction.sign(Collections.singletonList(new BigDecimal("1E-400")))
                        .intValue());
    }

    @Test
    public void testAbsRejectsMinValueInsteadOfReturningItUnchanged() {
        // Math.abs(MIN_VALUE) == MIN_VALUE, so ABS used to hand back a negative "absolute value".
        TransformException intError =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.abs(Collections.singletonList(Integer.MIN_VALUE)));
        Assertions.assertTrue(intError.getMessage().contains("INT"), intError.getMessage());
        Assertions.assertTrue(intError.getMessage().contains("-2147483648"), intError.getMessage());

        TransformException longError =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.abs(Collections.singletonList(Long.MIN_VALUE)));
        Assertions.assertTrue(longError.getMessage().contains("BIGINT"), longError.getMessage());

        // Everything one step inside the boundary is representable and must still work.
        Assertions.assertEquals(
                Integer.MAX_VALUE,
                NumericFunction.abs(Collections.singletonList(Integer.MIN_VALUE + 1)));
        Assertions.assertEquals(
                Long.MAX_VALUE, NumericFunction.abs(Collections.singletonList(Long.MIN_VALUE + 1)));
    }

    @Test
    public void testNegativeScaleRoundingRejectsResultsThatDoNotFit() {
        // ROUND(2147483647, -1) is 2147483650. Narrowing that through intValue() used to wrap it
        // to -2147483646, turning the largest INT into a negative number.
        TransformException roundInt =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.round(Arrays.asList(Integer.MAX_VALUE, -1)));
        Assertions.assertTrue(roundInt.getMessage().contains("ROUND"), roundInt.getMessage());
        Assertions.assertTrue(roundInt.getMessage().contains("INT"), roundInt.getMessage());
        Assertions.assertTrue(roundInt.getMessage().contains("2147483650"), roundInt.getMessage());

        // CEIL overflows at the top of the range, FLOOR at the bottom.
        TransformException ceilInt =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.ceil(Arrays.asList(Integer.MAX_VALUE, -1)));
        Assertions.assertTrue(ceilInt.getMessage().contains("CEIL"), ceilInt.getMessage());

        TransformException floorInt =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.floor(Arrays.asList(Integer.MIN_VALUE, -1)));
        Assertions.assertTrue(floorInt.getMessage().contains("FLOOR"), floorInt.getMessage());

        // BIGINT overflows in BigDecimal.longValue() itself, before any narrowing cast.
        TransformException roundLong =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.round(Arrays.asList(Long.MAX_VALUE, -1)));
        Assertions.assertTrue(roundLong.getMessage().contains("BIGINT"), roundLong.getMessage());

        // SMALLINT is narrowed by shortValue(), which wraps the same way.
        TransformException roundShort =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.round(Arrays.asList(Short.MAX_VALUE, -1)));
        Assertions.assertTrue(
                roundShort.getMessage().contains("SMALLINT"), roundShort.getMessage());

        // TRUNC rounds toward zero, so it can never grow a value out of its own range.
        Assertions.assertEquals(
                2147483640, NumericFunction.trunc(Arrays.asList(Integer.MAX_VALUE, -1)));
    }

    @Test
    public void testNegativeScaleRoundingKeepsArgumentTypeWhenResultFits() {
        Number roundedInt = NumericFunction.round(Arrays.asList(1234, -2));
        Assertions.assertEquals(Integer.valueOf(1200), roundedInt);

        Number roundedShort = NumericFunction.round(Arrays.asList(Short.valueOf((short) 1234), -2));
        Assertions.assertEquals(Short.valueOf((short) 1200), roundedShort);

        Number roundedLong = NumericFunction.round(Arrays.asList(1234L, -2));
        Assertions.assertEquals(Long.valueOf(1200L), roundedLong);

        // Rounding away every significant digit yields zero rather than an out-of-range error.
        Assertions.assertEquals(Integer.valueOf(0), NumericFunction.round(Arrays.asList(1234, -9)));
    }

    @Test
    public void testRoundingFamilyHandlesTinyInt() {
        // round()'s switch had no BYTE case, so a TINYINT argument fell straight through and
        // was returned unrounded - no exception, no log line.
        Assertions.assertEquals(
                Byte.valueOf((byte) 40), NumericFunction.round(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals(
                Byte.valueOf((byte) 50), NumericFunction.ceil(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals(
                Byte.valueOf((byte) 40), NumericFunction.floor(Arrays.asList((byte) 44, -1)));
        Assertions.assertEquals(
                Byte.valueOf((byte) 40), NumericFunction.trunc(Arrays.asList((byte) 44, -1)));

        // Scale 0 leaves an integral argument alone, as it does for the other integral types.
        Assertions.assertEquals(
                Byte.valueOf((byte) 44), NumericFunction.round(Arrays.asList((byte) 44)));

        // Rounding away every significant digit yields zero rather than an out-of-range error.
        Assertions.assertEquals(
                Byte.valueOf((byte) 0), NumericFunction.round(Arrays.asList((byte) 44, -9)));
    }

    @Test
    public void testTinyIntRoundingRejectsResultsThatDoNotFit() {
        // ROUND(127, -1) is 130, which does not fit a TINYINT.
        TransformException error =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.round(Arrays.asList(Byte.MAX_VALUE, -1)));
        Assertions.assertTrue(error.getMessage().contains("TINYINT"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("130"), error.getMessage());

        // TRUNC moves toward zero, so it can never grow a value out of its own range.
        Assertions.assertEquals(
                Byte.valueOf((byte) 120), NumericFunction.trunc(Arrays.asList(Byte.MAX_VALUE, -1)));
    }

    @Test
    public void testRoundingRejectsUnhandledNumericTypes() {
        // The switch had no default, so an unhandled Number was returned unrounded instead of
        // failing. BigInteger is the same type the ABS test uses for this purpose.
        TransformException error =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                NumericFunction.round(
                                        Arrays.asList(new java.math.BigInteger("12"), -1)));
        Assertions.assertTrue(error.getMessage().contains("ROUND"), error.getMessage());
        Assertions.assertTrue(
                error.getMessage().contains("java.math.BigInteger"), error.getMessage());
    }

    @Test
    public void testRoundingDispatchIsLocaleIndependent() {
        // "BigDecimal".toUpperCase() is "B\u0130GDEC\u0130MAL" under a Turkish default locale,
        // which no longer matches the "BIGDECIMAL" case label. That used to fall through the
        // switch and return the value unrounded; once the switch grew a default it would throw
        // instead. Asserting the rounded result rules out both failure modes at once.
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            Assertions.assertEquals(
                    new BigDecimal("1.3"),
                    NumericFunction.round(Arrays.asList(new BigDecimal("1.25"), 1)));
            Assertions.assertEquals(
                    new BigDecimal("2"),
                    NumericFunction.ceil(Collections.singletonList(new BigDecimal("1.25"))));
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    public void testAbsAndSignAcceptTinyIntAndSmallInt() {
        // Both used to throw "Unsupported arg type" on these two documented types.
        Assertions.assertEquals(
                Byte.valueOf((byte) 10),
                NumericFunction.abs(Collections.singletonList((byte) -10)));
        Assertions.assertEquals(
                Short.valueOf((short) 300),
                NumericFunction.abs(Collections.singletonList((short) -300)));

        Assertions.assertEquals(-1, NumericFunction.sign(Collections.singletonList((byte) -10)));
        Assertions.assertEquals(1, NumericFunction.sign(Collections.singletonList((short) 300)));
        Assertions.assertEquals(0, NumericFunction.sign(Collections.singletonList((byte) 0)));
    }

    @Test
    public void testAbsRejectsTinyIntAndSmallIntMinValue() {
        // Math.abs promotes to int, so (byte) -128 would come back as 128 and no longer fit.
        TransformException tinyError =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.abs(Collections.singletonList(Byte.MIN_VALUE)));
        Assertions.assertTrue(tinyError.getMessage().contains("TINYINT"), tinyError.getMessage());
        Assertions.assertTrue(tinyError.getMessage().contains("-128"), tinyError.getMessage());

        TransformException smallError =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> NumericFunction.abs(Collections.singletonList(Short.MIN_VALUE)));
        Assertions.assertTrue(
                smallError.getMessage().contains("SMALLINT"), smallError.getMessage());

        // Everything one step inside the boundary is representable and must still work.
        Assertions.assertEquals(
                Byte.valueOf(Byte.MAX_VALUE),
                NumericFunction.abs(Collections.singletonList((byte) (Byte.MIN_VALUE + 1))));
        Assertions.assertEquals(
                Short.valueOf(Short.MAX_VALUE),
                NumericFunction.abs(Collections.singletonList((short) (Short.MIN_VALUE + 1))));
    }

    @Test
    public void testSignIsExactForDecimalsBelowDoubleRange() {
        // doubleValue() underflows to 0.0 below Double.MIN_VALUE, so a non-zero decimal used to
        // report as zero. signum() is exact.
        Assertions.assertEquals(
                1, NumericFunction.sign(Collections.singletonList(new BigDecimal("1E-400"))));
        Assertions.assertEquals(
                -1, NumericFunction.sign(Collections.singletonList(new BigDecimal("-1E-400"))));
        Assertions.assertEquals(
                0, NumericFunction.sign(Collections.singletonList(new BigDecimal("0.0000"))));
    }
}
