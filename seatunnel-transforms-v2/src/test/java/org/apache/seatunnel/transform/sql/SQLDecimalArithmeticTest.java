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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/** Tests that binary arithmetic on DECIMAL columns stays exact and rounds division to nearest. */
public class SQLDecimalArithmeticTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    private static String plain(Object field) {
        Assertions.assertInstanceOf(BigDecimal.class, field);
        return ((BigDecimal) field).toPlainString();
    }

    private static SeaTunnelRowType twoDecimals(int precision, int scale) {
        return new SeaTunnelRowType(
                new String[] {"a", "b"},
                new SeaTunnelDataType[] {
                    new DecimalType(precision, scale), new DecimalType(precision, scale)
                });
    }

    /**
     * A DECIMAL(38,2) value with 20 significant digits does not survive a round trip through
     * double, so the operands must be converted exactly. The name says "operands" rather than
     * "results": {@code +} and {@code -} are exact end to end, but {@code *} is computed exactly
     * and then rounded to the scale declared for its column, so its result is exact only up to that
     * scale.
     */
    @Test
    public void testAddSubtractMultiplyUseExactOperands() {
        SeaTunnelRowType rowType = twoDecimals(38, 2);

        SeaTunnelRow outRow =
                runSql(
                        "select a + b as sum_val,"
                                + " a - b as diff_val,"
                                + " a * b as mul_val"
                                + " from dual",
                        rowType,
                        new BigDecimal("123456789012345678.99"),
                        new BigDecimal("0.01"));

        // Before the fix these were 123456789012345680.01, 123456789012345680.00 and
        // 1234567890123456.8 respectively.
        Assertions.assertEquals("123456789012345679.00", plain(outRow.getField(0)));
        Assertions.assertEquals("123456789012345678.98", plain(outRow.getField(1)));
        // The exact product is 1234567890123456.7899; it is rounded to the scale declared for
        // the output column, the same way division is.
        Assertions.assertEquals("1234567890123456.79", plain(outRow.getField(2)));
    }

    /** An integral operand wider than double's 53-bit mantissa must not be rounded either. */
    @Test
    public void testMixedDecimalAndBigintStaysExact() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {new DecimalType(38, 0), BasicType.LONG_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select a + b as sum_val from dual",
                        rowType,
                        BigDecimal.ONE,
                        9007199254740993L);

        // 9007199254740993 is not representable as a double; it collapses to ...992, so before
        // the fix this returned 9007199254740993 instead of 9007199254740994.
        Assertions.assertEquals("9007199254740994", plain(outRow.getField(0)));
    }

    /**
     * Division must round to nearest. RoundingMode.UP always rounds away from zero, which inflates
     * every inexact quotient.
     */
    @Test
    public void testDivisionRoundsHalfUp() {
        SeaTunnelRow outRow =
                runSql(
                        "select a / b as div_val from dual",
                        twoDecimals(38, 2),
                        new BigDecimal("10.00"),
                        new BigDecimal("3.00"));

        // RoundingMode.UP gave 3.34.
        Assertions.assertEquals("3.33", plain(outRow.getField(0)));
    }

    /** A quotient below half the last representable digit must round down to zero, not up to it. */
    @Test
    public void testDivisionDoesNotInventValue() {
        SeaTunnelRow outRow =
                runSql(
                        "select a / b as div_val from dual",
                        twoDecimals(38, 2),
                        new BigDecimal("1.00"),
                        new BigDecimal("1000.00"));

        // RoundingMode.UP gave 0.01, manufacturing value out of a quotient that rounds to zero.
        Assertions.assertEquals("0.00", plain(outRow.getField(0)));
    }

    /**
     * Negative quotients must round to nearest as well. RoundingMode.UP rounds away from zero in
     * both directions, so it made negative results more negative rather than closer to zero.
     */
    @Test
    public void testNegativeDivisionRoundsHalfUp() {
        SeaTunnelRow inexact =
                runSql(
                        "select a / b as div_val from dual",
                        twoDecimals(38, 2),
                        new BigDecimal("-10.00"),
                        new BigDecimal("3.00"));
        // RoundingMode.UP gave -3.34.
        Assertions.assertEquals("-3.33", plain(inexact.getField(0)));

        SeaTunnelRow towardsZero =
                runSql(
                        "select a / b as div_val from dual",
                        twoDecimals(38, 2),
                        new BigDecimal("-1.00"),
                        new BigDecimal("1000.00"));
        // RoundingMode.UP gave -0.01, inventing a debit out of a quotient that rounds to zero.
        Assertions.assertEquals("0.00", plain(towardsZero.getField(0)));
    }

    /**
     * HALF_UP breaks an exact tie by rounding away from zero, so -0.005 at scale 2 is -0.01 rather
     * than 0.00. Pinned here because it is the one case where HALF_UP and the old UP agree, and a
     * future switch to HALF_EVEN would silently change it.
     */
    @Test
    public void testNegativeDivisionTieRoundsAwayFromZero() {
        SeaTunnelRow outRow =
                runSql(
                        "select a / b as div_val from dual",
                        twoDecimals(38, 2),
                        new BigDecimal("-5.00"),
                        new BigDecimal("1000.00"));

        Assertions.assertEquals("-0.01", plain(outRow.getField(0)));
    }

    /**
     * Dividing by a zero DECIMAL surfaces BigDecimal's bare ArithmeticException("/ by zero") as the
     * cause. It is now reported the same way MOD by zero already is, naming the operation.
     */
    @Test
    public void testDivisionByZeroReportsExpression() {
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () ->
                                runSql(
                                        "select a / b as div_val from dual",
                                        twoDecimals(38, 2),
                                        new BigDecimal("1.00"),
                                        new BigDecimal("0.00")));

        // ZetaSQLEngine already wraps any failure with the expression that produced it.
        Assertions.assertTrue(exception.getMessage().contains("a / b"), exception.getMessage());

        // The cause is what changes here: previously ArithmeticException("/ by zero").
        Throwable cause = exception.getCause();
        Assertions.assertInstanceOf(TransformException.class, cause);
        Assertions.assertTrue(cause.getMessage().contains("Division by zero"), cause.getMessage());
    }

    /**
     * When both operands are DECIMAL, every emitted DECIMAL must carry the scale that the transform
     * declares for its column. A sink that builds its write schema from the declared type and then
     * encodes the value against it rejects the row when the two disagree, so an exact result is not
     * usable on its own.
     *
     * <p>The invariant is asserted only for DECIMAL-on-DECIMAL arithmetic, which is what this test
     * exercises. It does not hold in general: {@code getExpressionType} declares DECIMAL as soon as
     * either side is DECIMAL, while {@code BigDecimal.add}/{@code subtract} return the max of the
     * operands' <em>runtime</em> scales, so a FLOAT/DOUBLE operand can still push {@code +} and
     * {@code -} past the declared scale. That path is unchanged from before this fix and is tracked
     * separately.
     *
     * <p>Precision is asserted alongside scale because {@code setScale} bounds only the latter. The
     * check documents the range these operands stay within; it is not a proof that the declared
     * precision can never be exceeded, since nothing in the DECIMAL branch bounds it.
     */
    @Test
    public void testEmittedScaleMatchesDeclaredType() {
        SeaTunnelRowType rowType = twoDecimals(38, 2);
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                "query",
                                "select a + b as sum_val,"
                                        + " a - b as diff_val,"
                                        + " a * b as mul_val,"
                                        + " a / b as div_val"
                                        + " from dual"));
        SQLTransform transform = new SQLTransform(config, table);

        SeaTunnelRowType outType = transform.getProducedCatalogTable().getSeaTunnelRowType();
        SeaTunnelRow outRow =
                transform
                        .transformRow(
                                new SeaTunnelRow(
                                        new Object[] {
                                            new BigDecimal("10.25"), new BigDecimal("3.75")
                                        }))
                        .get(0);

        for (int i = 0; i < outType.getTotalFields(); i++) {
            SeaTunnelDataType<?> fieldType = outType.getFieldType(i);
            Assertions.assertInstanceOf(DecimalType.class, fieldType);
            Assertions.assertEquals(
                    ((DecimalType) fieldType).getScale(),
                    ((BigDecimal) outRow.getField(i)).scale(),
                    "declared and emitted scale differ for column "
                            + outType.getFieldName(i)
                            + " (value "
                            + outRow.getField(i)
                            + ")");
            Assertions.assertTrue(
                    ((BigDecimal) outRow.getField(i)).precision()
                            <= ((DecimalType) fieldType).getPrecision(),
                    "emitted precision exceeds the declared precision for column "
                            + outType.getFieldName(i)
                            + " (value "
                            + outRow.getField(i)
                            + ")");
        }
    }
}
