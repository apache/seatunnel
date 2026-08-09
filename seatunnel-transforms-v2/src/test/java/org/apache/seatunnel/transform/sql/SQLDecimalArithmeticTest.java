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
     * double, so the operands must be converted exactly.
     */
    @Test
    public void testAddSubtractMultiplyStayExact() {
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
        Assertions.assertEquals("1234567890123456.7899", plain(outRow.getField(2)));
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
}
