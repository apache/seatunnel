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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class SQLNumericFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    @Test
    public void testBasicNumericFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"i", "d"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.DOUBLE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select ABS(i) as abs_i,"
                                + " SIGN(i) as sign_i,"
                                + " CEIL(d) as ceil_d,"
                                + " FLOOR(d) as floor_d"
                                + " from dual",
                        rowType,
                        -3,
                        1.2d);

        Assertions.assertEquals(3, outRow.getField(0));
        Assertions.assertEquals(-1, outRow.getField(1));
        Assertions.assertEquals(2, outRow.getField(2));
        Assertions.assertEquals(1, outRow.getField(3));
    }

    @Test
    public void testModAndRound() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow outRow =
                runSql("select MOD(a, b) as m, ROUND(1.234, 2) as r from dual", rowType, 7, 3);

        Assertions.assertEquals(1, outRow.getField(0));
        Assertions.assertEquals(1.23d, (Double) outRow.getField(1), 1e-9);
    }

    @Test
    public void testModByZero() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE});

        Assertions.assertThrows(
                TransformException.class,
                () -> runSql("select MOD(a, b) as m from dual", rowType, 7, 0));
    }

    @Test
    public void testLnLogLog10() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"x"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select LN(x) as ln_x,"
                                + " LOG(10, x) as log10_x,"
                                + " LOG10(x) as log10_fn"
                                + " from dual",
                        rowType,
                        10.0d);

        double ln = (Double) outRow.getField(0);
        double log10ViaLog = (Double) outRow.getField(1);
        double log10 = (Double) outRow.getField(2);

        Assertions.assertEquals(Math.log(10.0d), ln, 1e-9);
        Assertions.assertEquals(Math.log10(10.0d), log10ViaLog, 1e-9);
        Assertions.assertEquals(Math.log10(10.0d), log10, 1e-9);
    }

    @Test
    public void testSqrtRadiansAndPi() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"angle"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select SQRT(4.0) as s,"
                                + " RADIANS(angle) as rad,"
                                + " PI() as pi"
                                + " from dual",
                        rowType,
                        180.0d);

        Assertions.assertEquals(2.0d, (Double) outRow.getField(0), 1e-9);
        Assertions.assertEquals(Math.toRadians(180.0d), (Double) outRow.getField(1), 1e-9);
        Assertions.assertEquals(Math.PI, (Double) outRow.getField(2), 1e-9);
    }

    @Test
    public void testRandDeterministicWithSeed() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"seed"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        SeaTunnelRow outRow = runSql("select RAND(1) as r1, RAND(1) as r2 from dual", rowType, 0);

        double r1 = (Double) outRow.getField(0);
        double r2 = (Double) outRow.getField(1);

        Assertions.assertEquals(r1, r2, 0.0d);
        Assertions.assertTrue(r1 >= 0.0d && r1 < 1.0d);
    }

    @Test
    public void testTruncAndTruncate() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select TRUNC(1.234, 2) as t1,"
                                + " TRUNCATE(1.234, 1) as t2"
                                + " from dual",
                        rowType,
                        0.0d);

        Assertions.assertEquals(1.23d, (Double) outRow.getField(0), 1e-9);
        Assertions.assertEquals(1.2d, (Double) outRow.getField(1), 1e-9);
    }

    @Test
    public void testArrayMaxAndArrayMin() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"arr_i", "arr_s"},
                        new SeaTunnelDataType[] {
                            ArrayType.INT_ARRAY_TYPE, ArrayType.STRING_ARRAY_TYPE
                        });

        SeaTunnelRow outRow =
                runSql(
                        "select ARRAY_MAX(arr_i) as max_i,"
                                + " ARRAY_MIN(arr_i) as min_i,"
                                + " ARRAY_MAX(arr_s) as max_s,"
                                + " ARRAY_MIN(arr_s) as min_s"
                                + " from dual",
                        rowType,
                        (Object) new Object[] {1, 2, 3},
                        (Object) new Object[] {"a", "c", "b"});

        Assertions.assertEquals(3, outRow.getField(0));
        Assertions.assertEquals(1, outRow.getField(1));
        Assertions.assertEquals("c", outRow.getField(2));
        Assertions.assertEquals("a", outRow.getField(3));
    }

    @Test
    public void testArrayMaxAndArrayMinWithEmptyArray() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"arr"}, new SeaTunnelDataType[] {ArrayType.INT_ARRAY_TYPE});

        // Provide an empty array as column value
        SeaTunnelRow outRow =
                runSql(
                        "select ARRAY_MAX(arr) as max_v,"
                                + " ARRAY_MIN(arr) as min_v"
                                + " from dual",
                        rowType,
                        (Object) new Object[] {});

        Assertions.assertNull(outRow.getField(0));
        Assertions.assertNull(outRow.getField(1));
    }

    @Test
    public void testTrigonometricFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"x"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        double x = 0.5d;
        SeaTunnelRow outRow =
                runSql(
                        "select ACOS(x) as acos_x,"
                                + " ASIN(x) as asin_x,"
                                + " ATAN(x) as atan_x,"
                                + " COS(x) as cos_x,"
                                + " COSH(x) as cosh_x,"
                                + " COT(x) as cot_x,"
                                + " SIN(x) as sin_x,"
                                + " SINH(x) as sinh_x,"
                                + " TAN(x) as tan_x,"
                                + " TANH(x) as tanh_x,"
                                + " ATAN2(x, 1.0) as atan2_x"
                                + " from dual",
                        rowType,
                        x);

        Assertions.assertEquals(Math.acos(x), (Double) outRow.getField(0), 1e-9);
        Assertions.assertEquals(Math.asin(x), (Double) outRow.getField(1), 1e-9);
        Assertions.assertEquals(Math.atan(x), (Double) outRow.getField(2), 1e-9);
        Assertions.assertEquals(Math.cos(x), (Double) outRow.getField(3), 1e-9);
        Assertions.assertEquals(Math.cosh(x), (Double) outRow.getField(4), 1e-9);

        double expectedCot = 1.0d / Math.tan(x);
        Assertions.assertEquals(expectedCot, (Double) outRow.getField(5), 1e-9);

        Assertions.assertEquals(Math.sin(x), (Double) outRow.getField(6), 1e-9);
        Assertions.assertEquals(Math.sinh(x), (Double) outRow.getField(7), 1e-9);
        Assertions.assertEquals(Math.tan(x), (Double) outRow.getField(8), 1e-9);
        Assertions.assertEquals(Math.tanh(x), (Double) outRow.getField(9), 1e-9);
        Assertions.assertEquals(Math.atan2(x, 1.0d), (Double) outRow.getField(10), 1e-9);
    }

    @Test
    public void testExpPowerAndRandom() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"x"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        double x = 2.0d;
        SeaTunnelRow outRow =
                runSql(
                        "select EXP(x) as e,"
                                + " POWER(2, 3) as p,"
                                + " RANDOM(1) as r1,"
                                + " RANDOM(1) as r2"
                                + " from dual",
                        rowType,
                        x);

        Assertions.assertEquals(Math.exp(x), (Double) outRow.getField(0), 1e-9);
        Assertions.assertEquals(Math.pow(2.0d, 3.0d), (Double) outRow.getField(1), 1e-9);

        double r1 = (Double) outRow.getField(2);
        double r2 = (Double) outRow.getField(3);
        Assertions.assertEquals(r1, r2, 0.0d);
        Assertions.assertTrue(r1 >= 0.0d && r1 < 1.0d);
    }

    @Test
    public void testSignWithZeroAndNaN() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"x"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});

        // x = 0.0
        SeaTunnelRow rowZero = runSql("select SIGN(x) as s from dual", rowType, 0.0d);
        Assertions.assertEquals(0, rowZero.getField(0));

        // x = -0.0
        SeaTunnelRow rowNegZero = runSql("select SIGN(x) as s from dual", rowType, -0.0d);
        Assertions.assertEquals(0, rowNegZero.getField(0));

        // x = NaN -> SIGN should return 0
        SeaTunnelRow rowNaN = runSql("select SIGN(x) as s from dual", rowType, Double.NaN);
        Assertions.assertEquals(0, rowNaN.getField(0));
    }

    @Test
    public void testNestedNumericExpressions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"x", "y"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, BasicType.DOUBLE_TYPE});

        double x = 30.0d;
        double y = 60.0d;
        SeaTunnelRow outRow =
                runSql(
                        "select ROUND(SIN(RADIANS(x)) + COS(RADIANS(y)), 4) as v1,"
                                + " LOG10(ABS(x)) as v2,"
                                + " TRUNC(POWER(x, 2) / 3, 2) as v3"
                                + " from dual",
                        rowType,
                        x,
                        y);

        double expectedV1 = Math.sin(Math.toRadians(x)) + Math.cos(Math.toRadians(y));
        double expectedV2 = Math.log10(Math.abs(x));
        double expectedV3 = Math.floor((Math.pow(x, 2) / 3) * 100.0d) / 100.0d;

        Assertions.assertEquals(
                Math.round(expectedV1 * 10000.0d) / 10000.0d, (Double) outRow.getField(0), 1e-4);
        Assertions.assertEquals(expectedV2, (Double) outRow.getField(1), 1e-9);
        Assertions.assertEquals(expectedV3, (Double) outRow.getField(2), 1e-9);
    }
}
