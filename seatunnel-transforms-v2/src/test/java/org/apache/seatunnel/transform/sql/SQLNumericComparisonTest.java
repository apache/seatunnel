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
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SQLNumericComparisonTest {
    private static final String[] OPERATORS = {
        "=", "!=", "<>", ">", ">=", "<", "<=", "IN", "NOT IN"
    };

    static Stream<Arguments> exactComparisons() {
        List<Arguments> cases = new ArrayList<>();
        addPair(cases, 9007199254740993L, 9007199254740992L, 1);
        addPair(cases, -9007199254740993L, -9007199254740992L, -1);
        addPair(cases, Long.MAX_VALUE, Long.MAX_VALUE - 1, 1);
        addPair(cases, Long.MIN_VALUE, Long.MIN_VALUE + 1, -1);
        addPair(cases, Long.MIN_VALUE, Long.MAX_VALUE, -1);
        addPair(
                cases,
                new BigDecimal("123456789012345678.99"),
                new BigDecimal("123456789012345678.98"),
                1);
        addPair(
                cases,
                new BigDecimal("1.00000000000000000002"),
                new BigDecimal("1.00000000000000000001"),
                1);
        addPair(cases, new BigDecimal("9007199254740993"), 9007199254740992L, 1);
        addPair(cases, new BigDecimal("2.00"), 2L, 0);
        addPair(cases, new BigDecimal("2.0"), new BigDecimal("2.00"), 0);
        addPair(cases, (byte) 1, (short) 2, -1);
        addPair(cases, Integer.MAX_VALUE, (long) Integer.MAX_VALUE, 0);
        return cases.stream();
    }

    private static void addPair(List<Arguments> cases, Number a, Number b, int comparison) {
        for (String operator : OPERATORS) {
            cases.add(Arguments.of(a, b, operator, accepts(operator, comparison)));
            cases.add(Arguments.of(b, a, operator, accepts(operator, -comparison)));
        }
    }

    @ParameterizedTest
    @MethodSource("exactComparisons")
    void comparesExactNumbers(Number a, Number b, String operator, boolean expected) {
        assertEquals(expected, matches(predicate(operator), a, b));
    }

    static Stream<Arguments> floatingComparisons() {
        List<Arguments> cases = new ArrayList<>();
        Number[][] pairs = {
            {Double.NaN, Double.NaN},
            {Double.NaN, 1L},
            {1L, Double.NaN},
            {Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY},
            {Double.NEGATIVE_INFINITY, 1L},
            {-0.0d, 0.0d},
            {-0.0f, 0L},
            {0.1f, 0.1d},
            {9007199254740993L, 9007199254740992d},
            {new BigDecimal("0.1"), 0.1d},
            {Float.NaN, 0f}
        };
        for (Number[] pair : pairs) {
            for (String op : OPERATORS) {
                double a = pair[0].doubleValue();
                double b = pair[1].doubleValue();
                boolean expected;
                switch (op) {
                    case "=":
                    case "IN":
                        expected = a == b;
                        break;
                    case "!=":
                    case "<>":
                    case "NOT IN":
                        expected = a != b;
                        break;
                    case ">":
                        expected = a > b;
                        break;
                    case ">=":
                        expected = a >= b;
                        break;
                    case "<":
                        expected = a < b;
                        break;
                    default:
                        expected = a <= b;
                }
                cases.add(Arguments.of(pair[0], pair[1], op, expected));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("floatingComparisons")
    void preservesFloatingPointComparisons(Number a, Number b, String operator, boolean expected) {
        assertEquals(expected, matches(predicate(operator), a, b));
    }

    @Test
    void comparesLargeIntegerLiteralAndMultipleInValues() {
        assertEquals(false, matches("a = 9007199254740992", 9007199254740993L, 0L));
        assertEquals(true, matches("a IN (0, 9007199254740993)", 9007199254740993L, 0L));
        assertEquals(false, matches("a IN (0, 9007199254740992)", 9007199254740993L, 0L));
        assertEquals(true, matches("a NOT IN (0, 9007199254740992)", 9007199254740993L, 0L));
    }

    @Test
    void comparesExactNumbersInCaseExpressions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE, BasicType.LONG_TYPE});
        SQLTransform transform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(
                                        "query",
                                        "select case when a > b then 'higher' else 'lower' end as ordering, "
                                                + "case a when b then 'same' else 'different' end as equality from dual")),
                        CatalogTableUtil.getCatalogTable("test", rowType));
        List<SeaTunnelRow> output =
                transform.transformRow(
                        new SeaTunnelRow(new Object[] {9007199254740993L, 9007199254740992L}));
        assertEquals("higher", output.get(0).getField(0));
        assertEquals("different", output.get(0).getField(1));
    }

    @Test
    void leavesNullAndStringHandlingUnchanged() {
        assertEquals(false, matches("a = b", null, 1L));
        assertEquals(false, matches("a > b", 1L, null));
        assertEquals(true, matches("a IS NULL", null, 1L));
        assertEquals(true, matches("a = b", "same", "same"));
        assertEquals(true, matches("a < b", "a", "b"));
    }

    private static String predicate(String operator) {
        return "a " + operator + (operator.endsWith("IN") ? " (b)" : " b");
    }

    private static boolean accepts(String op, int comparison) {
        switch (op) {
            case "=":
            case "IN":
                return comparison == 0;
            case "!=":
            case "<>":
            case "NOT IN":
                return comparison != 0;
            case ">":
                return comparison > 0;
            case ">=":
                return comparison >= 0;
            case "<":
                return comparison < 0;
            default:
                return comparison <= 0;
        }
    }

    private static boolean matches(String predicate, Object a, Object b) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"a", "b"}, new SeaTunnelDataType<?>[] {type(a), type(b)});
        SQLTransform transform =
                new SQLTransform(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(
                                        "query", "select a, b from dual where " + predicate)),
                        CatalogTableUtil.getCatalogTable("test", rowType));
        List<SeaTunnelRow> output = transform.transformRow(new SeaTunnelRow(new Object[] {a, b}));
        if (output == null || output.isEmpty()) {
            return false;
        }
        assertEquals(1, output.size());
        assertEquals(a, output.get(0).getField(0));
        assertEquals(b, output.get(0).getField(1));
        return true;
    }

    private static SeaTunnelDataType<?> type(Object value) {
        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            return new DecimalType(38, Math.max(0, decimal.scale()));
        }
        if (value instanceof Byte) return BasicType.BYTE_TYPE;
        if (value instanceof Short) return BasicType.SHORT_TYPE;
        if (value instanceof Integer) return BasicType.INT_TYPE;
        if (value instanceof Float) return BasicType.FLOAT_TYPE;
        if (value instanceof Double) return BasicType.DOUBLE_TYPE;
        if (value instanceof String) return BasicType.STRING_TYPE;
        return BasicType.LONG_TYPE;
    }
}
