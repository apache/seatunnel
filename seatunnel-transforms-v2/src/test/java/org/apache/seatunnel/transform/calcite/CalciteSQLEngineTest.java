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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.calcite.engine.CalciteSQLEngine;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CalciteSQLEngineTest {

    private SeaTunnelRowType buildRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "age"},
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                });
    }

    private SeaTunnelRowType numericRowType() {
        return new SeaTunnelRowType(
                new String[] {"a", "b"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.INT_TYPE});
    }

    private CalciteSQLEngine createAndInit(String sql, String table, SeaTunnelRowType rowType) {
        CalciteSQLEngine engine = new CalciteSQLEngine(sql, table, rowType);
        engine.init();
        return engine;
    }

    private Object singleField(CalciteSQLEngine engine, Object[] fields) {
        return engine.execute(new SeaTunnelRow(fields)).get(0).getField(0);
    }

    private List<SeaTunnelRow> exec(CalciteSQLEngine engine, Object[] fields) {
        return engine.execute(new SeaTunnelRow(fields));
    }

    @Test
    void testSelectWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, name FROM test_table WHERE age > 20",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));

        Assertions.assertTrue(exec(engine, new Object[] {2, "Bob", 18}).isEmpty());
        engine.close();
    }

    @Test
    void testSelectStar() {
        CalciteSQLEngine engine =
                createAndInit("SELECT * FROM test_table", "test_table", buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        Assertions.assertEquals(25, result.get(0).getField(2));
        Assertions.assertEquals(3, engine.getOutputRowType().getTotalFields());
        engine.close();
    }

    @Test
    void testSelectWithFunctions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, UPPER(name) AS name_upper, age + 1 AS next_age FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "alice", 30});
        Assertions.assertEquals("ALICE", result.get(0).getField(1));
        Assertions.assertEquals(31, result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testStringFunctions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT LOWER(name) AS lower_name, "
                                + "CHAR_LENGTH(name) AS name_len, "
                                + "SUBSTRING(name, 1, 3) AS name_sub "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "ALICE", 25});
        Assertions.assertEquals("alice", result.get(0).getField(0));
        Assertions.assertEquals(5, result.get(0).getField(1));
        Assertions.assertEquals("ALI", result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testConcatOperator() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name || '-' || CAST(age AS VARCHAR) AS combined FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("Alice-25", singleField(engine, new Object[] {1, "Alice", 25}));
        engine.close();
    }

    @Test
    void testTrimFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(name) AS trimmed FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "seatunnel", singleField(engine, new Object[] {1, "  seatunnel  ", 25}));
        engine.close();
    }

    @Test
    void testTrimLeading() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(LEADING ' ' FROM name) AS trimmed FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "seatunnel  ", singleField(engine, new Object[] {1, "  seatunnel  ", 25}));
        engine.close();
    }

    @Test
    void testTrimTrailing() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(TRAILING ' ' FROM name) AS trimmed FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "  seatunnel", singleField(engine, new Object[] {1, "  seatunnel  ", 25}));
        engine.close();
    }

    @Test
    void testReplaceFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT REPLACE(name, 'sea', 'lake') AS replaced FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "laketunnel", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testPositionFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT POSITION('tunnel' IN name) AS pos FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(4, singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testPositionNotFound() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT POSITION('xyz' IN name) AS pos FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(0, singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testOverlayFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT OVERLAY(name PLACING 'ZETA' FROM 4 FOR 4) AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "seaZETAel", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testSubstringFrom() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT SUBSTRING(name FROM 4) AS sub FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("tunnel", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testInitcap() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT INITCAP(name) AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "Sea Tunnel", singleField(engine, new Object[] {1, "sea tunnel", 25}));
        engine.close();
    }

    @Test
    void testCharLengthEmptyString() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CHAR_LENGTH(name) AS len FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(0, singleField(engine, new Object[] {1, "", 25}));
        engine.close();
    }

    @Test
    void testArithmeticExpressions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT a + b AS sum_val, a * b AS product, a - b AS diff FROM t",
                        "t",
                        numericRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {10, 3});
        Assertions.assertEquals(13, result.get(0).getField(0));
        Assertions.assertEquals(30, result.get(0).getField(1));
        Assertions.assertEquals(7, result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testDivision() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a / b AS quotient FROM t", "t", numericRowType());

        Assertions.assertEquals(3, singleField(engine, new Object[] {10, 3}));
        engine.close();
    }

    @Test
    void testModulo() {
        CalciteSQLEngine engine =
                createAndInit("SELECT MOD(a, b) AS remainder FROM t", "t", numericRowType());

        Assertions.assertEquals(1, singleField(engine, new Object[] {10, 3}));
        engine.close();
    }

    @Test
    void testModuloZeroResult() {
        CalciteSQLEngine engine =
                createAndInit("SELECT MOD(a, b) AS remainder FROM t", "t", numericRowType());

        Assertions.assertEquals(0, singleField(engine, new Object[] {9, 3}));
        engine.close();
    }

    @Test
    void testAbsPositive() {
        CalciteSQLEngine engine =
                createAndInit("SELECT ABS(a) AS abs_val FROM t", "t", numericRowType());

        Assertions.assertEquals(5, singleField(engine, new Object[] {5, 0}));
        engine.close();
    }

    @Test
    void testAbsNegative() {
        CalciteSQLEngine engine =
                createAndInit("SELECT ABS(a) AS abs_val FROM t", "t", numericRowType());

        Assertions.assertEquals(5, singleField(engine, new Object[] {-5, 0}));
        engine.close();
    }

    @Test
    void testAbsZero() {
        CalciteSQLEngine engine =
                createAndInit("SELECT ABS(a) AS abs_val FROM t", "t", numericRowType());

        Assertions.assertEquals(0, singleField(engine, new Object[] {0, 0}));
        engine.close();
    }

    @Test
    void testCeilFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT CEIL(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {3.2});
        Assertions.assertEquals(4.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testFloorFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT FLOOR(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {3.8});
        Assertions.assertEquals(3.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testPowerFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"base_val", "exponent"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT POWER(base_val, exponent) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {2.0, 10.0});
        Assertions.assertEquals(1024.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testSqrtFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT SQRT(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {144.0});
        Assertions.assertEquals(12.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testLnFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT LN(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Math.E});
        Assertions.assertEquals(1.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testLog10Function() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT LOG10(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {1000.0});
        Assertions.assertEquals(3.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testExpFunction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT EXP(val) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {0.0});
        Assertions.assertEquals(1.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testNegativeArithmetic() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a * b AS product FROM t", "t", numericRowType());

        Assertions.assertEquals(-15, singleField(engine, new Object[] {-3, 5}));
        engine.close();
    }

    @Test
    void testArithmeticWithZero() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a + b AS sum_val FROM t", "t", numericRowType());

        Assertions.assertEquals(0, singleField(engine, new Object[] {0, 0}));
        engine.close();
    }

    @Test
    void testIntegerOverflow() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a + b AS sum_val FROM t", "t", numericRowType());
        // int overflow wraps around
        Object result = singleField(engine, new Object[] {Integer.MAX_VALUE, 1});
        Assertions.assertNotNull(result);
        engine.close();
    }

    @Test
    void testCaseWhen() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS category "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "adult", exec(engine, new Object[] {1, "Alice", 25}).get(0).getField(1));
        Assertions.assertEquals(
                "minor", exec(engine, new Object[] {2, "Bob", 10}).get(0).getField(1));
        engine.close();
    }

    @Test
    void testCaseWhenMultipleBranches() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(CASE "
                                + "WHEN age < 13 THEN 'child' "
                                + "WHEN age < 18 THEN 'teen' "
                                + "WHEN age < 65 THEN 'adult' "
                                + "ELSE 'senior' END) AS group_name "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("child", singleField(engine, new Object[] {1, "A", 8}));
        Assertions.assertEquals("teen", singleField(engine, new Object[] {2, "B", 15}));
        Assertions.assertEquals("adult", singleField(engine, new Object[] {3, "C", 30}));
        Assertions.assertEquals("senior", singleField(engine, new Object[] {4, "D", 70}));
        engine.close();
    }

    @Test
    void testCaseWhenWithNullCondition() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN name IS NULL THEN 'no-name' ELSE name END AS safe "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("no-name", singleField(engine, new Object[] {1, null, 25}));
        Assertions.assertEquals("Alice", singleField(engine, new Object[] {2, "Alice", 25}));
        engine.close();
    }

    @Test
    void testNestedCaseWhen() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN age > 18 THEN "
                                + "CASE WHEN name IS NOT NULL THEN name ELSE 'anon' END "
                                + "ELSE 'minor' END AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("Alice", singleField(engine, new Object[] {1, "Alice", 25}));
        Assertions.assertEquals("minor", singleField(engine, new Object[] {2, "Bob", 10}));
        engine.close();
    }

    @Test
    void testCaseWhenNoElse() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN age > 50 THEN 'old' END AS label FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertNull(singleField(engine, new Object[] {1, "A", 25}));
        Assertions.assertEquals("old", singleField(engine, new Object[] {2, "B", 60}));
        engine.close();
    }

    @Test
    void testSimpleCaseExpression() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(CASE age WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' "
                                + "ELSE 'other' END) AS label FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("ten", singleField(engine, new Object[] {1, "A", 10}));
        Assertions.assertEquals("twenty", singleField(engine, new Object[] {2, "B", 20}));
        Assertions.assertEquals("other", singleField(engine, new Object[] {3, "C", 30}));
        engine.close();
    }

    @Test
    void testNullValuePassthrough() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id, name FROM test_table", "test_table", buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, null, 25});
        Assertions.assertNull(result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testCoalesce() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT COALESCE(name, 'unknown') AS safe_name FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("unknown", singleField(engine, new Object[] {1, null, 25}));
        Assertions.assertEquals("Alice", singleField(engine, new Object[] {2, "Alice", 25}));
        engine.close();
    }

    @Test
    void testCoalesceMultipleArgs() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b", "c"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT COALESCE(a, b, c) AS res FROM t", "t", rt);

        Assertions.assertEquals(
                "first", singleField(engine, new Object[] {"first", "second", "third"}));
        Assertions.assertEquals(
                "second", singleField(engine, new Object[] {null, "second", "third"}));
        Assertions.assertEquals("third", singleField(engine, new Object[] {null, null, "third"}));
        Assertions.assertNull(singleField(engine, new Object[] {null, null, null}));
        engine.close();
    }

    @Test
    void testNullif() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT NULLIF(name, 'N/A') AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertNull(singleField(engine, new Object[] {1, "N/A", 25}));
        Assertions.assertEquals("Alice", singleField(engine, new Object[] {2, "Alice", 25}));
        engine.close();
    }

    @Test
    void testIsNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name IS NULL AS is_null FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(true, singleField(engine, new Object[] {1, null, 25}));
        Assertions.assertEquals(false, singleField(engine, new Object[] {2, "Alice", 25}));
        engine.close();
    }

    @Test
    void testIsNotNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name IS NOT NULL AS has_name FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(false, singleField(engine, new Object[] {1, null, 25}));
        Assertions.assertEquals(true, singleField(engine, new Object[] {2, "Alice", 25}));
        engine.close();
    }

    @Test
    void testAllColumnsNull() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id, name, age FROM test_table", "test_table", buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {null, null, null});
        Assertions.assertEquals(1, result.size());
        Assertions.assertNull(result.get(0).getField(0));
        Assertions.assertNull(result.get(0).getField(1));
        Assertions.assertNull(result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testNullArithmetic() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a + b AS sum_val FROM t", "t", numericRowType());

        Assertions.assertNull(singleField(engine, new Object[] {null, 5}));
        Assertions.assertNull(singleField(engine, new Object[] {5, null}));
        engine.close();
    }

    @Test
    void testNullInConcat() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name || '-suffix' AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testWhereEquals() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age = 25", "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 26}).size());
        engine.close();
    }

    @Test
    void testWhereNotEquals() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age <> 25", "test_table", buildRowType());

        Assertions.assertEquals(0, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 26}).size());
        engine.close();
    }

    @Test
    void testWhereLessThan() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age < 18", "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 10}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 18}).size());
        engine.close();
    }

    @Test
    void testWhereGreaterThanOrEqual() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age >= 18", "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 18}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 17}).size());
        engine.close();
    }

    @Test
    void testWhereAnd() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age > 18 AND name = 'Alice'",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Bob", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "Alice", 10}).size());
        engine.close();
    }

    @Test
    void testWhereOr() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age < 10 OR age > 60",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 5}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 70}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "C", 30}).size());
        engine.close();
    }

    @Test
    void testWhereNot() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE NOT (age > 18)",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 10}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 25}).size());
        engine.close();
    }

    @Test
    void testWhereIn() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age IN (18, 25, 30)",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 20}).size());
        engine.close();
    }

    @Test
    void testWhereNotIn() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age NOT IN (18, 25, 30)",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(0, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 20}).size());
        engine.close();
    }

    @Test
    void testWhereBetween() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age BETWEEN 20 AND 30",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 20}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {3, "C", 30}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {4, "D", 15}).size());
        engine.close();
    }

    @Test
    void testWhereNotBetween() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age NOT BETWEEN 20 AND 30",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(0, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 15}).size());
        engine.close();
    }

    @Test
    void testWhereLike() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE 'A%'",
                        "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Bob", 20}).size());
        engine.close();
    }

    @Test
    void testWhereLikeSuffix() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE '%nel'",
                        "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "connector", 25}).size());
        engine.close();
    }

    @Test
    void testWhereLikeMiddle() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE '%tunn%'",
                        "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "connector", 25}).size());
        engine.close();
    }

    @Test
    void testWhereLikeSingleChar() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE 'A____'",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Al", 25}).size());
        engine.close();
    }

    @Test
    void testWhereNotLike() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name NOT LIKE 'A%'",
                        "test_table", buildRowType());

        Assertions.assertEquals(0, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "Bob", 25}).size());
        engine.close();
    }

    @Test
    void testWhereIsNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name IS NULL",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, null, 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Alice", 25}).size());
        engine.close();
    }

    @Test
    void testComplexWhereCondition() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table "
                                + "WHERE (age >= 18 AND name LIKE 'A%') OR age > 60",
                        "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Bob", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {3, "Bob", 70}).size());
        engine.close();
    }

    @Test
    void testCastIntToString() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS VARCHAR) AS age_str FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("25", singleField(engine, new Object[] {1, "Alice", 25}));
        engine.close();
    }

    @Test
    void testCastStringToInt() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(name AS INTEGER) AS val FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(42, singleField(engine, new Object[] {1, "42", 25}));
        engine.close();
    }

    @Test
    void testCastIntToDouble() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS DOUBLE) AS dbl FROM test_table",
                        "test_table",
                        buildRowType());

        Object result = singleField(engine, new Object[] {1, "A", 25});
        Assertions.assertEquals(25.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testCastIntToBigint() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS BIGINT) AS big FROM test_table",
                        "test_table",
                        buildRowType());

        Object result = singleField(engine, new Object[] {1, "A", 25});
        Assertions.assertEquals(25L, ((Number) result).longValue());
        engine.close();
    }

    @Test
    void testCastBooleanToVarchar() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"flag"}, new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT CAST(flag AS VARCHAR) AS res FROM t", "t", rt);

        Assertions.assertEquals("TRUE", singleField(engine, new Object[] {true}));
        Assertions.assertEquals("FALSE", singleField(engine, new Object[] {false}));
        engine.close();
    }

    @Test
    void testBooleanExpressions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, age > 18 AS is_adult, name IS NOT NULL AS has_name "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(true, result.get(0).getField(1));
        Assertions.assertEquals(true, result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testBooleanAnd() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT (age > 18 AND age < 65) AS working_age FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(true, singleField(engine, new Object[] {1, "A", 30}));
        Assertions.assertEquals(false, singleField(engine, new Object[] {2, "B", 10}));
        Assertions.assertEquals(false, singleField(engine, new Object[] {3, "C", 70}));
        engine.close();
    }

    @Test
    void testBooleanOr() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT (age < 10 OR age > 60) AS extreme_age FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(true, singleField(engine, new Object[] {1, "A", 5}));
        Assertions.assertEquals(true, singleField(engine, new Object[] {2, "B", 70}));
        Assertions.assertEquals(false, singleField(engine, new Object[] {3, "C", 30}));
        engine.close();
    }

    @Test
    void testBooleanNot() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT NOT (age > 18) AS is_minor FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(true, singleField(engine, new Object[] {1, "A", 10}));
        Assertions.assertEquals(false, singleField(engine, new Object[] {2, "B", 25}));
        engine.close();
    }

    @Test
    void testBooleanColumnDirect() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"id", "active"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.BOOLEAN_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT * FROM t WHERE active", "t", rt);

        Assertions.assertEquals(1, exec(engine, new Object[] {1, true}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, false}).size());
        engine.close();
    }

    @Test
    void testDateType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "dt"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, dt FROM t", "t", rowType);

        LocalDate date = LocalDate.of(2024, 6, 15);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, date});
        Assertions.assertInstanceOf(LocalDate.class, result.get(0).getField(1));
        Assertions.assertEquals(date, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testTimestampType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "ts"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, ts FROM t", "t", rowType);

        LocalDateTime ts = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, ts});
        Assertions.assertInstanceOf(LocalDateTime.class, result.get(0).getField(1));
        Assertions.assertEquals(ts, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testTimeType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "tm"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_TIME_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, tm FROM t", "t", rowType);

        LocalTime time = LocalTime.of(14, 30, 0);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, time});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(time, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testTimeTypeMillisecondRoundTrip() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "tm"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_TIME_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, tm FROM t", "t", rowType);

        LocalTime time = LocalTime.of(14, 30, 12, 345_000_000);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, time});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(time, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testTimeTypeNanoTruncation() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "tm"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_TIME_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, tm FROM t", "t", rowType);

        LocalTime input = LocalTime.of(14, 30, 12, 345_678_912);
        LocalTime expected = LocalTime.of(14, 30, 12, 345_000_000);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, input});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(expected, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testCurrentDateOutputType() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CURRENT_DATE AS today FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(1, outType.getTotalFields());
        Assertions.assertEquals("today", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testDateExtractYearOutputType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT EXTRACT(YEAR FROM dt) AS yr FROM t", "t", rowType);

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(1, outType.getTotalFields());
        Assertions.assertEquals("yr", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testDateExtractMonthOutputType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT EXTRACT(MONTH FROM dt) AS mon FROM t", "t", rowType);

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("mon", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testDateExtractDayOutputType() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT EXTRACT(DAY FROM dt) AS d FROM t", "t", rowType);

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("d", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testNullDate() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "dt"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, dt FROM t", "t", rowType);

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, null});
        Assertions.assertNull(result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testDateEpoch() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT dt FROM t", "t", rowType);

        LocalDate epoch = LocalDate.of(1970, 1, 1);
        Object result = singleField(engine, new Object[] {epoch});
        Assertions.assertEquals(epoch, result);
        engine.close();
    }

    @Test
    void testMultipleNumericTypes() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "byte_val", "short_val", "long_val", "float_val", "double_val"
                        },
                        new SeaTunnelDataType[] {
                            BasicType.BYTE_TYPE,
                            BasicType.SHORT_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            BasicType.DOUBLE_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT * FROM t", "t", rowType);

        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {(byte) 1, (short) 200, 100000L, 3.14f, 2.718281828});

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(SqlType.TINYINT, outType.getFieldType(0).getSqlType());
        Assertions.assertEquals(SqlType.SMALLINT, outType.getFieldType(1).getSqlType());
        Assertions.assertEquals(SqlType.BIGINT, outType.getFieldType(2).getSqlType());
        Assertions.assertEquals(SqlType.FLOAT, outType.getFieldType(3).getSqlType());
        Assertions.assertEquals(SqlType.DOUBLE, outType.getFieldType(4).getSqlType());
        engine.close();
    }

    @Test
    void testDecimalType() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"amount"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT amount FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {new BigDecimal("123.45")});
        Assertions.assertNotNull(result);
        engine.close();
    }

    @Test
    void testDecimalArithmetic() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {new DecimalType(10, 2), new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT a + b AS total FROM t", "t", rt);

        Object result =
                singleField(
                        engine, new Object[] {new BigDecimal("100.50"), new BigDecimal("200.75")});
        Assertions.assertNotNull(result);
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, new BigDecimal("301.25").compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testByteValueBoundary() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.BYTE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Byte.MAX_VALUE});
        Assertions.assertEquals(Byte.MAX_VALUE, ((Number) result).byteValue());
        engine.close();
    }

    @Test
    void testShortValueBoundary() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.SHORT_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Short.MAX_VALUE});
        Assertions.assertEquals(Short.MAX_VALUE, ((Number) result).shortValue());
        engine.close();
    }

    @Test
    void testLongValueBoundary() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Long.MAX_VALUE});
        Assertions.assertEquals(Long.MAX_VALUE, ((Number) result).longValue());
        engine.close();
    }

    @Test
    void testFloatPrecision() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.FLOAT_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {0.1f});
        Assertions.assertEquals(0.1f, ((Number) result).floatValue(), 0.0001f);
        engine.close();
    }

    @Test
    void testDoubleNaN() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Double.NaN});
        Assertions.assertTrue(Double.isNaN(((Number) result).doubleValue()));
        engine.close();
    }

    @Test
    void testDoubleInfinity() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {Double.POSITIVE_INFINITY});
        Assertions.assertTrue(Double.isInfinite(((Number) result).doubleValue()));
        engine.close();
    }

    @Test
    void testOutputRowType() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, UPPER(name) AS name_upper FROM test_table",
                        "test_table",
                        buildRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(2, outType.getTotalFields());
        Assertions.assertEquals("id", outType.getFieldName(0));
        Assertions.assertEquals("name_upper", outType.getFieldName(1));
        engine.close();
    }

    @Test
    void testOutputRowTypeWithAlias() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id AS user_id, name AS user_name, age AS user_age FROM test_table",
                        "test_table",
                        buildRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("user_id", outType.getFieldName(0));
        Assertions.assertEquals("user_name", outType.getFieldName(1));
        Assertions.assertEquals("user_age", outType.getFieldName(2));
        engine.close();
    }

    @Test
    void testOutputTypeSingleColumn() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(1, outType.getTotalFields());
        Assertions.assertEquals(SqlType.STRING, outType.getFieldType(0).getSqlType());
        engine.close();
    }

    @Test
    void testOutputTypeComputedColumn() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT age * 2 AS doubled FROM test_table", "test_table", buildRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("doubled", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testOutputTypeManyColumns() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, name, age, id + age AS sum_val, "
                                + "UPPER(name) AS upper_name, age > 18 AS adult "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(6, outType.getTotalFields());
        engine.close();
    }

    @Test
    void testConstantExpression() {
        CalciteSQLEngine engine =
                createAndInit("SELECT 42 AS answer FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(42, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testStringConstant() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT 'seatunnel' AS name FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals("seatunnel", singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testNullConstant() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(NULL AS VARCHAR) AS null_val FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertNull(singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testConstantArithmetic() {
        CalciteSQLEngine engine =
                createAndInit("SELECT 2 + 3 AS five FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(5, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testEmptyString() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals("", singleField(engine, new Object[] {1, "", 25}));
        engine.close();
    }

    @Test
    void testSpecialCharactersInString() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(
                "sea'tunnel", singleField(engine, new Object[] {1, "sea'tunnel", 25}));
        engine.close();
    }

    @Test
    void testLongString() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("seatunnel");
        }
        String longStr = sb.toString();
        Assertions.assertEquals(longStr, singleField(engine, new Object[] {1, longStr, 25}));
        engine.close();
    }

    @Test
    void testSingleColumnTable() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);

        Assertions.assertEquals(42, singleField(engine, new Object[] {42}));
        engine.close();
    }

    @Test
    void testManyColumnsTable() {
        String[] names = new String[20];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[20];
        Object[] values = new Object[20];
        for (int i = 0; i < 20; i++) {
            names[i] = "col_" + i;
            types[i] = BasicType.INT_TYPE;
            values[i] = i;
        }
        SeaTunnelRowType rt = new SeaTunnelRowType(names, types);
        CalciteSQLEngine engine = createAndInit("SELECT * FROM t", "t", rt);

        List<SeaTunnelRow> result = exec(engine, values);
        Assertions.assertEquals(20, result.get(0).getArity());
        for (int i = 0; i < 20; i++) {
            Assertions.assertEquals(i, result.get(0).getField(i));
        }
        engine.close();
    }

    @Test
    void testEngineReuse() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, name FROM test_table WHERE age > 20",
                        "test_table",
                        buildRowType());

        for (int i = 0; i < 100; i++) {
            int age = i % 40;
            List<SeaTunnelRow> result = exec(engine, new Object[] {i, "user_" + i, age});
            if (age > 20) {
                Assertions.assertEquals(1, result.size());
            } else {
                Assertions.assertTrue(result.isEmpty());
            }
        }
        engine.close();
    }

    @Test
    void testEngineReuseWithDifferentData() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT UPPER(name) AS upper_name FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("ALICE", singleField(engine, new Object[] {1, "alice", 25}));
        Assertions.assertEquals("BOB", singleField(engine, new Object[] {2, "bob", 30}));
        Assertions.assertEquals("", singleField(engine, new Object[] {3, "", 20}));
        engine.close();
    }

    @Test
    void testMultipleEngineInstances() {
        CalciteSQLEngine engine1 =
                createAndInit("SELECT id FROM test_table", "test_table", buildRowType());
        CalciteSQLEngine engine2 =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(1, singleField(engine1, new Object[] {1, "Alice", 25}));
        Assertions.assertEquals("Alice", singleField(engine2, new Object[] {1, "Alice", 25}));

        engine1.close();
        engine2.close();
    }

    @Test
    void testCloseAndReinit() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine("SELECT id FROM test_table", "test_table", buildRowType());
        engine.init();
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();

        engine.init();
        Assertions.assertEquals(2, singleField(engine, new Object[] {2, "B", 30}));
        engine.close();
    }

    @Test
    void testInvalidSqlThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine("SELECTTTTT * FROM test_table", "test_table", buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testInvalidColumnThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine(
                        "SELECT nonexistent_col FROM test_table", "test_table", buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testEmptySqlThrows() {
        CalciteSQLEngine engine = new CalciteSQLEngine("", "test_table", buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testInvalidFunctionThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine(
                        "SELECT NO_SUCH_FUNCTION(name) FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testWrongTableNameThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine("SELECT * FROM wrong_table", "test_table", buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testSyntaxErrorThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine("SELECT FROM test_table WHERE", "test_table", buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testUnclosedStringThrows() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine(
                        "SELECT * FROM test_table WHERE name = 'unclosed",
                        "test_table",
                        buildRowType());
        Assertions.assertThrows(TransformException.class, engine::init);
    }

    @Test
    void testMixedComputedColumns() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, "
                                + "UPPER(name) AS upper_name, "
                                + "age * 2 AS double_age, "
                                + "CHAR_LENGTH(name) AS name_len, "
                                + "age > 18 AS is_adult "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "seatunnel", 25});
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("SEATUNNEL", result.get(0).getField(1));
        Assertions.assertEquals(50, result.get(0).getField(2));
        Assertions.assertEquals(9, result.get(0).getField(3));
        Assertions.assertEquals(true, result.get(0).getField(4));
        engine.close();
    }

    @Test
    void testWhereWithComputedValue() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE age * 2 > 50",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 30}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 20}).size());
        engine.close();
    }

    @Test
    void testWhereWithStringFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE CHAR_LENGTH(name) > 5",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "zeta", 25}).size());
        engine.close();
    }

    @Test
    void testSubqueryLiteral() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, age, CASE "
                                + "WHEN age IN (18, 25, 30, 60) THEN 'milestone' "
                                + "ELSE 'normal' END AS tag "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> r1 = exec(engine, new Object[] {1, "A", 25});
        Assertions.assertTrue(r1.get(0).getField(2).toString().contains("milestone"));
        List<SeaTunnelRow> r2 = exec(engine, new Object[] {2, "B", 22});
        Assertions.assertTrue(r2.get(0).getField(2).toString().contains("normal"));
        engine.close();
    }

    @Test
    void testCaseInsensitiveColumn() {
        CalciteSQLEngine engine =
                createAndInit("SELECT ID, NAME, AGE FROM test_table", "test_table", buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testCaseInsensitiveFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT upper(name) AS res FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals("ALICE", singleField(engine, new Object[] {1, "Alice", 25}));
        engine.close();
    }

    @Test
    void testCaseInsensitiveKeywords() {
        CalciteSQLEngine engine =
                createAndInit(
                        "select id from test_table where age > 20", "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        engine.close();
    }

    @Test
    void testCountConstant() {
        CalciteSQLEngine engine =
                createAndInit("SELECT 1 AS cnt FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testConcatMultiple() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name || ' (id=' || CAST(id AS VARCHAR) || ')' AS label FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("Alice (id=1)", singleField(engine, new Object[] {1, "Alice", 25}));
        engine.close();
    }

    @Test
    void testSubstringWithLength() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT SUBSTRING(name, 1, 3) AS prefix FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("sea", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testSubstringBeyondLength() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT SUBSTRING(name, 1, 100) AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals("abc", singleField(engine, new Object[] {1, "abc", 25}));
        engine.close();
    }

    @Test
    void testNestedStringFunctions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT UPPER(TRIM(name)) AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Assertions.assertEquals(
                "SEATUNNEL", singleField(engine, new Object[] {1, "  seatunnel  ", 25}));
        engine.close();
    }

    @Test
    void testDistinctFromAllRows() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id + 0 AS same FROM test_table", "test_table", buildRowType());

        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testMultipleWhereConditionsParenthesized() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE (age > 10 AND age < 30) AND (name LIKE 'A%' OR name LIKE 'B%')",
                        "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "Bob", 20}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "Charlie", 25}).size());
        engine.close();
    }

    @Test
    void testTableId() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id FROM test_table", "test_table", buildRowType());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "A", 25});
        input.setTableId("source_table");
        List<SeaTunnelRow> result = engine.execute(input);
        Assertions.assertEquals("source_table", result.get(0).getTableId());
        engine.close();
    }

    @Test
    void testFilteredRowReturnsEmpty() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE 1 = 0", "test_table", buildRowType());

        Assertions.assertTrue(exec(engine, new Object[] {1, "A", 25}).isEmpty());
        engine.close();
    }

    @Test
    void testAlwaysTrueWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE 1 = 1", "test_table", buildRowType());

        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        engine.close();
    }

    @Test
    void testUpperNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT UPPER(name) AS res FROM test_table", "test_table", buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testLowerNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT LOWER(name) AS res FROM test_table", "test_table", buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testCharLengthNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CHAR_LENGTH(name) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testSubstringNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT SUBSTRING(name, 1, 3) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testTrimNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(name) AS res FROM test_table", "test_table", buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testReplaceNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT REPLACE(name, 'a', 'b') AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testPositionNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT POSITION('x' IN name) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testAbsNull() {
        CalciteSQLEngine engine =
                createAndInit("SELECT ABS(a) AS res FROM t", "t", numericRowType());
        Assertions.assertNull(singleField(engine, new Object[] {null, 0}));
        engine.close();
    }

    @Test
    void testCeilNull() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT CEIL(val) AS res FROM t", "t", rt);
        Assertions.assertNull(singleField(engine, new Object[] {null}));
        engine.close();
    }

    @Test
    void testFloorNull() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT FLOOR(val) AS res FROM t", "t", rt);
        Assertions.assertNull(singleField(engine, new Object[] {null}));
        engine.close();
    }

    @Test
    void testSignPositive() {
        CalciteSQLEngine engine =
                createAndInit("SELECT SIGN(a) AS res FROM t", "t", numericRowType());
        Assertions.assertEquals(1, singleField(engine, new Object[] {42, 0}));
        engine.close();
    }

    @Test
    void testSignNegative() {
        CalciteSQLEngine engine =
                createAndInit("SELECT SIGN(a) AS res FROM t", "t", numericRowType());
        Assertions.assertEquals(-1, singleField(engine, new Object[] {-42, 0}));
        engine.close();
    }

    @Test
    void testSignZero() {
        CalciteSQLEngine engine =
                createAndInit("SELECT SIGN(a) AS res FROM t", "t", numericRowType());
        Assertions.assertEquals(0, singleField(engine, new Object[] {0, 0}));
        engine.close();
    }

    @Test
    void testCeilNegative() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT CEIL(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {-3.2});
        Assertions.assertEquals(-3.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testFloorNegative() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT FLOOR(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {-3.2});
        Assertions.assertEquals(-4.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testSqrtZero() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT SQRT(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {0.0});
        Assertions.assertEquals(0.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testPowerZeroExponent() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"base_val", "exponent"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT POWER(base_val, exponent) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {999.0, 0.0});
        Assertions.assertEquals(1.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testPowerNegativeExponent() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"base_val", "exponent"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT POWER(base_val, exponent) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {2.0, -1.0});
        Assertions.assertEquals(0.5, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testModNegative() {
        CalciteSQLEngine engine =
                createAndInit("SELECT MOD(a, b) AS res FROM t", "t", numericRowType());
        Object result = singleField(engine, new Object[] {-10, 3});
        Assertions.assertEquals(-1, result);
        engine.close();
    }

    @Test
    void testNestedArithmetic() {
        CalciteSQLEngine engine =
                createAndInit("SELECT (a + b) * (a - b) AS res FROM t", "t", numericRowType());
        Assertions.assertEquals(91, singleField(engine, new Object[] {10, 3}));
        engine.close();
    }

    @Test
    void testArithmeticPrecedence() {
        CalciteSQLEngine engine =
                createAndInit("SELECT a + b * 2 AS res FROM t", "t", numericRowType());
        Assertions.assertEquals(16, singleField(engine, new Object[] {10, 3}));
        engine.close();
    }

    @Test
    void testNullEqualsNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name = name", "test_table", buildRowType());
        Assertions.assertEquals(0, exec(engine, new Object[] {1, null, 25}).size());
        engine.close();
    }

    @Test
    void testNullNotEqualsAnything() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name = 'Alice'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(0, exec(engine, new Object[] {1, null, 25}).size());
        engine.close();
    }

    @Test
    void testNullInBetween() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age BETWEEN 10 AND 30",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(0, exec(engine, new Object[] {1, "A", null}).size());
        engine.close();
    }

    @Test
    void testNullInLike() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE 'A%'",
                        "test_table", buildRowType());
        Assertions.assertEquals(0, exec(engine, new Object[] {1, null, 25}).size());
        engine.close();
    }

    @Test
    void testNullInIn() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age IN (10, 20, 30)",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(0, exec(engine, new Object[] {1, "A", null}).size());
        engine.close();
    }

    @Test
    void testCoalesceAllNonNull() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT COALESCE(a, b) AS res FROM t", "t", rt);
        Assertions.assertEquals("first", singleField(engine, new Object[] {"first", "second"}));
        engine.close();
    }

    @Test
    void testNullifBothSame() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT NULLIF(name, name) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, "Alice", 25}));
        engine.close();
    }

    @Test
    void testNullifWithNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT NULLIF(name, 'X') AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, null, 25}));
        engine.close();
    }

    @Test
    void testSelectColumnReorder() {
        CalciteSQLEngine engine =
                createAndInit("SELECT age, name, id FROM test_table", "test_table", buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(25, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        Assertions.assertEquals(1, result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testSelectDuplicateColumn() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name AS n1, name AS n2 FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals("Alice", result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testSelectSameColumnMultipleTimes() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, id + 1 AS id_next, id * 2 AS id_double FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {5, "A", 25});
        Assertions.assertEquals(5, result.get(0).getField(0));
        Assertions.assertEquals(6, result.get(0).getField(1));
        Assertions.assertEquals(10, result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testCastIntToFloat() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS FLOAT) AS res FROM test_table",
                        "test_table",
                        buildRowType());

        Object result = singleField(engine, new Object[] {1, "A", 25});
        Assertions.assertNotNull(result);
        Assertions.assertEquals(25.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testCastStringToDouble() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT CAST(val AS DOUBLE) AS res FROM t", "t", rt);

        Object result = singleField(engine, new Object[] {"3.14"});
        Assertions.assertEquals(3.14, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testCastNullToInt() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(NULL AS INTEGER) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testCastNullToDouble() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(NULL AS DOUBLE) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testCastNullToBigint() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(NULL AS BIGINT) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertNull(singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testCastChain() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(CAST(age AS VARCHAR) AS INTEGER) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(25, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testCastDecimalToInt() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});
        CalciteSQLEngine engine =
                createAndInit("SELECT CAST(val AS INTEGER) AS res FROM t", "t", rt);
        Assertions.assertEquals(123, singleField(engine, new Object[] {new BigDecimal("123.99")}));
        engine.close();
    }

    @Test
    void testCastIntToDecimal() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS DECIMAL(10,2)) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Object result = singleField(engine, new Object[] {1, "A", 25});
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, new BigDecimal("25.00").compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testDecimalSubtraction() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {new DecimalType(10, 2), new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT a - b AS diff FROM t", "t", rt);
        Object result =
                singleField(
                        engine, new Object[] {new BigDecimal("100.50"), new BigDecimal("30.25")});
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, new BigDecimal("70.25").compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testDecimalMultiplication() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {new DecimalType(10, 2), new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT a * b AS product FROM t", "t", rt);
        Object result =
                singleField(engine, new Object[] {new BigDecimal("10.00"), new BigDecimal("3.50")});
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, new BigDecimal("35.0000").compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testDecimalDivision() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {new DecimalType(10, 2), new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT a / b AS quotient FROM t", "t", rt);
        Object result =
                singleField(
                        engine, new Object[] {new BigDecimal("100.00"), new BigDecimal("4.00")});
        Assertions.assertNotNull(result);
        Assertions.assertInstanceOf(BigDecimal.class, result);
        engine.close();
    }

    @Test
    void testDecimalComparisonInWhere() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"amount"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});
        CalciteSQLEngine engine =
                createAndInit("SELECT amount FROM t WHERE amount > 100.00", "t", rt);
        Assertions.assertEquals(1, exec(engine, new Object[] {new BigDecimal("200.50")}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {new BigDecimal("50.00")}).size());
        engine.close();
    }

    @Test
    void testMixedIntDoubleArithmetic() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"i", "d"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT i + d AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {10, 3.14});
        Assertions.assertEquals(13.14, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testMixedIntLongArithmetic() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"i", "l"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT i + l AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {10, 100000L});
        Assertions.assertEquals(100010L, ((Number) result).longValue());
        engine.close();
    }

    @Test
    void testBooleanWithNull() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"flag"}, new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT flag FROM t", "t", rt);
        Assertions.assertNull(singleField(engine, new Object[] {null}));
        engine.close();
    }

    @Test
    void testBooleanInCaseWhen() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"active"}, new SeaTunnelDataType[] {BasicType.BOOLEAN_TYPE});
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT TRIM(CASE WHEN active THEN 'on' ELSE 'off' END) AS status FROM t",
                        "t",
                        rt);
        Assertions.assertEquals("on", singleField(engine, new Object[] {true}));
        Assertions.assertEquals("off", singleField(engine, new Object[] {false}));
        engine.close();
    }

    @Test
    void testMultipleCaseWhenInSelect() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS cat1, "
                                + "CASE WHEN name IS NULL THEN 'anon' ELSE name END AS cat2 "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, null, 10});
        Assertions.assertEquals("minor", result.get(0).getField(0));
        Assertions.assertEquals("anon", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testCaseWhenWithArithmetic() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN age > 18 THEN age * 2 ELSE age END AS res "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(50, singleField(engine, new Object[] {1, "A", 25}));
        Assertions.assertEquals(10, singleField(engine, new Object[] {2, "B", 10}));
        engine.close();
    }

    @Test
    void testCaseWhenInWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE CASE WHEN age > 18 THEN 1 ELSE 0 END = 1",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 10}).size());
        engine.close();
    }

    @Test
    void testWhereWithMultipleColumnsComplex() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE age > 18 AND CHAR_LENGTH(name) > 3 AND id > 0",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Al", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "Alice", 10}).size());
        engine.close();
    }

    @Test
    void testStringComparisonInWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name > 'M'", "test_table", buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Zeta", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Alice", 25}).size());
        engine.close();
    }

    @Test
    void testStringEqualityInWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name = 'seatunnel'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "SeaTunnel", 25}).size());
        engine.close();
    }

    @Test
    void testInWithStrings() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name IN ('Alice', 'Bob', 'Charlie')",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "Bob", 30}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "Dave", 20}).size());
        engine.close();
    }

    @Test
    void testLikeEmptyPattern() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE '%'",
                        "test_table", buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "anything", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "", 25}).size());
        engine.close();
    }

    @Test
    void testLikeExactMatch() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name LIKE 'seatunnel'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "seatunnel2", 25}).size());
        engine.close();
    }

    @Test
    void testWhereOnComputedExpression() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE UPPER(name) = 'SEATUNNEL'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "SeaTunnel", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "seatunnel", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "zeta", 25}).size());
        engine.close();
    }

    @Test
    void testWhereOnConcatExpression() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE name || '-suffix' = 'Alice-suffix'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Bob", 25}).size());
        engine.close();
    }

    @Test
    void testTimestampExtractOutputType() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT EXTRACT(HOUR FROM ts) AS h FROM t", "t", rt);
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(1, outType.getTotalFields());
        Assertions.assertEquals("h", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testDateWithIntFilter() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"id", "dt"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, dt FROM t WHERE id > 0", "t", rt);
        List<SeaTunnelRow> rows = exec(engine, new Object[] {1, LocalDate.of(2024, 6, 15)});
        Assertions.assertEquals(1, rows.size());
        Assertions.assertInstanceOf(LocalDate.class, rows.get(0).getField(1));
        engine.close();
    }

    @Test
    void testTimestampWithIntFilter() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"id", "ts"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT id, ts FROM t WHERE id > 0", "t", rt);
        List<SeaTunnelRow> rows =
                exec(engine, new Object[] {1, LocalDateTime.of(2024, 6, 15, 10, 30, 0)});
        Assertions.assertEquals(1, rows.size());
        Assertions.assertInstanceOf(LocalDateTime.class, rows.get(0).getField(1));
        engine.close();
    }

    @Test
    void testCurrentTimestampOutputType() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CURRENT_TIMESTAMP AS ts_now FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("ts_now", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testCurrentTimeOutputType() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CURRENT_TIME AS t_now FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals("t_now", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testDateLeapYear() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT dt FROM t", "t", rt);
        LocalDate leapDay = LocalDate.of(2024, 2, 29);
        Object result = singleField(engine, new Object[] {leapDay});
        Assertions.assertEquals(leapDay, result);
        engine.close();
    }

    @Test
    void testDateFarFuture() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT dt FROM t", "t", rt);
        LocalDate future = LocalDate.of(9999, 12, 31);
        Object result = singleField(engine, new Object[] {future});
        Assertions.assertEquals(future, result);
        engine.close();
    }

    @Test
    void testTimestampMidnight() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT ts FROM t", "t", rt);
        LocalDateTime midnight = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        Object result = singleField(engine, new Object[] {midnight});
        Assertions.assertEquals(midnight, result);
        engine.close();
    }

    @Test
    void testTimestampEndOfDay() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT ts FROM t", "t", rt);
        LocalDateTime endOfDay = LocalDateTime.of(2024, 12, 31, 23, 59, 59);
        Object result = singleField(engine, new Object[] {endOfDay});
        Assertions.assertEquals(endOfDay, result);
        engine.close();
    }

    @Test
    void testByteMinValue() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.BYTE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Byte.MIN_VALUE});
        Assertions.assertEquals(Byte.MIN_VALUE, ((Number) result).byteValue());
        engine.close();
    }

    @Test
    void testShortMinValue() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.SHORT_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Short.MIN_VALUE});
        Assertions.assertEquals(Short.MIN_VALUE, ((Number) result).shortValue());
        engine.close();
    }

    @Test
    void testLongMinValue() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Long.MIN_VALUE});
        Assertions.assertEquals(Long.MIN_VALUE, ((Number) result).longValue());
        engine.close();
    }

    @Test
    void testDoubleNegativeInfinity() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Double.NEGATIVE_INFINITY});
        Assertions.assertTrue(Double.isInfinite(((Number) result).doubleValue()));
        Assertions.assertTrue(((Number) result).doubleValue() < 0);
        engine.close();
    }

    @Test
    void testDoubleMinValue() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Double.MIN_VALUE});
        Assertions.assertEquals(Double.MIN_VALUE, ((Number) result).doubleValue(), 0.0);
        engine.close();
    }

    @Test
    void testDoubleMaxValue() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {Double.MAX_VALUE});
        Assertions.assertEquals(Double.MAX_VALUE, ((Number) result).doubleValue(), 0.0);
        engine.close();
    }

    @Test
    void testDecimalZero() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {BigDecimal.ZERO});
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, BigDecimal.ZERO.compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testDecimalNegative() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});
        CalciteSQLEngine engine = createAndInit("SELECT val FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {new BigDecimal("-99.99")});
        Assertions.assertInstanceOf(BigDecimal.class, result);
        Assertions.assertEquals(0, new BigDecimal("-99.99").compareTo((BigDecimal) result));
        engine.close();
    }

    @Test
    void testOutputTypeForCaseWhen() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END AS label "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(1, outType.getTotalFields());
        Assertions.assertEquals("label", outType.getFieldName(0));
        engine.close();
    }

    @Test
    void testOutputTypeForArithmetic() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT age + 1 AS next_age, age * 2 AS double_age FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(2, outType.getTotalFields());
        Assertions.assertEquals("next_age", outType.getFieldName(0));
        Assertions.assertEquals("double_age", outType.getFieldName(1));
        engine.close();
    }

    @Test
    void testOutputTypeForBoolean() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT age > 18 AS is_adult FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(SqlType.BOOLEAN, outType.getFieldType(0).getSqlType());
        engine.close();
    }

    @Test
    void testOutputTypeForCast() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(age AS BIGINT) AS big_age FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(SqlType.BIGINT, outType.getFieldType(0).getSqlType());
        engine.close();
    }

    @Test
    void testOutputTypeForConcat() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT name || '-suffix' AS combined FROM test_table",
                        "test_table",
                        buildRowType());
        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(SqlType.STRING, outType.getFieldType(0).getSqlType());
        engine.close();
    }

    @Test
    void testStringWithNewline() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(
                "line1\nline2", singleField(engine, new Object[] {1, "line1\nline2", 25}));
        engine.close();
    }

    @Test
    void testStringWithTab() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(
                "col1\tcol2", singleField(engine, new Object[] {1, "col1\tcol2", 25}));
        engine.close();
    }

    @Test
    void testStringWithBackslash() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(
                "path\\to\\file", singleField(engine, new Object[] {1, "path\\to\\file", 25}));
        engine.close();
    }

    @Test
    void testStringWithDoubleQuotes() {
        CalciteSQLEngine engine =
                createAndInit("SELECT name FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(
                "say \"hi\"", singleField(engine, new Object[] {1, "say \"hi\"", 25}));
        engine.close();
    }

    @Test
    void testReplaceEmptyString() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT REPLACE(name, '-', '') AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "seatunnel", singleField(engine, new Object[] {1, "sea-tunnel", 25}));
        engine.close();
    }

    @Test
    void testReplaceNoMatch() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT REPLACE(name, 'xyz', 'abc') AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "seatunnel", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testReplaceMultipleOccurrences() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT REPLACE(name, 'e', 'E') AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "sEatunnEl", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testSubstringStartAtEnd() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT SUBSTRING(name, 10) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals("", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testOverlayAtStart() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT OVERLAY(name PLACING 'NEW' FROM 1 FOR 3) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "NEWtunnel", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testOverlayAtEnd() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT OVERLAY(name PLACING 'XYZ' FROM 7 FOR 3) AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "seatunXYZ", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testDeepNestedFunctions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT UPPER(TRIM(REPLACE(LOWER(name), 'sea', 'lake'))) AS res "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "LAKETUNNEL", singleField(engine, new Object[] {1, "  SeaTunnel  ", 25}));
        engine.close();
    }

    @Test
    void testComplexSelectWithManyExpressions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id, "
                                + "name, "
                                + "age, "
                                + "UPPER(name) AS upper_name, "
                                + "LOWER(name) AS lower_name, "
                                + "CHAR_LENGTH(name) AS name_len, "
                                + "age + 1 AS next_age, "
                                + "age * 2 AS double_age, "
                                + "age > 18 AS is_adult, "
                                + "CASE WHEN age >= 65 THEN 'senior' ELSE 'non-senior' END AS cat "
                                + "FROM test_table",
                        "test_table",
                        buildRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Seatunnel", 30});
        Assertions.assertEquals(10, result.get(0).getArity());
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Seatunnel", result.get(0).getField(1));
        Assertions.assertEquals(30, result.get(0).getField(2));
        Assertions.assertEquals("SEATUNNEL", result.get(0).getField(3));
        Assertions.assertEquals("seatunnel", result.get(0).getField(4));
        Assertions.assertEquals(9, result.get(0).getField(5));
        Assertions.assertEquals(31, result.get(0).getField(6));
        Assertions.assertEquals(60, result.get(0).getField(7));
        Assertions.assertEquals(true, result.get(0).getField(8));
        engine.close();
    }

    @Test
    void testWhereWithBooleanExpression() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE (age > 18) = true",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "B", 10}).size());
        engine.close();
    }

    @Test
    void testWhereMultipleOrConditions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table "
                                + "WHERE name = 'Alice' OR name = 'Bob' OR name = 'Charlie'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "Bob", 30}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {3, "Charlie", 35}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {4, "Dave", 40}).size());
        engine.close();
    }

    @Test
    void testBetweenWithStrings() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE name BETWEEN 'A' AND 'M'",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Zeta", 25}).size());
        engine.close();
    }

    @Test
    void testTableIdPreservedWithFilter() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE age > 18", "test_table", buildRowType());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "Alice", 25});
        input.setTableId("my_source");
        List<SeaTunnelRow> result = engine.execute(input);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("my_source", result.get(0).getTableId());
        engine.close();
    }

    @Test
    void testTableIdPreservedWhenFiltered() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table WHERE age > 30", "test_table", buildRowType());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "Alice", 25});
        input.setTableId("my_source");
        List<SeaTunnelRow> result = engine.execute(input);
        Assertions.assertTrue(result.isEmpty());
        engine.close();
    }

    @Test
    void testDefaultTableId() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id FROM test_table", "test_table", buildRowType());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "A", 25});
        List<SeaTunnelRow> result = engine.execute(input);
        Assertions.assertEquals("", result.get(0).getTableId());
        engine.close();
    }

    @Test
    void testEngineReuseStressTest() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT UPPER(name) AS res FROM test_table", "test_table", buildRowType());

        for (int i = 0; i < 1000; i++) {
            Object result = singleField(engine, new Object[] {i, "test_" + i, i % 100});
            Assertions.assertEquals("TEST_" + i, result);
        }
        engine.close();
    }

    @Test
    void testDifferentTableNames() {
        CalciteSQLEngine engine1 =
                createAndInit("SELECT id FROM source_data", "source_data", buildRowType());
        CalciteSQLEngine engine2 =
                createAndInit("SELECT id FROM sink_data", "sink_data", buildRowType());

        Assertions.assertEquals(1, singleField(engine1, new Object[] {1, "A", 25}));
        Assertions.assertEquals(2, singleField(engine2, new Object[] {2, "B", 30}));

        engine1.close();
        engine2.close();
    }

    @Test
    void testLongTableName() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("seatunnel_");
        }
        sb.append("tbl");
        String longName = sb.toString();
        CalciteSQLEngine engine =
                createAndInit("SELECT id FROM " + longName, longName, buildRowType());
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testUnderscoreInTableName() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id FROM my_source_table", "my_source_table", buildRowType());
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testNumericTableName() {
        CalciteSQLEngine engine = createAndInit("SELECT id FROM t123", "t123", buildRowType());
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testColumnNameWithUnderscore() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"user_id", "user_name", "user_age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        CalciteSQLEngine engine = createAndInit("SELECT user_id, user_name FROM t", "t", rt);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testMixedCaseColumnName() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"userId", "userName"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT userId, userName FROM t", "t", rt);
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice"});
        Assertions.assertEquals(1, result.get(0).getField(0));
        Assertions.assertEquals("Alice", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testSelectStarWithWhere() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT * FROM test_table WHERE age > 18 AND name IS NOT NULL",
                        "test_table",
                        buildRowType());
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "Alice", 25});
        Assertions.assertEquals(3, result.get(0).getArity());
        Assertions.assertEquals(1, result.get(0).getField(0));
        engine.close();
    }

    @Test
    void testSelectStarFilterAll() {
        CalciteSQLEngine engine =
                createAndInit("SELECT * FROM test_table WHERE 1 = 0", "test_table", buildRowType());
        Assertions.assertTrue(exec(engine, new Object[] {1, "A", 25}).isEmpty());
        engine.close();
    }

    @Test
    void testMultipleAliasesOnSameExpression() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT age + 1 AS next1, age + 1 AS next2 FROM test_table",
                        "test_table",
                        buildRowType());
        List<SeaTunnelRow> result = exec(engine, new Object[] {1, "A", 25});
        Assertions.assertEquals(26, result.get(0).getField(0));
        Assertions.assertEquals(26, result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testLiteralTrue() {
        CalciteSQLEngine engine =
                createAndInit("SELECT TRUE AS flag FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(true, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testLiteralFalse() {
        CalciteSQLEngine engine =
                createAndInit("SELECT FALSE AS flag FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(false, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testNegativeConstant() {
        CalciteSQLEngine engine =
                createAndInit("SELECT -1 AS neg FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(-1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testStringConcatWithConstants() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT 'prefix-' || name || '-suffix' AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "prefix-seatunnel-suffix", singleField(engine, new Object[] {1, "seatunnel", 25}));
        engine.close();
    }

    @Test
    void testArithmeticWithConstants() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT age + 100 - 50 * 2 AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(25, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testNestedCoalesce() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"a", "b"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT COALESCE(a, COALESCE(b, 'fallback')) AS res FROM t", "t", rt);
        Assertions.assertEquals("first", singleField(engine, new Object[] {"first", "second"}));
        Assertions.assertEquals("second", singleField(engine, new Object[] {null, "second"}));
        Assertions.assertEquals("fallback", singleField(engine, new Object[] {null, null}));
        engine.close();
    }

    @Test
    void testCaseWhenWithStringFunction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CASE WHEN CHAR_LENGTH(name) > 5 THEN UPPER(name) "
                                + "ELSE LOWER(name) END AS res FROM test_table",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(
                "SEATUNNEL", singleField(engine, new Object[] {1, "Seatunnel", 25}));
        Assertions.assertEquals("zeta", singleField(engine, new Object[] {2, "Zeta", 25}));
        engine.close();
    }

    @Test
    void testWhereWithNullSafeCheck() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE name IS NOT NULL AND CHAR_LENGTH(name) > 0",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, null, 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "", 25}).size());
        engine.close();
    }

    @Test
    void testWhereWithNestedOr() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE (age = 10 OR age = 20) OR (age = 30 OR age = 40)",
                        "test_table",
                        buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "A", 10}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {2, "B", 30}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {3, "C", 25}).size());
        engine.close();
    }

    @Test
    void testWhereWithMixedAndOr() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT id FROM test_table "
                                + "WHERE (age > 20 OR age < 10) AND name LIKE 'A%'",
                        "test_table", buildRowType());
        Assertions.assertEquals(1, exec(engine, new Object[] {1, "Alice", 25}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {2, "Alice", 15}).size());
        Assertions.assertEquals(1, exec(engine, new Object[] {3, "Alice", 5}).size());
        Assertions.assertEquals(0, exec(engine, new Object[] {4, "Bob", 25}).size());
        engine.close();
    }

    @Test
    void testInitCalledTwice() {
        CalciteSQLEngine engine =
                new CalciteSQLEngine("SELECT id FROM test_table", "test_table", buildRowType());
        engine.init();
        engine.init();
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
    }

    @Test
    void testCloseCalledTwice() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id FROM test_table", "test_table", buildRowType());
        Assertions.assertEquals(1, singleField(engine, new Object[] {1, "A", 25}));
        engine.close();
        engine.close();
    }

    @Test
    void testReinitWithDifferentSql() {
        CalciteSQLEngine engine1 =
                new CalciteSQLEngine("SELECT id FROM test_table", "test_table", buildRowType());
        engine1.init();
        Assertions.assertEquals(1, singleField(engine1, new Object[] {1, "A", 25}));
        engine1.close();

        CalciteSQLEngine engine2 =
                new CalciteSQLEngine("SELECT name FROM test_table", "test_table", buildRowType());
        engine2.init();
        Assertions.assertEquals("Alice", singleField(engine2, new Object[] {1, "Alice", 25}));
        engine2.close();
    }

    @Test
    void testExpOne() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT EXP(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {1.0});
        Assertions.assertEquals(Math.E, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testLnOne() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT LN(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {1.0});
        Assertions.assertEquals(0.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testLog10One() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT LOG10(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {1.0});
        Assertions.assertEquals(0.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testPowerOneBase() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"base_val", "exponent"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine =
                createAndInit("SELECT POWER(base_val, exponent) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {1.0, 100.0});
        Assertions.assertEquals(1.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testSqrtPerfectSquare() {
        SeaTunnelRowType rt =
                new SeaTunnelRowType(
                        new String[] {"val"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        CalciteSQLEngine engine = createAndInit("SELECT SQRT(val) AS res FROM t", "t", rt);
        Object result = singleField(engine, new Object[] {10000.0});
        Assertions.assertEquals(100.0, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    private static final String FRAUD_JSON =
            "{"
                    + "\"request_id\":\"req-0042\","
                    + "\"model\":\"fraud-detection-v3\","
                    + "\"timestamp\":\"2026-06-11T15:30:00Z\","
                    + "\"input\":{"
                    + "  \"user_id\":\"U123\","
                    + "  \"device\":{\"type\":\"mobile\",\"os\":\"iOS 19\",\"ip\":\"10.0.0.1\"}"
                    + "},"
                    + "\"predictions\":["
                    + "  {"
                    + "    \"rule_id\":\"R001\",\"rule_name\":\"high_freq_small_amount\","
                    + "    \"score\":0.92,\"label\":\"FRAUD\","
                    + "    \"evidence\":["
                    + "      {\"feature\":\"txn_count_1h\",\"value\":47,\"threshold\":20,\"contrib\":0.35},"
                    + "      {\"feature\":\"avg_amount_1h\",\"value\":12.5,\"threshold\":50,\"contrib\":0.28},"
                    + "      {\"feature\":\"distinct_merchant\",\"value\":15,\"threshold\":5,\"contrib\":0.29}"
                    + "    ]"
                    + "  },"
                    + "  {"
                    + "    \"rule_id\":\"R002\",\"rule_name\":\"geo_anomaly\","
                    + "    \"score\":0.45,\"label\":\"NORMAL\","
                    + "    \"evidence\":["
                    + "      {\"feature\":\"geo_distance_km\",\"value\":8.2,\"threshold\":100,\"contrib\":0.45}"
                    + "    ]"
                    + "  },"
                    + "  {"
                    + "    \"rule_id\":\"R003\",\"rule_name\":\"device_fingerprint_anomaly\","
                    + "    \"score\":0.78,\"label\":\"SUSPECT\","
                    + "    \"evidence\":["
                    + "      {\"feature\":\"device_age_days\",\"value\":1,\"threshold\":7,\"contrib\":0.40},"
                    + "      {\"feature\":\"fingerprint_change\",\"value\":1,\"threshold\":0,\"contrib\":0.38}"
                    + "    ]"
                    + "  }"
                    + "]"
                    + "}";

    private SeaTunnelRowType jsonRowType() {
        return new SeaTunnelRowType(
                new String[] {"raw"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    @Test
    void testJsonValueTopLevelScalar() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.request_id') AS req_id FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("req-0042", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueModel() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.model') AS model_name FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals(
                "fraud-detection-v3", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueTimestamp() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.timestamp') AS ts FROM t", "t", jsonRowType());
        Assertions.assertEquals(
                "2026-06-11T15:30:00Z", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueNestedUserId() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.input.user_id') AS uid FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("U123", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueDeepNestedDeviceType() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.input.device.type') AS dev_type FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("mobile", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueDeepNestedDeviceOs() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.input.device.os') AS dev_os FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("iOS 19", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueDeepNestedDeviceIp() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.input.device.ip') AS dev_ip FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("10.0.0.1", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueArrayIndexRuleId() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].rule_id') AS rid FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("R001", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueArrayIndexRuleName() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].rule_name') AS rn FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals(
                "high_freq_small_amount", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueArrayIndexScore() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].score') AS score FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("0.92", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueArrayIndexLabel() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].label') AS lbl FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("FRAUD", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueSecondPrediction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[1].rule_id') AS rid FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("R002", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueThirdPrediction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[2].rule_id') AS rid FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("R003", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueThirdPredictionLabel() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[2].label') AS lbl FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("SUSPECT", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueDeepNestedEvidence() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[0].feature') AS feat FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("txn_count_1h", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueEvidenceValue() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[0].value') AS val FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("47", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueEvidenceThreshold() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[0].threshold') AS th FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("20", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueEvidenceContrib() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[0].contrib') AS c FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("0.35", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueCrossArrayEvidence() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[2].evidence[1].feature') AS feat FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals(
                "fingerprint_change", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueNonExistentPath() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.nonexistent') AS missing FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueNonExistentNested() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.input.address.city') AS city FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueOutOfBoundsIndex() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[99].rule_id') AS rid FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonValueFromNull() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.request_id') AS req_id FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {(Object) null}));
        engine.close();
    }

    @Test
    void testJsonQueryPredictionsArray() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_QUERY(raw, '$.predictions') AS preds FROM t",
                        "t",
                        jsonRowType());
        String result = (String) singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.startsWith("["));
        Assertions.assertTrue(result.contains("R001"));
        Assertions.assertTrue(result.contains("R002"));
        Assertions.assertTrue(result.contains("R003"));
        engine.close();
    }

    @Test
    void testJsonQueryInputObject() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_QUERY(raw, '$.input') AS inp FROM t", "t", jsonRowType());
        String result = (String) singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("U123"));
        Assertions.assertTrue(result.contains("mobile"));
        engine.close();
    }

    @Test
    void testJsonQueryDeviceObject() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_QUERY(raw, '$.input.device') AS dev FROM t",
                        "t",
                        jsonRowType());
        String result = (String) singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("mobile"));
        Assertions.assertTrue(result.contains("iOS 19"));
        Assertions.assertTrue(result.contains("10.0.0.1"));
        engine.close();
    }

    @Test
    void testJsonQuerySinglePrediction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_QUERY(raw, '$.predictions[0]') AS pred FROM t",
                        "t",
                        jsonRowType());
        String result = (String) singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("R001"));
        Assertions.assertTrue(result.contains("FRAUD"));
        Assertions.assertTrue(result.contains("0.92"));
        engine.close();
    }

    @Test
    void testJsonQueryEvidenceArray() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_QUERY(raw, '$.predictions[0].evidence') AS ev FROM t",
                        "t",
                        jsonRowType());
        String result = (String) singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.startsWith("["));
        Assertions.assertTrue(result.contains("txn_count_1h"));
        Assertions.assertTrue(result.contains("avg_amount_1h"));
        Assertions.assertTrue(result.contains("distinct_merchant"));
        engine.close();
    }

    @Test
    void testJsonMultiFieldExtraction() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.input.user_id') AS uid, "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_id') AS rid, "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_name') AS rn, "
                                + "JSON_VALUE(raw, '$.predictions[0].score') AS score, "
                                + "JSON_VALUE(raw, '$.predictions[0].label') AS lbl "
                                + "FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals("req-0042", row.getField(0));
        Assertions.assertEquals("U123", row.getField(1));
        Assertions.assertEquals("R001", row.getField(2));
        Assertions.assertEquals("high_freq_small_amount", row.getField(3));
        Assertions.assertEquals("0.92", row.getField(4));
        Assertions.assertEquals("FRAUD", row.getField(5));
        engine.close();
    }

    @Test
    void testJsonExtractTopRiskRule() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.input.user_id') AS uid, "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_id') AS rid, "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_name') AS rn, "
                                + "CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) AS score, "
                                + "JSON_VALUE(raw, '$.predictions[0].label') AS lbl "
                                + "FROM t "
                                + "WHERE JSON_VALUE(raw, '$.predictions[0].label') IN ('FRAUD', 'SUSPECT')",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals("req-0042", row.getField(0));
        Assertions.assertEquals("U123", row.getField(1));
        Assertions.assertEquals("R001", row.getField(2));
        Assertions.assertEquals("high_freq_small_amount", row.getField(3));
        Assertions.assertEquals(0.92, ((Number) row.getField(4)).doubleValue(), 0.001);
        Assertions.assertEquals("FRAUD", row.getField(5));
        engine.close();
    }

    @Test
    void testJsonFilterByLabel() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[1].label') AS lbl FROM t "
                                + "WHERE JSON_VALUE(raw, '$.predictions[1].label') = 'NORMAL'",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("NORMAL", result.get(0).getField(0));
        engine.close();
    }

    @Test
    void testJsonFilterByLabelNotMatch() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[1].label') AS lbl FROM t "
                                + "WHERE JSON_VALUE(raw, '$.predictions[1].label') = 'FRAUD'",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertTrue(result.isEmpty());
        engine.close();
    }

    @Test
    void testJsonCastScoreToDouble() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) AS score FROM t",
                        "t",
                        jsonRowType());

        Object result = singleField(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals(0.92, ((Number) result).doubleValue(), 0.001);
        engine.close();
    }

    @Test
    void testJsonCastScoreFilterHighRisk() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].rule_id') AS rid FROM t "
                                + "WHERE CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) > 0.8",
                        "t",
                        jsonRowType());

        Assertions.assertEquals("R001", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonCastScoreFilterLowRisk() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[1].rule_id') AS rid FROM t "
                                + "WHERE CAST(JSON_VALUE(raw, '$.predictions[1].score') AS DOUBLE) > 0.8",
                        "t",
                        jsonRowType());

        Assertions.assertTrue(exec(engine, new Object[] {FRAUD_JSON}).isEmpty());
        engine.close();
    }

    @Test
    void testJsonCastEvidenceValueToInt() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT CAST(JSON_VALUE(raw, '$.predictions[0].evidence[0].value') AS INTEGER) AS val FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertEquals(47, singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonEvidenceExceedThresholdCheck() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[0].feature') AS feat FROM t "
                                + "WHERE CAST(JSON_VALUE(raw, '$.predictions[0].evidence[0].value') AS INTEGER) "
                                + "    > CAST(JSON_VALUE(raw, '$.predictions[0].evidence[0].threshold') AS INTEGER)",
                        "t",
                        jsonRowType());
        Assertions.assertEquals("txn_count_1h", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonEvidenceBelowThresholdFiltered() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.predictions[0].evidence[1].feature') AS feat FROM t "
                                + "WHERE CAST(JSON_VALUE(raw, '$.predictions[0].evidence[1].value') AS DOUBLE) "
                                + "    > CAST(JSON_VALUE(raw, '$.predictions[0].evidence[1].threshold') AS DOUBLE)",
                        "t",
                        jsonRowType());
        Assertions.assertTrue(exec(engine, new Object[] {FRAUD_JSON}).isEmpty());
        engine.close();
    }

    @Test
    void testJsonComplexFraudRiskReport() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.input.user_id') AS uid, "
                                + "JSON_VALUE(raw, '$.input.device.type') AS dev_type, "
                                + "JSON_VALUE(raw, '$.input.device.ip') AS dev_ip, "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_name') AS top_rule, "
                                + "CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) AS top_score, "
                                + "JSON_VALUE(raw, '$.predictions[0].label') AS top_label, "
                                + "TRIM(CASE WHEN JSON_VALUE(raw, '$.predictions[0].label') = 'FRAUD' "
                                + "     THEN 'BLOCK' ELSE 'PASS' END) AS action "
                                + "FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals(1, result.size());
        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals("req-0042", row.getField(0));
        Assertions.assertEquals("U123", row.getField(1));
        Assertions.assertEquals("mobile", row.getField(2));
        Assertions.assertEquals("10.0.0.1", row.getField(3));
        Assertions.assertEquals("high_freq_small_amount", row.getField(4));
        Assertions.assertEquals(0.92, ((Number) row.getField(5)).doubleValue(), 0.001);
        Assertions.assertEquals("FRAUD", row.getField(6));
        Assertions.assertEquals("BLOCK", row.getField(7));
        engine.close();
    }

    @Test
    void testJsonWithNormalLabelAction() {
        String normalJson =
                "{\"request_id\":\"req-0099\",\"model\":\"fraud-v3\","
                        + "\"input\":{\"user_id\":\"U456\",\"device\":{\"type\":\"desktop\",\"os\":\"Windows\",\"ip\":\"192.168.1.1\"}},"
                        + "\"predictions\":["
                        + "{\"rule_id\":\"R001\",\"rule_name\":\"high_freq\",\"score\":0.15,\"label\":\"NORMAL\",\"evidence\":[]}"
                        + "]}";

        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.input.user_id') AS uid, "
                                + "TRIM(CASE WHEN JSON_VALUE(raw, '$.predictions[0].label') = 'FRAUD' "
                                + "     THEN 'BLOCK' ELSE 'PASS' END) AS action "
                                + "FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {normalJson});
        Assertions.assertEquals("req-0099", result.get(0).getField(0));
        Assertions.assertEquals("U456", result.get(0).getField(1));
        Assertions.assertEquals("PASS", result.get(0).getField(2));
        engine.close();
    }

    @Test
    void testJsonExtractAllThreePredictions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.predictions[0].rule_id') AS r0, "
                                + "JSON_VALUE(raw, '$.predictions[0].label') AS l0, "
                                + "JSON_VALUE(raw, '$.predictions[1].rule_id') AS r1, "
                                + "JSON_VALUE(raw, '$.predictions[1].label') AS l1, "
                                + "JSON_VALUE(raw, '$.predictions[2].rule_id') AS r2, "
                                + "JSON_VALUE(raw, '$.predictions[2].label') AS l2 "
                                + "FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {FRAUD_JSON});
        SeaTunnelRow row = result.get(0);
        Assertions.assertEquals("R001", row.getField(0));
        Assertions.assertEquals("FRAUD", row.getField(1));
        Assertions.assertEquals("R002", row.getField(2));
        Assertions.assertEquals("NORMAL", row.getField(3));
        Assertions.assertEquals("R003", row.getField(4));
        Assertions.assertEquals("SUSPECT", row.getField(5));
        engine.close();
    }

    @Test
    void testJsonMaxScoreAmongPredictions() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "CASE "
                                + "  WHEN CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) >= "
                                + "       CAST(JSON_VALUE(raw, '$.predictions[1].score') AS DOUBLE) "
                                + "   AND CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) >= "
                                + "       CAST(JSON_VALUE(raw, '$.predictions[2].score') AS DOUBLE) "
                                + "  THEN JSON_VALUE(raw, '$.predictions[0].rule_id') "
                                + "  WHEN CAST(JSON_VALUE(raw, '$.predictions[1].score') AS DOUBLE) >= "
                                + "       CAST(JSON_VALUE(raw, '$.predictions[2].score') AS DOUBLE) "
                                + "  THEN JSON_VALUE(raw, '$.predictions[1].rule_id') "
                                + "  ELSE JSON_VALUE(raw, '$.predictions[2].rule_id') "
                                + "END AS max_rule "
                                + "FROM t",
                        "t",
                        jsonRowType());

        Assertions.assertEquals("R001", singleField(engine, new Object[] {FRAUD_JSON}));
        engine.close();
    }

    @Test
    void testJsonWithSimpleObject() {
        String simpleJson = "{\"name\":\"seatunnel\",\"version\":\"3.0.0\",\"type\":\"etl\"}";
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.name') AS n, "
                                + "JSON_VALUE(raw, '$.version') AS v FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> result = exec(engine, new Object[] {simpleJson});
        Assertions.assertEquals("seatunnel", result.get(0).getField(0));
        Assertions.assertEquals("3.0.0", result.get(0).getField(1));
        engine.close();
    }

    @Test
    void testJsonEmptyObject() {
        CalciteSQLEngine engine =
                createAndInit("SELECT JSON_VALUE(raw, '$.name') AS n FROM t", "t", jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {"{}"}));
        engine.close();
    }

    @Test
    void testJsonEmptyArray() {
        String json = "{\"items\":[]}";
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.items[0]') AS first_item FROM t",
                        "t",
                        jsonRowType());
        Assertions.assertNull(singleField(engine, new Object[] {json}));
        engine.close();
    }

    @Test
    void testJsonMultipleRowsReuse() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.predictions[0].label') AS lbl FROM t",
                        "t",
                        jsonRowType());

        List<SeaTunnelRow> r1 = exec(engine, new Object[] {FRAUD_JSON});
        Assertions.assertEquals("req-0042", r1.get(0).getField(0));
        Assertions.assertEquals("FRAUD", r1.get(0).getField(1));

        String anotherJson =
                "{\"request_id\":\"req-0100\",\"predictions\":[{\"label\":\"NORMAL\"}]}";
        List<SeaTunnelRow> r2 = exec(engine, new Object[] {anotherJson});
        Assertions.assertEquals("req-0100", r2.get(0).getField(0));
        Assertions.assertEquals("NORMAL", r2.get(0).getField(1));

        engine.close();
    }

    @Test
    void testJsonOutputSchema() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT "
                                + "JSON_VALUE(raw, '$.request_id') AS req_id, "
                                + "JSON_VALUE(raw, '$.input.user_id') AS uid, "
                                + "CAST(JSON_VALUE(raw, '$.predictions[0].score') AS DOUBLE) AS score "
                                + "FROM t",
                        "t",
                        jsonRowType());

        SeaTunnelRowType outType = engine.getOutputRowType();
        Assertions.assertEquals(3, outType.getTotalFields());
        Assertions.assertEquals("req_id", outType.getFieldName(0));
        Assertions.assertEquals("uid", outType.getFieldName(1));
        Assertions.assertEquals("score", outType.getFieldName(2));
        Assertions.assertEquals(SqlType.STRING, outType.getFieldType(0).getSqlType());
        Assertions.assertEquals(SqlType.STRING, outType.getFieldType(1).getSqlType());
        Assertions.assertEquals(SqlType.DOUBLE, outType.getFieldType(2).getSqlType());
        engine.close();
    }

    private SeaTunnelRowType twoVectorRowType() {
        return new SeaTunnelRowType(
                new String[] {"vec1", "vec2"},
                new SeaTunnelDataType[] {
                    VectorType.VECTOR_FLOAT_TYPE, VectorType.VECTOR_FLOAT_TYPE
                });
    }

    private SeaTunnelRowType singleVectorRowType() {
        return new SeaTunnelRowType(
                new String[] {"vec"}, new SeaTunnelDataType[] {VectorType.VECTOR_FLOAT_TYPE});
    }

    private static ByteBuffer floatVec(Float... values) {
        return VectorUtils.toByteBuffer(values);
    }

    @Test
    void testCosineDistanceIdentical() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT COSINE_DISTANCE(vec1, vec2) AS dist FROM t",
                        "t",
                        twoVectorRowType());
        ByteBuffer v = floatVec(1.0f, 2.0f, 3.0f);
        Object result = singleField(engine, new Object[] {v, v.duplicate()});
        Assertions.assertEquals(0.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testCosineDistanceOrthogonal() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT COSINE_DISTANCE(vec1, vec2) AS dist FROM t",
                        "t",
                        twoVectorRowType());
        Object result =
                singleField(engine, new Object[] {floatVec(1.0f, 0.0f), floatVec(0.0f, 1.0f)});
        Assertions.assertEquals(1.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testL1Distance() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT L1_DISTANCE(vec1, vec2) AS dist FROM t", "t", twoVectorRowType());
        Object result =
                singleField(
                        engine,
                        new Object[] {floatVec(2.0f, 4.0f, 6.0f), floatVec(1.0f, 2.0f, 3.0f)});
        Assertions.assertEquals(6.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testL2Distance() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT L2_DISTANCE(vec1, vec2) AS dist FROM t", "t", twoVectorRowType());
        Object result =
                singleField(
                        engine,
                        new Object[] {floatVec(2.0f, 4.0f, 4.0f), floatVec(1.0f, 2.0f, 2.0f)});
        Assertions.assertEquals(3.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testVectorDims() {
        CalciteSQLEngine engine =
                createAndInit("SELECT VECTOR_DIMS(vec) AS dims FROM t", "t", singleVectorRowType());
        Object result = singleField(engine, new Object[] {floatVec(1.0f, 2.0f, 3.0f)});
        Assertions.assertEquals(3, ((Number) result).intValue());
        engine.close();
    }

    @Test
    void testVectorNorm() {
        CalciteSQLEngine engine =
                createAndInit("SELECT VECTOR_NORM(vec) AS norm FROM t", "t", singleVectorRowType());
        Object result = singleField(engine, new Object[] {floatVec(1.0f, 2.0f, 2.0f)});
        Assertions.assertEquals(3.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testInnerProduct() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT INNER_PRODUCT(vec1, vec2) AS ip FROM t", "t", twoVectorRowType());
        Object result =
                singleField(
                        engine,
                        new Object[] {floatVec(1.0f, 2.0f, 3.0f), floatVec(7.0f, 8.0f, 9.0f)});
        Assertions.assertEquals(50.0, ((Number) result).doubleValue(), 1e-9);
        engine.close();
    }

    @Test
    void testVectorReduceTruncate() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT VECTOR_REDUCE(vec, 2, 'TRUNCATE') AS reduced FROM t",
                        "t",
                        singleVectorRowType());
        Object result = singleField(engine, new Object[] {floatVec(1.0f, 2.0f, 3.0f, 4.0f)});
        Assertions.assertNotNull(result);
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT_TYPE, engine.getOutputRowType().getFieldType(0));
        Float[] reduced = VectorUtils.toFloatArray((ByteBuffer) result);
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f}, reduced);
        engine.close();
    }

    @Test
    void testVectorNormalize() {
        CalciteSQLEngine engine =
                createAndInit(
                        "SELECT VECTOR_NORMALIZE(vec) AS nvec FROM t", "t", singleVectorRowType());
        Object result = singleField(engine, new Object[] {floatVec(3.0f, 4.0f)});
        Assertions.assertNotNull(result);
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT_TYPE, engine.getOutputRowType().getFieldType(0));
        Float[] normalized = VectorUtils.toFloatArray((ByteBuffer) result);
        Assertions.assertEquals(2, normalized.length);
        double norm = Math.sqrt(normalized[0] * normalized[0] + normalized[1] * normalized[1]);
        Assertions.assertEquals(1.0, norm, 1e-6);
        engine.close();
    }

    @Test
    void testVectorAliasPreservesType() {
        CalciteSQLEngine engine =
                createAndInit("SELECT vec AS alias_vec FROM t", "t", singleVectorRowType());
        Object result = singleField(engine, new Object[] {floatVec(1.0f, 2.0f, 3.0f)});
        Assertions.assertInstanceOf(ByteBuffer.class, result);
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT_TYPE, engine.getOutputRowType().getFieldType(0));
        Float[] values = VectorUtils.toFloatArray((ByteBuffer) result);
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f, 3.0f}, values);
        engine.close();
    }

    @Test
    void testSelectStarPreservesVectorType() {
        CalciteSQLEngine engine = createAndInit("SELECT * FROM t", "t", singleVectorRowType());
        List<SeaTunnelRow> results = exec(engine, new Object[] {floatVec(1.0f, 2.0f, 3.0f)});
        Assertions.assertEquals(1, results.size());
        Assertions.assertEquals(
                VectorType.VECTOR_FLOAT_TYPE, engine.getOutputRowType().getFieldType(0));
        Assertions.assertInstanceOf(ByteBuffer.class, results.get(0).getField(0));
        Float[] values = VectorUtils.toFloatArray((ByteBuffer) results.get(0).getField(0));
        Assertions.assertArrayEquals(new Float[] {1.0f, 2.0f, 3.0f}, values);
        engine.close();
    }

    @Test
    void testRowKindPropagation() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id, name FROM test_table", "test_table", buildRowType());

        for (RowKind kind : RowKind.values()) {
            SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "Alice", 25});
            input.setRowKind(kind);
            input.setTableId("db.schema.users");

            List<SeaTunnelRow> results = engine.execute(input);
            Assertions.assertEquals(1, results.size());
            SeaTunnelRow output = results.get(0);
            Assertions.assertEquals(
                    kind, output.getRowKind(), "RowKind should be preserved for " + kind);
            Assertions.assertEquals("db.schema.users", output.getTableId());
        }
        engine.close();
    }

    @Test
    void testRowOptionsPropagation() {
        CalciteSQLEngine engine =
                createAndInit("SELECT id, name FROM test_table", "test_table", buildRowType());

        SeaTunnelRow input = new SeaTunnelRow(new Object[] {1, "Alice", 25});
        input.setRowKind(RowKind.UPDATE_AFTER);
        input.setTableId("mydb.mytable");
        Map<String, Object> options = new HashMap<>();
        options.put("partition", "p0");
        options.put("offset", "42");
        input.setOptions(options);

        List<SeaTunnelRow> results = engine.execute(input);
        Assertions.assertEquals(1, results.size());
        SeaTunnelRow output = results.get(0);
        Assertions.assertEquals(RowKind.UPDATE_AFTER, output.getRowKind());
        Assertions.assertEquals("mydb.mytable", output.getTableId());
        Assertions.assertNotNull(output.getOptions());
        Assertions.assertEquals("p0", output.getOptions().get("partition"));
        Assertions.assertEquals("42", output.getOptions().get("offset"));
        engine.close();
    }
}
