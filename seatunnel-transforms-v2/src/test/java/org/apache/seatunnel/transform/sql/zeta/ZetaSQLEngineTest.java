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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ZetaSQLEngineTest {

    private SeaTunnelRowType simpleRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name", "age"},
                new SeaTunnelDataType[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                });
    }

    @Test
    public void testTypeMappingAndTransformBySQL() {
        SeaTunnelRowType rowType = simpleRowType();
        ZetaSQLEngine engine = new ZetaSQLEngine();
        engine.init("test", "test", rowType, "select id, name, age + 1 as age_next from test");

        List<String> inputColumnsMapping = new ArrayList<>();
        SeaTunnelRowType outType = engine.typeMapping(inputColumnsMapping);

        Assertions.assertArrayEquals(
                new String[] {"id", "name", "age_next"}, outType.getFieldNames());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "Alice", 20});
        List<SeaTunnelRow> outRows = engine.transformBySQL(inputRow, outType);
        Assertions.assertNotNull(outRows);
        Assertions.assertEquals(1, outRows.size());

        SeaTunnelRow outRow = outRows.get(0);
        Assertions.assertEquals(1, outRow.getField(0));
        Assertions.assertEquals("Alice", outRow.getField(1));
        Assertions.assertEquals(21, outRow.getField(2));
    }

    @Test
    public void testWhereFilterDropsRow() {
        SeaTunnelRowType rowType = simpleRowType();
        ZetaSQLEngine engine = new ZetaSQLEngine();
        engine.init("test", "test", rowType, "select id from test where age > 18");

        SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());

        SeaTunnelRow young = new SeaTunnelRow(new Object[] {1, "Bob", 17});
        List<SeaTunnelRow> outYoung = engine.transformBySQL(young, outType);
        Assertions.assertNull(outYoung);

        SeaTunnelRow adult = new SeaTunnelRow(new Object[] {2, "Carol", 20});
        List<SeaTunnelRow> outAdult = engine.transformBySQL(adult, outType);
        Assertions.assertNotNull(outAdult);
        Assertions.assertEquals(1, outAdult.size());
        Assertions.assertEquals(2, outAdult.get(0).getField(0));
    }

    @Test
    public void testInvalidSqlThrowsTransformException() {
        SeaTunnelRowType rowType = simpleRowType();
        ZetaSQLEngine engine = new ZetaSQLEngine();

        Assertions.assertThrows(
                TransformException.class,
                () ->
                        engine.init(
                                "test",
                                "test",
                                rowType,
                                "insert into test(id, name, age) values (1, 'bad', 10)"));
    }

    @Test
    public void testSchemaInferenceShouldNotOpenUdf() {
        TrackingUdf trackingUdf = new TrackingUdf("tracking", false);
        ZetaSQLEngine engine = new TestableZetaSQLEngine(Collections.singletonList(trackingUdf));
        engine.init("test", "test", simpleRowType(), "select id, name from test");

        SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());

        Assertions.assertNotNull(outType);
        Assertions.assertEquals(0, trackingUdf.getOpenCount());
        Assertions.assertEquals(0, trackingUdf.getCloseCount());
    }

    @Test
    public void testOpenUdfWhenExecuteAndCloseOnEngineClose() {
        TrackingUdf trackingUdf = new TrackingUdf("tracking", false);
        ZetaSQLEngine engine = new TestableZetaSQLEngine(Collections.singletonList(trackingUdf));
        engine.init("test", "test", simpleRowType(), "select id from test");
        SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "Alice", 20});
        engine.transformBySQL(inputRow, outType);
        engine.transformBySQL(inputRow, outType);

        Assertions.assertEquals(1, trackingUdf.getOpenCount());
        Assertions.assertEquals(0, trackingUdf.getCloseCount());

        engine.close();

        Assertions.assertEquals(1, trackingUdf.getCloseCount());
    }

    @Test
    public void testOpenFailureShouldCloseFailedAndOpenedUdfs() {
        TrackingUdf firstUdf = new TrackingUdf("first", false);
        TrackingUdf failedUdf = new TrackingUdf("failed", true);
        ZetaSQLEngine engine = new TestableZetaSQLEngine(Arrays.asList(firstUdf, failedUdf));
        engine.init("test", "test", simpleRowType(), "select id from test");
        SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());

        Assertions.assertThrows(
                TransformException.class,
                () ->
                        engine.transformBySQL(
                                new SeaTunnelRow(new Object[] {1, "Alice", 20}), outType));

        Assertions.assertEquals(1, firstUdf.getOpenCount());
        Assertions.assertEquals(1, firstUdf.getCloseCount());
        Assertions.assertEquals(1, failedUdf.getOpenCount());
        Assertions.assertEquals(1, failedUdf.getCloseCount());
    }

    private static final class TestableZetaSQLEngine extends ZetaSQLEngine {

        private final List<ZetaUDF> testUdfs;

        private TestableZetaSQLEngine(List<ZetaUDF> testUdfs) {
            this.testUdfs = testUdfs;
        }

        @Override
        protected List<ZetaUDF> loadUDFs() {
            return new ArrayList<>(testUdfs);
        }
    }

    private static final class TrackingUdf implements ZetaUDF {
        private final String functionName;
        private final boolean failOnOpen;
        private int openCount;
        private int closeCount;

        private TrackingUdf(String functionName, boolean failOnOpen) {
            this.functionName = functionName;
            this.failOnOpen = failOnOpen;
        }

        @Override
        public String functionName() {
            return functionName;
        }

        @Override
        public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
            return BasicType.STRING_TYPE;
        }

        @Override
        public Object evaluate(List<Object> args) {
            return null;
        }

        @Override
        public void open() throws Exception {
            openCount++;
            if (failOnOpen) {
                throw new Exception("open failed");
            }
        }

        @Override
        public void close() {
            closeCount++;
        }

        private int getOpenCount() {
            return openCount;
        }

        private int getCloseCount() {
            return closeCount;
        }
    }
    // ---- integral CAST range checking, issue #12571 ----

    private SeaTunnelRowType integralRowType() {
        return new SeaTunnelRowType(
                new String[] {"c_tinyint", "c_smallint", "c_int", "c_bigint", "c_str"},
                new SeaTunnelDataType[] {
                    BasicType.BYTE_TYPE,
                    BasicType.SHORT_TYPE,
                    BasicType.INT_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE
                });
    }

    /** Runs one expression and returns its single output value. */
    private Object castResult(String sql, Object[] row) {
        ZetaSQLEngine engine = new ZetaSQLEngine();
        engine.init("test", "test", integralRowType(), sql);
        SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());
        return engine.transformBySQL(new SeaTunnelRow(row), outType).get(0).getField(0);
    }

    private Object[] rowWith(long bigintValue, String stringValue) {
        return new Object[] {(byte) 0, (short) 0, 0, bigintValue, stringValue};
    }

    @Test
    public void testCastBigintToIntAcceptsTheExactBoundaries() {
        Assertions.assertEquals(
                Integer.MIN_VALUE,
                castResult(
                        "select cast(c_bigint as INT) as r from test",
                        rowWith(Integer.MIN_VALUE, "x")));
        Assertions.assertEquals(
                Integer.MAX_VALUE,
                castResult(
                        "select cast(c_bigint as INT) as r from test",
                        rowWith(Integer.MAX_VALUE, "x")));
        Assertions.assertEquals(
                0, castResult("select cast(c_bigint as INT) as r from test", rowWith(0L, "x")));
    }

    @Test
    public void testCastBigintToIntRejectsJustOutsideTheBoundaries() {
        // Before this change these wrapped silently: MIN-1 produced +2147483647 and MAX+1
        // produced -2147483648, so a negative input could surface as the largest positive int.
        for (long out : new long[] {Integer.MIN_VALUE - 1L, Integer.MAX_VALUE + 1L, 3000000000L}) {
            Assertions.assertThrows(
                    TransformException.class,
                    () ->
                            castResult(
                                    "select cast(c_bigint as INT) as r from test",
                                    rowWith(out, "x")),
                    "expected CAST to reject " + out);
        }
    }

    @Test
    public void testTryCastReturnsNullWhereCastNowFails() {
        // The point of failing rather than wrapping: TRY_CAST can finally report it as null.
        for (long out : new long[] {Integer.MIN_VALUE - 1L, Integer.MAX_VALUE + 1L}) {
            Assertions.assertNull(
                    castResult(
                            "select try_cast(c_bigint as INT) as r from test", rowWith(out, "x")),
                    "expected TRY_CAST to yield null for " + out);
        }
        Assertions.assertEquals(
                Integer.MAX_VALUE,
                castResult(
                        "select try_cast(c_bigint as INT) as r from test",
                        rowWith(Integer.MAX_VALUE, "x")));
    }

    @Test
    public void testStringSourceBoundariesAreUnchangedForEveryIntegralTarget() {
        Object[][] accepted = {
            {"TINYINT", String.valueOf(Byte.MIN_VALUE), (byte) Byte.MIN_VALUE},
            {"TINYINT", String.valueOf(Byte.MAX_VALUE), (byte) Byte.MAX_VALUE},
            {"SMALLINT", String.valueOf(Short.MIN_VALUE), (short) Short.MIN_VALUE},
            {"SMALLINT", String.valueOf(Short.MAX_VALUE), (short) Short.MAX_VALUE},
            {"INT", String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE},
            {"INT", String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE}
        };
        for (Object[] c : accepted) {
            Assertions.assertEquals(
                    c[2],
                    castResult(
                            String.format("select cast(c_str as %s) as r from test", c[0]),
                            rowWith(0L, (String) c[1])),
                    c[0] + " should accept " + c[1]);
        }

        String[][] rejected = {
            {"TINYINT", String.valueOf(Byte.MIN_VALUE - 1)},
            {"TINYINT", String.valueOf(Byte.MAX_VALUE + 1)},
            {"SMALLINT", String.valueOf(Short.MIN_VALUE - 1)},
            {"SMALLINT", String.valueOf(Short.MAX_VALUE + 1)},
            {"INT", String.valueOf(Integer.MIN_VALUE - 1L)},
            {"INT", String.valueOf(Integer.MAX_VALUE + 1L)}
        };
        for (String[] c : rejected) {
            Assertions.assertThrows(
                    Exception.class,
                    () ->
                            castResult(
                                    String.format("select cast(c_str as %s) as r from test", c[0]),
                                    rowWith(0L, c[1])),
                    c[0] + " should reject " + c[1]);
            Assertions.assertNull(
                    castResult(
                            String.format("select try_cast(c_str as %s) as r from test", c[0]),
                            rowWith(0L, c[1])),
                    c[0] + " TRY_CAST should be null for " + c[1]);
        }
    }

    @Test
    public void testWideningAndIdentityCastsAreUnchanged() {
        SeaTunnelRowType rowType = integralRowType();
        Object[] row = {(byte) 42, (short) 4242, 424242, 42424242424L, "42"};
        String[][] pairs = {
            {"c_tinyint", "SMALLINT"},
            {"c_tinyint", "INT"},
            {"c_tinyint", "BIGINT"},
            {"c_smallint", "INT"},
            {"c_smallint", "BIGINT"},
            {"c_int", "BIGINT"},
            {"c_tinyint", "TINYINT"},
            {"c_smallint", "SMALLINT"},
            {"c_int", "INT"},
            {"c_bigint", "BIGINT"}
        };
        Object[] expected = {
            (short) 42, 42, 42L, 4242, 4242L, 424242L, (byte) 42, (short) 4242, 424242, 42424242424L
        };
        for (int i = 0; i < pairs.length; i++) {
            ZetaSQLEngine engine = new ZetaSQLEngine();
            engine.init(
                    "test",
                    "test",
                    rowType,
                    String.format(
                            "select cast(%s as %s) as r from test", pairs[i][0], pairs[i][1]));
            SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());
            Assertions.assertEquals(
                    expected[i],
                    engine.transformBySQL(new SeaTunnelRow(row), outType).get(0).getField(0),
                    pairs[i][0] + " -> " + pairs[i][1]);
        }
    }

    @Test
    public void testCoalesceAndIfnullAlsoRejectAnOutOfRangeNarrowing() {
        // COALESCE and IFNULL reach the same castAs boundary, and ZetaSQLType infers their type
        // from the first non-null argument rather than the widest one. So COALESCE(int, bigint)
        // targets INT, and before this change an overflowing bigint arrived silently truncated,
        // exactly as it did through CAST. Pinned here so the shared behaviour is deliberate.
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_int", "c_bigint"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE});

        for (String sql :
                new String[] {
                    "select coalesce(c_int, c_bigint) as r from test",
                    "select ifnull(c_int, c_bigint) as r from test"
                }) {
            ZetaSQLEngine overflowEngine = new ZetaSQLEngine();
            overflowEngine.init("test", "test", rowType, sql);
            SeaTunnelRowType overflowType = overflowEngine.typeMapping(new ArrayList<>());
            Assertions.assertThrows(
                    TransformException.class,
                    () ->
                            overflowEngine.transformBySQL(
                                    new SeaTunnelRow(new Object[] {null, 3000000000L}),
                                    overflowType),
                    sql + " should reject an out-of-range bigint");

            ZetaSQLEngine inRangeEngine = new ZetaSQLEngine();
            inRangeEngine.init("test", "test", rowType, sql);
            SeaTunnelRowType inRangeType = inRangeEngine.typeMapping(new ArrayList<>());
            Assertions.assertEquals(
                    5,
                    inRangeEngine
                            .transformBySQL(new SeaTunnelRow(new Object[] {null, 5L}), inRangeType)
                            .get(0)
                            .getField(0),
                    sql + " should still pass an in-range value through");
        }
    }

    @Test
    public void testCaseExpressionWidensAndIsUnaffected() {
        // CASE also reaches castAs, but ZetaSQLType infers the widest branch type for it, so no
        // narrowing happens and the range check cannot fire. Pinned so a later change to that
        // inference does not quietly start failing CASE expressions.
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_int", "c_bigint"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.LONG_TYPE});
        for (String sql :
                new String[] {
                    "select case when c_int is null then c_bigint else c_int end as r from test",
                    "select case when c_int is not null then c_int else c_bigint end as r from test"
                }) {
            ZetaSQLEngine engine = new ZetaSQLEngine();
            engine.init("test", "test", rowType, sql);
            SeaTunnelRowType outType = engine.typeMapping(new ArrayList<>());
            Assertions.assertEquals(
                    3000000000L,
                    engine.transformBySQL(
                                    new SeaTunnelRow(new Object[] {null, 3000000000L}), outType)
                            .get(0)
                            .getField(0),
                    sql + " should widen to BIGINT and keep the value");
        }
    }
}
