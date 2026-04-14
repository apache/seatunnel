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
}
