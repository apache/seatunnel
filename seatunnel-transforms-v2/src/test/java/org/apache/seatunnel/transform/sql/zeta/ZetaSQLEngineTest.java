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
}
