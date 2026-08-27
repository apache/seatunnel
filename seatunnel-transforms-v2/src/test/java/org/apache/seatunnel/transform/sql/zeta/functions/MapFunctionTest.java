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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class MapFunctionTest {
    private SQLEngine zeta() {
        return SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
    }

    private SeaTunnelRowType dummyInputType() {
        return new SeaTunnelRowType(
                new String[] {"dummy"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
    }

    private SeaTunnelRow dummyRow() {
        return new SeaTunnelRow(new Object[] {1});
    }

    @Test
    void testNestedMapLiteralEvaluation() {
        SQLEngine sql = zeta();
        SeaTunnelRowType inType = dummyInputType();

        String sqlText =
                "select "
                        + "  MAP('k1', MAP('a', 1, 'b', 2), 'k2', MAP('c', 3, 'd', 4)) as m1 "
                        + "from test";

        sql.init("test", null, inType, sqlText);

        List<SeaTunnelRow> out = sql.transformBySQL(dummyRow(), inType);
        Assertions.assertEquals(1, out.size());

        Map m1 = (Map) out.get(0).getField(0);
        Assertions.assertNotNull(m1);

        Map k1 = (Map) m1.get("k1");
        Map k2 = (Map) m1.get("k2");
        Assertions.assertNotNull(k1);
        Assertions.assertNotNull(k2);

        Assertions.assertEquals(1, ((Number) k1.get("a")).intValue());
        Assertions.assertEquals(2, ((Number) k1.get("b")).intValue());
        Assertions.assertEquals(3, ((Number) k2.get("c")).intValue());
        Assertions.assertEquals(4, ((Number) k2.get("d")).intValue());
    }

    @Test
    void testMapWithArrayValues() {
        SQLEngine sql = zeta();
        SeaTunnelRowType inType = dummyInputType();

        String sqlText =
                "select " + "  MAP('x', ARRAY(1,2,3), 'y', ARRAY(4,5)) as m2 " + "from test";

        sql.init("test", null, inType, sqlText);

        List<SeaTunnelRow> out = sql.transformBySQL(dummyRow(), inType);
        Assertions.assertEquals(1, out.size());

        Map m2 = (Map) out.get(0).getField(0);
        Assertions.assertNotNull(m2);

        Object[] x = (Object[]) m2.get("x");
        Object[] y = (Object[]) m2.get("y");
        Assertions.assertArrayEquals(
                new int[] {1, 2, 3},
                new int[] {
                    ((Number) x[0]).intValue(),
                    ((Number) x[1]).intValue(),
                    ((Number) x[2]).intValue()
                });
        Assertions.assertArrayEquals(
                new int[] {4, 5},
                new int[] {((Number) y[0]).intValue(), ((Number) y[1]).intValue()});
    }

    @Test
    void testArrayOfMapLiterals() {
        SQLEngine sql = zeta();
        SeaTunnelRowType inType = dummyInputType();

        String sqlText = "select " + "  ARRAY(MAP('aa', 10), MAP('bb', 20)) as a1 " + "from test";

        sql.init("test", null, inType, sqlText);

        List<SeaTunnelRow> out = sql.transformBySQL(dummyRow(), inType);
        Assertions.assertEquals(1, out.size());

        Object[] a1 = (Object[]) out.get(0).getField(0);
        Assertions.assertEquals(2, a1.length);

        Map m0 = (Map) a1[0];
        Map m1 = (Map) a1[1];
        Assertions.assertEquals(10, ((Number) m0.get("aa")).intValue());
        Assertions.assertEquals(20, ((Number) m1.get("bb")).intValue());
    }
}
