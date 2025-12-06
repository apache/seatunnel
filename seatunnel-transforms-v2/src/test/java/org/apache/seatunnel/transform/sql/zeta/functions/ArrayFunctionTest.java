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

class ArrayFunctionTest {
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
    void testNestedArrayEvaluateWithSQLEngine() {
        SQLEngine sql = zeta();
        SeaTunnelRowType inType = dummyInputType();

        String sqlText = "select ARRAY(ARRAY(1,2), ARRAY(3,4)) as a from test";
        sql.init("test", null, inType, sqlText);

        List<SeaTunnelRow> out = sql.transformBySQL(dummyRow(), inType);
        Assertions.assertEquals(1, out.size());

        Object field0 = out.get(0).getField(0);
        Assertions.assertTrue(field0 instanceof Object[], "outer should be array");
        Object[] outer = (Object[]) field0;
        Assertions.assertEquals(2, outer.length);

        Assertions.assertTrue(outer[0] instanceof Object[], "inner[0] should be array");
        Assertions.assertTrue(outer[1] instanceof Object[], "inner[1] should be array");

        Object[] inner1 = (Object[]) outer[0];
        Object[] inner2 = (Object[]) outer[1];
        Assertions.assertEquals(2, inner1.length);
        Assertions.assertEquals(2, inner2.length);

        Assertions.assertEquals(1, ((Number) inner1[0]).intValue());
        Assertions.assertEquals(2, ((Number) inner1[1]).intValue());
        Assertions.assertEquals(3, ((Number) inner2[0]).intValue());
        Assertions.assertEquals(4, ((Number) inner2[1]).intValue());
    }
}
