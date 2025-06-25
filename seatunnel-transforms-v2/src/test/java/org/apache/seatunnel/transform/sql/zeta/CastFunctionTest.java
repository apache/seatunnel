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
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CastFunctionTest {

    @Test
    public void testCastFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        String f1 = "1";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {f1});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select f1, cast(f1 as TINYINT) as f2, cast(f1 as SMALLINT) as f3 from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Object f2Object = outRow.getField(1);
        Object f3Object = outRow.getField(2);
        Assertions.assertEquals("1", f1Object);
        Assertions.assertEquals(Byte.parseByte("1"), f2Object);
        Assertions.assertEquals(Short.parseShort("1"), f3Object);
    }
}
