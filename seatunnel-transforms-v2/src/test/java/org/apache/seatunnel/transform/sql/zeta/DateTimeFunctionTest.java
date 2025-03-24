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

public class DateTimeFunctionTest {

    @Test
    public void testFromUnixtimeFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"unixtime"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});

        // 1672502400 means `2023-01-01 12:00:00 UTC+8` in unix time
        Long unixTime = 1672545600L;
        SeaTunnelRow inputRow = new SeaTunnelRow(new Long[] {unixTime});

        // transform by `from_unixtime` function
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select from_unixtime(unixtime,'yyyy-MM-dd') as ts from dual");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object field = outRow.getField(0);
        Assertions.assertNotNull(field.toString());

        // transform by `from_unixtime` time zone function
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select from_unixtime(unixtime,'yyyy-MM-dd HH:mm:ss','UTC+6') as ts from dual");
        SeaTunnelRow outRow1 = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object field1 = outRow1.getField(0);
        Assertions.assertEquals("2023-01-01 10:00:00", field1.toString());
    }
}
