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

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ExtractFunctionTest {

    @Test
    public void testLocalDateTimeExtractFunction() {
        // Test using EXTRACT function through SQL engine
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        // Use 2025-05-20 14:30:45 as test time
        LocalDateTime testDateTime = LocalDateTime.of(2025, 5, 20, 14, 30, 45, 123456789);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDateTime});

        // Test basic fields
        // Test YEAR field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(YEAR FROM event_time) as year from dual");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2025, outRow.getField(0));

        // Test MONTH field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(MONTH FROM event_time) as month from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(5, outRow.getField(0));

        // Test DAY field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DAY FROM event_time) as day from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(20, outRow.getField(0));

        // Test HOUR field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(HOUR FROM event_time) as hour from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(14, outRow.getField(0));

        // Test MINUTE field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(MINUTE FROM event_time) as minute from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(30, outRow.getField(0));

        // Test SECOND field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(SECOND FROM event_time) as second from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(45, outRow.getField(0));

        // Test MILLISECOND field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(MILLISECOND FROM event_time) as millisecond from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(123, outRow.getField(0));

        // Test newly added PostgreSQL compatible fields
        // Test MICROSECONDS field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(MICROSECONDS FROM event_time) as microseconds from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(123456, outRow.getField(0));

        // Test EPOCH field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(EPOCH FROM event_time) as epoch from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                (int) testDateTime.toEpochSecond(ZoneOffset.UTC), outRow.getField(0));

        // Test QUARTER field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(QUARTER FROM event_time) as quarter from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2, outRow.getField(0));

        // Test CENTURY field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(CENTURY FROM event_time) as century from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(21, outRow.getField(0)); // 2025 belongs to the 21st century

        // Test DECADE field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(DECADE FROM event_time) as decade from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(202, outRow.getField(0)); // 2025/10 = 202

        // Test DOW field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DOW FROM event_time) as dow from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2, outRow.getField(0)); // 2025-05-20 is Tuesday, should return 1

        // Test ISODOW field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(ISODOW FROM event_time) as isodow from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                2, outRow.getField(0)); // 2025-05-20 is Tuesday, in ISO standard it's 2

        // Test DOY field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DOY FROM event_time) as doy from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(140, outRow.getField(0)); // May 20 is the 140th day of the year

        // Test MILLENNIUM field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(MILLENNIUM FROM event_time) as millennium from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(3, outRow.getField(0));
    }

    @Test
    public void testLocalDateExtractFunction() {
        // Test using EXTRACT function with LocalDate through SQL engine
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_date"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        // Use 2025-05-20 as test date
        LocalDate testDate = LocalDate.of(2025, 5, 20);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDate});

        // Test YEAR field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(YEAR FROM event_date) as year from dual");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2025, outRow.getField(0));

        // Test MONTH field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(MONTH FROM event_date) as month from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(5, outRow.getField(0));

        // Test DAY field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DAY FROM event_date) as day from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(20, outRow.getField(0));

        // Test PostgreSQL compatible fields
        // Test QUARTER field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(QUARTER FROM event_date) as quarter from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2, outRow.getField(0));

        // Test CENTURY field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(CENTURY FROM event_date) as century from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(21, outRow.getField(0)); // 2025 belongs to the 21st century

        // Test DECADE field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(DECADE FROM event_date) as decade from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(202, outRow.getField(0)); // 2025/10 = 202

        // Test DOW field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DOW FROM event_date) as dow from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(2, outRow.getField(0)); // 2025-05-20 is Tuesday, should return 1

        // Test ISODOW field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(ISODOW FROM event_date) as isodow from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                2, outRow.getField(0)); // 2025-05-20 is Tuesday, in ISO standard it's 2

        // Test DOY field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DOY FROM event_date) as doy from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(140, outRow.getField(0)); // May 20 is the 140th day of the year

        // Test MILLENNIUM field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(MILLENNIUM FROM event_date) as millennium from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(3, outRow.getField(0));

        // Test EPOCH field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(EPOCH FROM event_date) as epoch from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                (int) testDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC), outRow.getField(0));
    }

    @Test
    public void testSundayExtractFunction() {
        // Test using EXTRACT function through SQL engine
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create test data
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime testDateTime = LocalDateTime.of(2025, 5, 25, 14, 30, 45, 123456789);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDateTime});

        // Test DOW field
        sqlEngine.init(
                "test", null, rowType, "select EXTRACT(DOW FROM event_time) as dow from dual");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(0, outRow.getField(0));

        // Test ISODOW field
        sqlEngine.init(
                "test",
                null,
                rowType,
                "select EXTRACT(ISODOW FROM event_time) as isodow from dual");
        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(7, outRow.getField(0));
    }
}
