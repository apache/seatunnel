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
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime testDateTime = LocalDateTime.of(2025, 5, 20, 14, 30, 45, 123456789);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDateTime});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "SELECT "
                        + "EXTRACT(YEAR FROM event_time) as year, "
                        + "EXTRACT(MONTH FROM event_time) as month, "
                        + "EXTRACT(DAY FROM event_time) as day, "
                        + "EXTRACT(HOUR FROM event_time) as hour, "
                        + "EXTRACT(MINUTE FROM event_time) as minute, "
                        + "EXTRACT(SECOND FROM event_time) as second, "
                        + "EXTRACT(MILLISECOND FROM event_time) as millisecond, "
                        + "EXTRACT(MICROSECONDS FROM event_time) as microseconds, "
                        + "EXTRACT(EPOCH FROM event_time) as epoch, "
                        + "EXTRACT(QUARTER FROM event_time) as quarter, "
                        + "EXTRACT(CENTURY FROM event_time) as century, "
                        + "EXTRACT(DECADE FROM event_time) as decade, "
                        + "EXTRACT(DOW FROM event_time) as dow, "
                        + "EXTRACT(ISODOW FROM event_time) as isodow, "
                        + "EXTRACT(DOY FROM event_time) as doy, "
                        + "EXTRACT(MILLENNIUM FROM event_time) as millennium "
                        + "FROM dual");

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);

        Assertions.assertEquals(2025, outRow.getField(0)); // year
        Assertions.assertEquals(5, outRow.getField(1)); // month
        Assertions.assertEquals(20, outRow.getField(2)); // day
        Assertions.assertEquals(14, outRow.getField(3)); // hour
        Assertions.assertEquals(30, outRow.getField(4)); // minute
        Assertions.assertEquals(45, outRow.getField(5)); // second
        Assertions.assertEquals(123, outRow.getField(6)); // millisecond
        Assertions.assertEquals(123456, outRow.getField(7)); // microseconds

        Assertions.assertEquals(
                (int) testDateTime.toEpochSecond(ZoneOffset.UTC), outRow.getField(8)); // epoch
        Assertions.assertEquals(2, outRow.getField(9)); // quarter
        Assertions.assertEquals(
                21, outRow.getField(10)); // century (2025 belongs to the 21st century)
        Assertions.assertEquals(202, outRow.getField(11)); // decade (2025/10 = 202)
        Assertions.assertEquals(2, outRow.getField(12)); // dow (2025-05-20 is Tuesday, should be 2)
        Assertions.assertEquals(
                2, outRow.getField(13)); // isodow (2025-05-20 is Tuesday, in ISO standard it's 2)
        Assertions.assertEquals(
                140, outRow.getField(14)); // doy (May 20 is the 140th day of the year)
        Assertions.assertEquals(3, outRow.getField(15)); // millennium
    }

    @Test
    public void testLocalDateExtractFunction() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_date"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        LocalDate testDate = LocalDate.of(2025, 5, 20);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDate});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "SELECT "
                        + "EXTRACT(YEAR FROM event_date) as year, "
                        + "EXTRACT(MONTH FROM event_date) as month, "
                        + "EXTRACT(DAY FROM event_date) as day, "
                        + "EXTRACT(QUARTER FROM event_date) as quarter, "
                        + "EXTRACT(CENTURY FROM event_date) as century, "
                        + "EXTRACT(DECADE FROM event_date) as decade, "
                        + "EXTRACT(DOW FROM event_date) as dow, "
                        + "EXTRACT(ISODOW FROM event_date) as isodow, "
                        + "EXTRACT(DOY FROM event_date) as doy, "
                        + "EXTRACT(MILLENNIUM FROM event_date) as millennium, "
                        + "EXTRACT(EPOCH FROM event_date) as epoch "
                        + "FROM dual");

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);

        Assertions.assertEquals(2025, outRow.getField(0));
        Assertions.assertEquals(5, outRow.getField(1));
        Assertions.assertEquals(20, outRow.getField(2));

        Assertions.assertEquals(2, outRow.getField(3));
        Assertions.assertEquals(21, outRow.getField(4));
        Assertions.assertEquals(202, outRow.getField(5));
        Assertions.assertEquals(2, outRow.getField(6));
        Assertions.assertEquals(2, outRow.getField(7));
        Assertions.assertEquals(140, outRow.getField(8));
        Assertions.assertEquals(3, outRow.getField(9));
        Assertions.assertEquals(
                (int) testDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC), outRow.getField(10));
    }

    @Test
    public void testSundayExtractFunction() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"event_time"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime testDateTime = LocalDateTime.of(2025, 5, 25, 14, 30, 45, 123456789);
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {testDateTime});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "SELECT "
                        + "EXTRACT(DOW FROM event_time) as dow, "
                        + "EXTRACT(ISODOW FROM event_time) as isodow, "
                        + "EXTRACT(YEAR FROM event_time) as year, "
                        + "EXTRACT(MONTH FROM event_time) as month, "
                        + "EXTRACT(DAY FROM event_time) as day "
                        + "FROM dual");

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);

        // Verify DOW and ISODOW for Sunday
        Assertions.assertEquals(0, outRow.getField(0));
        Assertions.assertEquals(7, outRow.getField(1));

        // Verify basic date fields
        Assertions.assertEquals(2025, outRow.getField(2));
        Assertions.assertEquals(5, outRow.getField(3));
        Assertions.assertEquals(25, outRow.getField(4));
    }

    @Test
    public void testDateTimeLiteralExpression() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow dummyRow = new SeaTunnelRow(new Object[] {LocalDateTime.now()});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "SELECT "
                        // DATE type tests
                        + "EXTRACT(YEAR FROM DATE '2025-05-21') as date_year, "
                        + "EXTRACT(MONTH FROM DATE '2025-05-21') as date_month, "
                        + "EXTRACT(DAY FROM DATE '2025-05-21') as date_day, "
                        + "EXTRACT(QUARTER FROM DATE '2025-05-21') as date_quarter, "
                        + "EXTRACT(DOW FROM DATE '2025-05-21') as date_dow, "

                        // TIME type tests
                        + "EXTRACT(HOUR FROM TIME '17:57:40') as time_hour, "
                        + "EXTRACT(MINUTE FROM TIME '17:57:40') as time_minute, "
                        + "EXTRACT(SECOND FROM TIME '17:57:40') as time_second, "

                        // TIMESTAMP type tests
                        + "EXTRACT(YEAR FROM TIMESTAMP '2025-05-21T17:57:40') as ts_year, "
                        + "EXTRACT(MONTH FROM TIMESTAMP '2025-05-21T17:57:40') as ts_month, "
                        + "EXTRACT(DAY FROM TIMESTAMP '2025-05-21T17:57:40') as ts_day, "
                        + "EXTRACT(HOUR FROM TIMESTAMP '2025-05-21T17:57:40') as ts_hour, "
                        + "EXTRACT(MINUTE FROM TIMESTAMP '2025-05-21T17:57:40') as ts_minute, "
                        + "EXTRACT(SECOND FROM TIMESTAMP '2025-05-21T17:57:40') as ts_second, "
                        + "EXTRACT(QUARTER FROM TIMESTAMP '2025-05-21T17:57:40') as ts_quarter, "
                        + "EXTRACT(DOW FROM TIMESTAMP '2025-05-21T17:57:40') as ts_dow, "

                        // TIMESTAMP_WITH_TIMEZONE type tests
                        + "EXTRACT(YEAR FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_year, "
                        + "EXTRACT(MONTH FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_month, "
                        + "EXTRACT(DAY FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_day, "
                        + "EXTRACT(HOUR FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_hour, "
                        + "EXTRACT(MINUTE FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_minute, "
                        + "EXTRACT(SECOND FROM TIMESTAMPTZ '2025-05-21T17:57:40.123+08:00') as tstz_second "
                        + "FROM dual");

        SeaTunnelRow outRow = sqlEngine.transformBySQL(dummyRow, rowType).get(0);

        // Verify DATE type extractions
        Assertions.assertEquals(2025, outRow.getField(0));
        Assertions.assertEquals(5, outRow.getField(1));
        Assertions.assertEquals(21, outRow.getField(2));
        Assertions.assertEquals(2, outRow.getField(3));
        Assertions.assertEquals(3, outRow.getField(4));

        // Verify TIME type extractions
        Assertions.assertEquals(17, outRow.getField(5));
        Assertions.assertEquals(57, outRow.getField(6));
        Assertions.assertEquals(40, outRow.getField(7));

        // Verify TIMESTAMP type extractions
        Assertions.assertEquals(2025, outRow.getField(8));
        Assertions.assertEquals(5, outRow.getField(9));
        Assertions.assertEquals(21, outRow.getField(10));
        Assertions.assertEquals(17, outRow.getField(11));
        Assertions.assertEquals(57, outRow.getField(12));
        Assertions.assertEquals(40, outRow.getField(13));
        Assertions.assertEquals(2, outRow.getField(14));
        Assertions.assertEquals(3, outRow.getField(15));

        // Verify TIMESTAMP_WITH_TIMEZONE type extractions
        Assertions.assertEquals(2025, outRow.getField(16));
        Assertions.assertEquals(5, outRow.getField(17));
        Assertions.assertEquals(21, outRow.getField(18));
        Assertions.assertEquals(17, outRow.getField(19));
        Assertions.assertEquals(57, outRow.getField(20));
        Assertions.assertEquals(40, outRow.getField(21));
    }
}
