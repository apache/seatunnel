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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;
import org.apache.seatunnel.transform.sql.zeta.functions.DateTimeFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;

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

    @Test
    public void testAtTimeZoneFunction() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"local_date_time", "offset_date_time"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TIME_TYPE, LocalTimeType.OFFSET_DATE_TIME_TYPE
                        });

        LocalDateTime now = LocalDateTime.now();
        SeaTunnelRow inputRow =
                new SeaTunnelRow(
                        new Object[] {now, now.atZone(ZoneId.systemDefault()).toOffsetDateTime()});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select local_date_time AT TIME ZONE '+09:00' as date_time_with_zone,"
                        + "offset_date_time AT TIME ZONE '-05:00' as offset_date_time_with_zone"
                        + " from dual");
        SeaTunnelRowType seaTunnelRowType = sqlEngine.typeMapping(new ArrayList<>());
        Assertions.assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, seaTunnelRowType.getFieldType(0));

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                now.atZone(ZoneId.systemDefault())
                        .withZoneSameInstant(ZoneId.of("+09:00"))
                        .toOffsetDateTime(),
                outRow.getField(0));
        Assertions.assertEquals(
                now.atZone(ZoneId.systemDefault())
                        .withZoneSameInstant(ZoneId.of("-05:00"))
                        .toOffsetDateTime(),
                outRow.getField(1));

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select local_date_time AT TIME ZONE 'Asia/Tokyo' as date_time_with_zone,"
                        + "offset_date_time AT TIME ZONE 'Pacific/Honolulu' as offset_date_time_with_zone"
                        + " from dual");
        seaTunnelRowType = sqlEngine.typeMapping(new ArrayList<>());
        Assertions.assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, seaTunnelRowType.getFieldType(0));
        Assertions.assertEquals(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, seaTunnelRowType.getFieldType(1));

        outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Assertions.assertEquals(
                now.atZone(ZoneId.systemDefault())
                        .withZoneSameInstant(ZoneId.of("+09:00"))
                        .toOffsetDateTime(),
                outRow.getField(0));
        Assertions.assertEquals(
                now.atZone(ZoneId.systemDefault())
                        .withZoneSameInstant(ZoneId.of("-10:00"))
                        .toOffsetDateTime(),
                outRow.getField(1));
    }

    @Test
    public void testFromUnixtimeFunctionWithIntegerInput() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Test with Integer type (simulating MySQL INT field)
        SeaTunnelRowType rowTypeInt =
                new SeaTunnelRowType(
                        new String[] {"unixtime"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        // 1672545600 means `2023-01-01 12:00:00 UTC+8` in unix time (as Integer)
        Integer unixTimeInt = 1672545600;
        SeaTunnelRow inputRowInt = new SeaTunnelRow(new Integer[] {unixTimeInt});

        // Transform by `from_unixtime` function with Integer input
        sqlEngine.init(
                "test",
                null,
                rowTypeInt,
                "select from_unixtime(unixtime,'yyyy-MM-dd HH:mm:ss') as ts from dual");
        SeaTunnelRow outRowInt = sqlEngine.transformBySQL(inputRowInt, rowTypeInt).get(0);
        Object fieldInt = outRowInt.getField(0);
        Assertions.assertNotNull(fieldInt.toString());

        // Test with Long type (original working case)
        SeaTunnelRowType rowTypeLong =
                new SeaTunnelRowType(
                        new String[] {"unixtime"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});

        Long unixTimeLong = 1672545600L;
        SeaTunnelRow inputRowLong = new SeaTunnelRow(new Long[] {unixTimeLong});

        // Transform by `from_unixtime` function with Long input
        sqlEngine.init(
                "test",
                null,
                rowTypeLong,
                "select from_unixtime(unixtime,'yyyy-MM-dd HH:mm:ss') as ts from dual");
        SeaTunnelRow outRowLong = sqlEngine.transformBySQL(inputRowLong, rowTypeLong).get(0);
        Object fieldLong = outRowLong.getField(0);
        Assertions.assertNotNull(fieldLong.toString());

        // Both Integer and Long inputs should produce the same result
        Assertions.assertEquals(fieldInt.toString(), fieldLong.toString());
    }

    @Test
    public void testDateDiffMonthAcrossYearUsesTotalMonths() {
        LocalDate start = LocalDate.of(2023, 1, 1);
        LocalDate end = LocalDate.of(2024, 3, 1);

        Long months = DateTimeFunction.datediff(Arrays.asList(start, end, "MONTH"));

        Assertions.assertEquals(14L, months);
    }
}
