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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class DateTimeFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    @Test
    public void testDateAddAndDateSub() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select DATEADD(dt, 1, 'DAY') as d1, DATEADD(dt, -1, 'MONTH') as d2 from dual",
                        rowType,
                        LocalDate.of(2024, 1, 15));

        Assertions.assertEquals(LocalDate.of(2024, 1, 16), outRow.getField(0));
        Assertions.assertEquals(LocalDate.of(2023, 12, 15), outRow.getField(1));
    }

    @Test
    public void testDateDiffDays() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt1", "dt2"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TYPE
                        });

        SeaTunnelRow outRow =
                runSql(
                        "select DATEDIFF(dt1, dt2, 'DAY') as diff from dual",
                        rowType,
                        LocalDate.of(2024, 1, 1),
                        LocalDate.of(2024, 1, 10));

        Assertions.assertEquals(9L, outRow.getField(0));
    }

    @Test
    public void testDateDiffMonthsCrossYear() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt1", "dt2"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TYPE
                        });

        SeaTunnelRow outRow =
                runSql(
                        "select DATEDIFF(dt1, dt2, 'MONTH') as diff from dual",
                        rowType,
                        LocalDate.of(2023, 1, 1),
                        LocalDate.of(2024, 3, 1));

        Assertions.assertEquals(14L, outRow.getField(0));
    }

    @Test
    public void testExtractFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select EXTRACT(YEAR FROM dt) as y,"
                                + " EXTRACT(MONTH FROM dt) as m,"
                                + " EXTRACT(DAY FROM dt) as d,"
                                + " EXTRACT(HOUR FROM dt) as h"
                                + " from dual",
                        rowType,
                        LocalDateTime.of(2024, 6, 15, 14, 30, 0));

        Assertions.assertEquals(2024, outRow.getField(0));
        Assertions.assertEquals(6, outRow.getField(1));
        Assertions.assertEquals(15, outRow.getField(2));
        Assertions.assertEquals(14, outRow.getField(3));
    }

    @Test
    public void testFormatDateTime() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select FORMATDATETIME(dt, 'yyyy-MM-dd') as formatted from dual",
                        rowType,
                        LocalDateTime.of(2024, 6, 15, 14, 30, 45));

        Assertions.assertEquals("2024-06-15", outRow.getField(0));
    }

    @Test
    public void testWeekFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql("select WEEK(dt) as w from dual", rowType, LocalDate.of(2024, 1, 1));

        Assertions.assertEquals(1, outRow.getField(0));
    }

    @Test
    public void testYearMonthDayFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select YEAR(dt) as y, MONTH(dt) as m, DAY_OF_MONTH(dt) as d from dual",
                        rowType,
                        LocalDate.of(2024, 6, 15));

        Assertions.assertEquals(2024, outRow.getField(0));
        Assertions.assertEquals(6, outRow.getField(1));
        Assertions.assertEquals(15, outRow.getField(2));
    }

    @Test
    public void testHourMinuteSecond() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select HOUR(dt) as h, MINUTE(dt) as m, SECOND(dt) as s from dual",
                        rowType,
                        LocalDateTime.of(2024, 6, 15, 14, 30, 45));

        Assertions.assertEquals(14, outRow.getField(0));
        Assertions.assertEquals(30, outRow.getField(1));
        Assertions.assertEquals(45, outRow.getField(2));
    }

    @Test
    public void testDateTruncWithVariousUnits() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime base = LocalDateTime.of(2024, 6, 15, 14, 30, 45);
        SeaTunnelRow outRow =
                runSql(
                        "select DATE_TRUNC(dt, 'YEAR') as y,"
                                + " DATE_TRUNC(dt, 'DAY') as d,"
                                + " DATE_TRUNC(dt, 'HOUR') as h,"
                                + " DATE_TRUNC(dt, 'MINUTE') as m,"
                                + " DATE_TRUNC(dt, 'SECOND') as s"
                                + " from dual",
                        rowType,
                        base);

        Assertions.assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0), outRow.getField(0));
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 0, 0, 0), outRow.getField(1));
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 0, 0), outRow.getField(2));
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 0), outRow.getField(3));
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 45), outRow.getField(4));
    }

    @Test
    public void testFromUnixTimeWithZone() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"unixtime"},
                        new SeaTunnelDataType[] {
                            org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE
                        });

        long unixTime = LocalDateTime.of(2023, 1, 1, 0, 0).atZone(ZoneId.of("UTC")).toEpochSecond();

        SeaTunnelRow outRow =
                runSql(
                        "select FROM_UNIXTIME(unixtime, 'yyyy-MM-dd HH:mm:ss', 'UTC+8') as ts from dual",
                        rowType,
                        unixTime);

        Assertions.assertEquals("2023-01-01 08:00:00", outRow.getField(0));
    }

    @Test
    public void testToDateAliasFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select TO_DATE('2021-04-08T13:34:45', 'yyyy-MM-dd''T''HH:mm:ss') as dt from dual",
                        rowType,
                        LocalDateTime.now());

        Assertions.assertEquals(LocalDateTime.of(2021, 4, 8, 13, 34, 45), outRow.getField(0));
    }

    @Test
    public void testNestedDateTimeFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime base = LocalDateTime.of(2024, 6, 15, 12, 0, 0);
        SeaTunnelRow outRow =
                runSql(
                        "select FORMATDATETIME(DATEADD(dt, 1, 'DAY'), 'yyyy-MM-dd') as f1,"
                                + " EXTRACT(DAYOFWEEK FROM DATEADD(dt, 1, 'DAY')) as dow"
                                + " from dual",
                        rowType,
                        base);

        LocalDate nextDay = base.plusDays(1).toLocalDate();
        Assertions.assertEquals("2024-06-16", outRow.getField(0));
        int expectedDow = nextDay.getDayOfWeek().getValue() % 7;
        Assertions.assertEquals(expectedDow, outRow.getField(1));
    }

    @Test
    public void testNestedIsDateAndToDate() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"s"},
                        new SeaTunnelDataType[] {
                            org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE
                        });

        SeaTunnelRow outRow =
                runSql(
                        "select CASE WHEN IS_DATE(s, 'yyyy-MM-dd')"
                                + " THEN TO_DATE(s, 'yyyy-MM-dd')"
                                + " ELSE null END as dt from dual",
                        rowType,
                        "2024-06-15");

        Assertions.assertEquals(LocalDate.of(2024, 6, 15), outRow.getField(0));
    }

    @Test
    public void testParseDateTimeWithInvalidPattern() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        runSql(
                                "select PARSEDATETIME('2021-04-08', 'invalid_pattern') as parsed from dual",
                                rowType,
                                LocalDateTime.now()));
    }

    @Test
    public void testDateAddWithUnsupportedField() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        Assertions.assertThrows(
                TransformException.class,
                () ->
                        runSql(
                                "select DATEADD(dt, 1, 'UNSUPPORTED') as d from dual",
                                rowType,
                                LocalDate.of(2024, 6, 15)));
    }

    @Test
    public void testParseDateTimeWithAllDateTimeFormats() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        // DATETIME_STANDARD: yyyy-MM-dd HH:mm:ss
        SeaTunnelRow row1 =
                runSql(
                        "select PARSEDATETIME('2024-06-15 14:30:45', 'yyyy-MM-dd HH:mm:ss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 45), row1.getField(0));

        // DATETIME_WITH_MILLIS: yyyy-MM-dd HH:mm:ss.SSS
        SeaTunnelRow row2 =
                runSql(
                        "select PARSEDATETIME('2024-06-15 14:30:45.123', 'yyyy-MM-dd HH:mm:ss.SSS') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(
                LocalDateTime.of(2024, 6, 15, 14, 30, 45, 123000000), row2.getField(0));

        // DATETIME_ISO8601: yyyy-MM-dd'T'HH:mm:ss
        SeaTunnelRow row3 =
                runSql(
                        "select PARSEDATETIME('2024-06-15T14:30:45', 'yyyy-MM-dd''T''HH:mm:ss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 45), row3.getField(0));

        // DATETIME_ISO8601_WITH_MILLIS: yyyy-MM-dd'T'HH:mm:ss.SSS
        SeaTunnelRow row4 =
                runSql(
                        "select PARSEDATETIME('2024-06-15T14:30:45.987', 'yyyy-MM-dd''T''HH:mm:ss.SSS') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(
                LocalDateTime.of(2024, 6, 15, 14, 30, 45, 987000000), row4.getField(0));

        // DATETIME_SLASH: yyyy/MM/dd HH:mm:ss
        SeaTunnelRow row5 =
                runSql(
                        "select PARSEDATETIME('2024/06/15 14:30:45', 'yyyy/MM/dd HH:mm:ss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 45), row5.getField(0));

        // DATETIME_SLASH_WITH_MILLIS: yyyy/MM/dd HH:mm:ss.SSS
        SeaTunnelRow row6 =
                runSql(
                        "select PARSEDATETIME('2024/06/15 14:30:45.123', 'yyyy/MM/dd HH:mm:ss.SSS') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(
                LocalDateTime.of(2024, 6, 15, 14, 30, 45, 123000000), row6.getField(0));

        // DATETIME_COMPACT: yyyyMMddHHmmss
        SeaTunnelRow row7 =
                runSql(
                        "select PARSEDATETIME('20240615143045', 'yyyyMMddHHmmss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDateTime.of(2024, 6, 15, 14, 30, 45), row7.getField(0));
    }

    @Test
    public void testParseDateTimeWithAllTimeFormats() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        // TIME_STANDARD: HH:mm:ss
        SeaTunnelRow row1 =
                runSql(
                        "select PARSEDATETIME('14:30:45', 'HH:mm:ss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(java.time.LocalTime.of(14, 30, 45), row1.getField(0));

        // TIME_WITH_MILLIS: HH:mm:ss.SSS
        SeaTunnelRow row2 =
                runSql(
                        "select PARSEDATETIME('14:30:45.123', 'HH:mm:ss.SSS') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(java.time.LocalTime.of(14, 30, 45, 123000000), row2.getField(0));

        // TIME_COMPACT: HHmmss
        SeaTunnelRow row3 =
                runSql(
                        "select PARSEDATETIME('143045', 'HHmmss') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(java.time.LocalTime.of(14, 30, 45), row3.getField(0));
    }

    @Test
    public void testParseDateTimeWithUnsupportedFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        runSql(
                                "select PARSEDATETIME('2024-06-15', 'dd/MM/yyyy') as dt from dual",
                                rowType,
                                LocalDateTime.now()));
    }

    @Test
    public void testParseDateTimeWithMalformedInput() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        runSql(
                                "select PARSEDATETIME('not-a-date', 'yyyy-MM-dd') as dt from dual",
                                rowType,
                                LocalDateTime.now()));
    }

    @Test
    public void testParseDateTimeWithAllDateFormats() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        // DATE_ISO8601: yyyy-MM-dd
        SeaTunnelRow row1 =
                runSql(
                        "select TO_DATE('2024-06-15', 'yyyy-MM-dd') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDate.of(2024, 6, 15), row1.getField(0));

        // DATE_SLASH: yyyy/MM/dd
        SeaTunnelRow row2 =
                runSql(
                        "select PARSEDATETIME('2024/06/15', 'yyyy/MM/dd') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDate.of(2024, 6, 15), row2.getField(0));

        // DATE_COMPACT: yyyyMMdd
        SeaTunnelRow row3 =
                runSql(
                        "select PARSEDATETIME('20240615', 'yyyyMMdd') as dt from dual",
                        rowType,
                        LocalDateTime.now());
        Assertions.assertEquals(LocalDate.of(2024, 6, 15), row3.getField(0));
    }
}
