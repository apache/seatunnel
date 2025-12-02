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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class SQLDateTimeFunctionsTest {

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
        // Test fix: DATEDIFF MONTH should count total months including year difference
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

        // Should be 14 months (12 + 2), not just 2
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
                        LocalDateTime.of(2024, 6, 15, 14, 30, 45));

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

        // 2024-01-01 is Monday of week 1
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
    public void testDateTrunc() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select DATE_TRUNC(dt, 'MONTH') as truncated from dual",
                        rowType,
                        LocalDateTime.of(2024, 6, 15, 14, 30, 45));

        LocalDateTime expected = LocalDateTime.of(2024, 6, 1, 0, 0, 0);
        Assertions.assertEquals(expected, outRow.getField(0));
    }

    @Test
    public void testNullDateHandling() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql("select YEAR(dt) as y, MONTH(dt) as m from dual", rowType, (Object) null);

        Assertions.assertNull(outRow.getField(0));
        Assertions.assertNull(outRow.getField(1));
    }

    @Test
    public void testDayOfWeekAndDayOfYear() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        // 2024-06-15 is Saturday (day 6 in ISO), day 167 of year
        SeaTunnelRow outRow =
                runSql(
                        "select DAY_OF_WEEK(dt) as dow, DAY_OF_YEAR(dt) as doy from dual",
                        rowType,
                        LocalDate.of(2024, 6, 15));

        Assertions.assertEquals(6, outRow.getField(0));
        Assertions.assertEquals(167, outRow.getField(1));
    }

    @Test
    public void testDayOfMonthWeekYearFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        LocalDate date = LocalDate.of(2024, 6, 15);
        SeaTunnelRow outRow =
                runSql(
                        "select DAY_OF_MONTH(dt) as dom,"
                                + " DAY_OF_WEEK(dt) as dow,"
                                + " DAY_OF_YEAR(dt) as doy"
                                + " from dual",
                        rowType,
                        date);

        Assertions.assertEquals(date.getDayOfMonth(), outRow.getField(0));
        Assertions.assertEquals(date.getDayOfWeek().getValue(), outRow.getField(1));
        Assertions.assertEquals(date.getDayOfYear(), outRow.getField(2));
    }

    @Test
    public void testQuarter() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql("select QUARTER(dt) as q from dual", rowType, LocalDate.of(2024, 6, 15));

        Assertions.assertEquals(2, outRow.getField(0));
    }

    @Test
    public void testFromUnixTime() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {
                            org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE
                        });

        // 1672545600 = 2023-01-01 12:00:00 UTC+8
        SeaTunnelRow outRow =
                runSql(
                        "select FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss', 'UTC+6') as formatted from dual",
                        rowType,
                        1672545600L);

        Assertions.assertEquals("2023-01-01 10:00:00", outRow.getField(0));
    }

    @Test
    public void testAtTimeZone() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime now = LocalDateTime.of(2024, 6, 15, 12, 0, 0);
        SeaTunnelRow outRow =
                runSql("select dt AT TIME ZONE '+09:00' as tz from dual", rowType, now);

        // Result should be OffsetDateTime
        Assertions.assertNotNull(outRow.getField(0));
        Assertions.assertEquals(
                now.atZone(ZoneId.systemDefault())
                        .withZoneSameInstant(ZoneId.of("+09:00"))
                        .toOffsetDateTime(),
                outRow.getField(0));
    }

    @Test
    public void testIsDateFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select IS_DATE('2021-04-08 13:34:45', 'yyyy-MM-dd HH:mm:ss') as valid,"
                                + " IS_DATE('bad', 'yyyy-MM-dd HH:mm:ss') as invalid from dual",
                        rowType,
                        LocalDateTime.now());

        Assertions.assertEquals(true, outRow.getField(0));
        Assertions.assertEquals(false, outRow.getField(1));
    }

    @Test
    public void testParseDateTimeFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select PARSEDATETIME('2021-04-08 13:34:45', 'yyyy-MM-dd HH:mm:ss') as parsed from dual",
                        rowType,
                        LocalDateTime.now());

        Assertions.assertEquals(LocalDateTime.of(2021, 4, 8, 13, 34, 45), outRow.getField(0));
    }

    @Test
    public void testCurrentDateTimeFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dummy"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select CURRENT_DATE as cd,"
                                + " CURRENT_TIME as ct,"
                                + " CURRENT_TIMESTAMP as cts,"
                                + " NOW() as now_ts"
                                + " from dual",
                        rowType,
                        LocalDateTime.now());

        Assertions.assertTrue(outRow.getField(0) instanceof LocalDate);
        Assertions.assertTrue(outRow.getField(1) instanceof LocalTime);
        Assertions.assertTrue(outRow.getField(2) instanceof LocalDateTime);
        Assertions.assertTrue(outRow.getField(3) instanceof LocalDateTime);

        LocalDate cd = (LocalDate) outRow.getField(0);
        LocalDateTime cts = (LocalDateTime) outRow.getField(2);
        LocalDateTime nowTs = (LocalDateTime) outRow.getField(3);

        Assertions.assertEquals(cd, cts.toLocalDate());
        Assertions.assertEquals(cts, nowTs);
    }

    @Test
    public void testDaynameAndMonthnameFunctions() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});

        SeaTunnelRow outRow =
                runSql(
                        "select DAYNAME(dt) as dn, MONTHNAME(dt) as mn from dual",
                        rowType,
                        LocalDate.of(2024, 6, 15));

        Assertions.assertEquals("Saturday", outRow.getField(0));
        Assertions.assertEquals("June", outRow.getField(1));
    }

    @Test
    public void testDateAddWithVariousUnits() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        LocalDateTime base = LocalDateTime.of(2024, 6, 15, 12, 0, 0);
        SeaTunnelRow outRow =
                runSql(
                        "select DATEADD(dt, 1, 'DAY') as d1,"
                                + " DATEADD(dt, 2, 'YEAR') as d2,"
                                + " DATEADD(dt, 10, 'HOUR') as d3,"
                                + " DATEADD(dt, 30, 'MINUTE') as d4,"
                                + " DATEADD(dt, 15, 'SECOND') as d5,"
                                + " TIMESTAMPADD(dt, 1, 'DAY') as t1"
                                + " from dual",
                        rowType,
                        base);

        Assertions.assertEquals(base.plusDays(1), outRow.getField(0));
        Assertions.assertEquals(base.plusYears(2), outRow.getField(1));
        Assertions.assertEquals(base.plusHours(10), outRow.getField(2));
        Assertions.assertEquals(base.plusMinutes(30), outRow.getField(3));
        Assertions.assertEquals(base.plusSeconds(15), outRow.getField(4));
        Assertions.assertEquals(base.plusDays(1), outRow.getField(5));
    }

    @Test
    public void testDateDiffWithOtherUnits() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"dt1", "dt2"},
                        new SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TIME_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        LocalDateTime dt1 = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
        LocalDateTime dt2 = LocalDateTime.of(2024, 1, 2, 1, 1, 0);

        SeaTunnelRow outRow =
                runSql(
                        "select DATEDIFF(dt1, dt2, 'YEAR') as dy,"
                                + " DATEDIFF(dt1, dt2, 'HOUR') as dh,"
                                + " DATEDIFF(dt1, dt2, 'MINUTE') as dm,"
                                + " DATEDIFF(dt1, dt2, 'SECOND') as ds"
                                + " from dual",
                        rowType,
                        dt1,
                        dt2);

        Assertions.assertEquals(0L, outRow.getField(0));
        Assertions.assertEquals(25L, outRow.getField(1));
        Assertions.assertEquals(25L * 60 + 1, outRow.getField(2));
        Assertions.assertEquals((25L * 60 + 1) * 60, outRow.getField(3));
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
                TransformException.class,
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
}
