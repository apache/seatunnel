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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

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
                                + " EXTRACT(HOUR FROM dt) as h from dual",
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
                                + " DATE_TRUNC(dt, 'SECOND') as s from dual",
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
                        new String[] {"ts"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});

        // 1672545600 = 2023-01-01 10:00:00 UTC+6, when timestamp is in UTC+8
        SeaTunnelRow outRow =
                runSql(
                        "select FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss', 'UTC+6') as formatted from dual",
                        rowType,
                        1672545600L);

        Assertions.assertEquals("2023-01-01 10:00:00", outRow.getField(0));
    }

    @Test
    public void testAtTimeZone() {
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"dt"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

            LocalDateTime now = LocalDateTime.of(2024, 6, 15, 12, 0, 0);
            ZoneId fixedZone = ZoneId.of("UTC");
            SeaTunnelRow outRow =
                    runSql("select dt AT TIME ZONE '+09:00' as tz from dual", rowType, now);

            Assertions.assertNotNull(outRow.getField(0));
            Assertions.assertEquals(
                    now.atZone(fixedZone)
                            .withZoneSameInstant(ZoneId.of("+09:00"))
                            .toOffsetDateTime(),
                    outRow.getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testIsDateFunction() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        SeaTunnelRow outRow =
                runSql("select IS_DATE(s, 'yyyy-MM-dd') as r from dual", rowType, "2024-06-15");

        Assertions.assertEquals(true, outRow.getField(0));
    }

    @Test
    public void testNestedIsDateAndToDate() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

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
    public void testDateAndTimeNullHandling() {
        SeaTunnelRowType dateType =
                new SeaTunnelRowType(
                        new String[] {"d"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
        SeaTunnelRowType timeType =
                new SeaTunnelRowType(
                        new String[] {"t"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_TIME_TYPE});

        SeaTunnelRow dateRow =
                runSql("select YEAR(d) as y, MONTH(d) as m from dual", dateType, (Object) null);
        Assertions.assertNull(dateRow.getField(0));
        Assertions.assertNull(dateRow.getField(1));

        SeaTunnelRow timeRow =
                runSql("select HOUR(t) as h, MINUTE(t) as m from dual", timeType, (Object) null);
        Assertions.assertNull(timeRow.getField(0));
        Assertions.assertNull(timeRow.getField(1));
    }

    @Test
    public void testUnixTimestamp() {
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

            // 1) no-arg: current epoch seconds (flake-tolerant window for CI)
            SeaTunnelRow noArg =
                    runSql(
                            "select UNIX_TIMESTAMP() as ts from dual",
                            new SeaTunnelRowType(
                                    new String[] {"x"},
                                    new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                            "placeholder");
            long now = Instant.now().getEpochSecond();
            Object noArgTs = noArg.getField(0);
            Assertions.assertNotNull(noArgTs);
            Assertions.assertTrue(
                    Math.abs((Long) noArgTs - now) < 60,
                    "UNIX_TIMESTAMP() should be within 60s of now");

            // 2) string with default pattern yyyy-MM-dd HH:mm:ss
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s) as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00")
                            .getField(0));

            // 3) string with custom pattern
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'yyyy/MM/dd HH:mm:ss') as ts from dual",
                                    strType,
                                    "2023/01/01 00:00:00")
                            .getField(0));

            // 4) date-only pattern -> midnight of that day
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'yyyy-MM-dd') as ts from dual",
                                    strType,
                                    "2023-01-01")
                            .getField(0));

            // 5) null input -> null
            Assertions.assertNull(
                    runSql("select UNIX_TIMESTAMP(s) as ts from dual", strType, new Object[] {null})
                            .getField(0));

            // 6) unparseable string -> null (does not raise on bad input)
            Assertions.assertNull(
                    runSql("select UNIX_TIMESTAMP(s) as ts from dual", strType, "not-a-date")
                            .getField(0));

            // 6b) explicit NULL pattern -> null (propagates, does not fall back to default)
            Assertions.assertNull(
                    runSql(
                                    "select UNIX_TIMESTAMP(s, null) as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00")
                            .getField(0));

            // 7) numeric input is not supported -> null
            SeaTunnelRowType longType =
                    new SeaTunnelRowType(
                            new String[] {"n"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
            Assertions.assertNull(
                    runSql("select UNIX_TIMESTAMP(n) as ts from dual", longType, 1672531200L)
                            .getField(0));

            // 8) typed LocalDateTime argument
            SeaTunnelRowType dtType =
                    new SeaTunnelRowType(
                            new String[] {"dt"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(dt) as ts from dual",
                                    dtType,
                                    LocalDateTime.of(2023, 1, 1, 0, 0, 0))
                            .getField(0));

            // 9) typed OffsetDateTime argument (offset-aware, TZ-independent)
            SeaTunnelRowType odtType =
                    new SeaTunnelRowType(
                            new String[] {"odt"},
                            new SeaTunnelDataType[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(odt) as ts from dual",
                                    odtType,
                                    OffsetDateTime.of(2023, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
                            .getField(0));

            // 10) typed LocalDate argument (DATE column, resolved at midnight)
            SeaTunnelRowType dateType =
                    new SeaTunnelRowType(
                            new String[] {"d"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TYPE});
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(d) as ts from dual",
                                    dateType,
                                    LocalDate.of(2023, 1, 1))
                            .getField(0));

            // 11) timezone contract: the string overload resolves against the JVM default
            // zone, not a hardcoded UTC. Under Asia/Shanghai (UTC+8), 2023-01-01 00:00:00
            // maps to 1672502400 (28800s earlier than UTC's 1672531200).
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            Assertions.assertEquals(
                    1672502400L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s) as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00")
                            .getField(0));
            // OffsetDateTime is offset-aware, so its epoch is zone-independent even here.
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(odt) as ts from dual",
                                    odtType,
                                    OffsetDateTime.of(2023, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
                            .getField(0));
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

            // 12) impossible date (2023-02-30) is clamped to 2023-02-28 by the SMART
            // resolver, consistent with the other date functions in this class.
            // Expected value 1677542400 is 2023-02-28 00:00:00 under UTC (reset by
            // case 11); under Asia/Shanghai it would be 1677513600 (8h earlier).
            Assertions.assertEquals(
                    1677542400L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s) as ts from dual",
                                    strType,
                                    "2023-02-30 00:00:00")
                            .getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }
}
