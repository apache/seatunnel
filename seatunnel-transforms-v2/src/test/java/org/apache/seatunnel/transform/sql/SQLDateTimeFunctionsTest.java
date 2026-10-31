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
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    public void testUnixTimestampNoArg() {
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
    }

    @Test
    public void testUnixTimestampStringPatterns() {
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

            // default pattern yyyy-MM-dd HH:mm:ss
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s) as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00")
                            .getField(0));

            // custom pattern
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'yyyy/MM/dd HH:mm:ss') as ts from dual",
                                    strType,
                                    "2023/01/01 00:00:00")
                            .getField(0));

            // date-only pattern -> midnight of that day
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'yyyy-MM-dd') as ts from dual",
                                    strType,
                                    "2023-01-01")
                            .getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampNullAndInvalid() {
        SeaTunnelRowType strType =
                new SeaTunnelRowType(
                        new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        // null input -> null
        Assertions.assertNull(
                runSql("select UNIX_TIMESTAMP(s) as ts from dual", strType, new Object[] {null})
                        .getField(0));

        // unparseable string -> null (does not raise on bad input)
        Assertions.assertNull(
                runSql("select UNIX_TIMESTAMP(s) as ts from dual", strType, "not-a-date")
                        .getField(0));

        // explicit NULL pattern -> null (propagates, does not fall back to default)
        Assertions.assertNull(
                runSql(
                                "select UNIX_TIMESTAMP(s, null) as ts from dual",
                                strType,
                                "2023-01-01 00:00:00")
                        .getField(0));

        // numeric input is not supported -> null
        SeaTunnelRowType longType =
                new SeaTunnelRowType(
                        new String[] {"n"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
        Assertions.assertNull(
                runSql("select UNIX_TIMESTAMP(n) as ts from dual", longType, 1672531200L)
                        .getField(0));
    }

    @Test
    public void testUnixTimestampTypedInputs() {
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            // typed LocalDateTime argument
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

            // typed OffsetDateTime argument (offset-aware, TZ-independent)
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

            // typed LocalDate argument (DATE column, resolved at midnight)
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
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampNonUtcZone() {
        // timezone contract: the string overload resolves against the JVM default
        // zone, not a hardcoded UTC. Under Asia/Shanghai (UTC+8), 2023-01-01 00:00:00
        // maps to 1672502400 (28800s earlier than UTC's 1672531200).
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
            Assertions.assertEquals(
                    1672502400L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s) as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00")
                            .getField(0));

            // OffsetDateTime is offset-aware, so its epoch is zone-independent.
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
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampDateClamping() {
        // impossible date (2023-02-30) is clamped to 2023-02-28 by the SMART
        // resolver, consistent with the other date functions in this class.
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
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

    @Test
    public void testFromUnixTimeUnixTimestampRoundTrip() {
        // Unit-level round trip: FROM_UNIXTIME(UNIX_TIMESTAMP(s)) under a non-UTC
        // zone. Both functions use the same default zone, so the round trip holds.
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
            SeaTunnelRow outRow =
                    runSql(
                            "select FROM_UNIXTIME(UNIX_TIMESTAMP(s), 'yyyy-MM-dd HH:mm:ss') as rt from dual",
                            strType,
                            "2023-01-01 00:00:00");
            Assertions.assertEquals("2023-01-01 00:00:00", outRow.getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampInvalidPattern() {
        // An invalid pattern returns NULL rather than raising, so a bad pattern
        // value (including one sourced from a row column) does not fail the query.
        SeaTunnelRowType strType =
                new SeaTunnelRowType(
                        new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        Assertions.assertNull(
                runSql(
                                "select UNIX_TIMESTAMP(s, 'yyyy-MM-dd HHH:mm:ss') as ts from dual",
                                strType,
                                "2023-01-01 00:00:00")
                        .getField(0));
    }

    @Test
    public void testUnixTimestampOffsetString() {
        // A string whose pattern carries a zone offset uses that offset rather
        // than the JVM default zone. Under Asia/Shanghai (UTC+8), the string
        // '2023-01-01 00:00:00+00:00' with pattern 'yyyy-MM-dd HH:mm:ssXXX'
        // denotes 2023-01-01 00:00:00 UTC = epoch 1672531200. If the offset
        // were discarded, the result would be 1672502400 (Shanghai midnight).
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
            Assertions.assertEquals(
                    1672531200L,
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'yyyy-MM-dd HH:mm:ssXXX') as ts from dual",
                                    strType,
                                    "2023-01-01 00:00:00+00:00")
                            .getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampNullPatternTypedInput() {
        // An explicit NULL pattern returns NULL for all input types, not only
        // for string input. UNIX_TIMESTAMP(dt, null) where dt is a typed
        // LocalDateTime returns NULL rather than ignoring the pattern.
        SeaTunnelRowType dtType =
                new SeaTunnelRowType(
                        new String[] {"dt"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        Assertions.assertNull(
                runSql(
                                "select UNIX_TIMESTAMP(dt, null) as ts from dual",
                                dtType,
                                LocalDateTime.of(2023, 1, 1, 0, 0, 0))
                        .getField(0));
    }

    @Test
    public void testUnixTimestampUnsupportedType() {
        // Non-temporal, non-string types raise a TransformException so a
        // schema/type mistake is not silent. A TIME column (LocalTime) has no
        // date component, so converting it to epoch seconds is meaningless.
        SeaTunnelRowType timeType =
                new SeaTunnelRowType(
                        new String[] {"t"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_TIME_TYPE});
        Assertions.assertThrows(
                TransformException.class,
                () ->
                        runSql(
                                "select UNIX_TIMESTAMP(t) as ts from dual",
                                timeType,
                                LocalTime.of(0, 0, 0)));
    }

    @Test
    public void testUnixTimestampAtTimeZoneExample() {
        // Pins the documented example:
        //   select UNIX_TIMESTAMP(dt AT TIME ZONE 'UTC+8') as ts
        // (docs/{en,zh}/transforms/sql-functions.md). Without a time-zone
        // argument on UNIX_TIMESTAMP, AT TIME ZONE is the supported way to
        // convert a TIMESTAMP in a non-default zone. Under Asia/Shanghai
        // (UTC+8), LocalDateTime 2023-01-01 00:00:00 is rebased to
        // OffsetDateTime 2023-01-01 00:00:00+08:00, whose epoch is
        // 2022-12-31 16:00:00 UTC = 1672502400. If the AT TIME ZONE argument
        // were ever ignored, the result would instead be 1672531200 (Shanghai
        // midnight 2023-01-01).
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            SeaTunnelRowType dtType =
                    new SeaTunnelRowType(
                            new String[] {"dt"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
            Assertions.assertEquals(
                    1672502400L,
                    runSql(
                                    "select UNIX_TIMESTAMP(dt AT TIME ZONE 'UTC+8') as ts from dual",
                                    dtType,
                                    LocalDateTime.of(2023, 1, 1, 0, 0, 0))
                            .getField(0));
        } finally {
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testUnixTimestampTextPatternLocaleIndependent() {
        // getFormatter previously used DateTimeFormatter.ofPattern(pattern), which
        // captures the JVM default FORMAT locale. A text pattern like 'dd MMM yyyy'
        // then failed to parse on a non-English worker (de-DE expects "Okt", not
        // "Oct") and the catch block returned NULL. With Locale.ENGLISH in
        // getFormatter, parsing is locale-independent. Under Locale.GERMANY this
        // test fails without the fix.
        Locale originalFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY);
            SeaTunnelRowType strType =
                    new SeaTunnelRowType(
                            new String[] {"s"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
            Object field =
                    runSql(
                                    "select UNIX_TIMESTAMP(s, 'dd MMM yyyy') as ts from dual",
                                    strType,
                                    "23 Oct 2026")
                            .getField(0);
            Assertions.assertNotNull(
                    field, "text pattern must not return NULL under a non-English locale");
            // 2026-10-23 00:00:00+08:00 (Asia/Shanghai) = 2026-10-22 16:00:00 UTC
            Assertions.assertEquals(1792684800L, field);
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalFormatLocale);
            TimeZone.setDefault(originalTz);
        }
    }

    @Test
    public void testFromUnixTimeTextPatternLocaleIndependent() {
        // FROM_UNIXTIME rendered text patterns in the worker's locale (de-DE ->
        // "Okt"), inconsistent with DAYNAME/MONTHNAME which pin English. With
        // Locale.ENGLISH in getFormatter, formatting is locale-independent. Under
        // Locale.GERMANY this test fails without the fix.
        Locale originalFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
        TimeZone originalTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        try {
            Locale.setDefault(Locale.Category.FORMAT, Locale.GERMANY);
            long epoch = LocalDate.of(2026, 10, 23).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
            SeaTunnelRowType numType =
                    new SeaTunnelRowType(
                            new String[] {"n"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});
            Object field =
                    runSql("select FROM_UNIXTIME(n, 'dd MMM yyyy') as d from dual", numType, epoch)
                            .getField(0);
            Assertions.assertNotNull(field);
            Assertions.assertTrue(
                    ((String) field).contains("Oct"),
                    "FROM_UNIXTIME text pattern must render English month names, got: " + field);
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalFormatLocale);
            TimeZone.setDefault(originalTz);
        }
    }
}
