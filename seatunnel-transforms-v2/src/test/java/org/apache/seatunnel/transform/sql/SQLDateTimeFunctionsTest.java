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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

    /**
     * Locale.setDefault(Locale) also overwrites the FORMAT and DISPLAY category defaults, so
     * restoring only Locale.getDefault() would silently drop a category value that differed from
     * the base locale before the test ran. Snapshot all three and put all three back.
     *
     * @return the base, FORMAT and DISPLAY defaults, in that order
     */
    private static Locale[] snapshotDefaultLocales() {
        return new Locale[] {
            Locale.getDefault(),
            Locale.getDefault(Locale.Category.FORMAT),
            Locale.getDefault(Locale.Category.DISPLAY)
        };
    }

    /** Restores the base locale first, because setting it overwrites both category defaults. */
    private static void restoreDefaultLocales(Locale[] saved) {
        Locale.setDefault(saved[0]);
        Locale.setDefault(Locale.Category.FORMAT, saved[1]);
        Locale.setDefault(Locale.Category.DISPLAY, saved[2]);
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

    /**
     * The pattern reaches DateTimeFormatter straight from the user's SQL, so any text bearing
     * letter in it used to be rendered in the default locale of whichever worker ran the transform.
     * DAYNAME and MONTHNAME never had that problem, because DateTimeFunction pins Locale.ENGLISH
     * for them, so the two disagreed on the same row. These locales are chosen for that: Turkish,
     * Japanese and German each translate at least one of the ten text bearing patterns covered here
     * (G, MMM, MMMM, LLLL, EEE, EEEE, ccc, a, QQQ, QQQQ). Measured on an unpinned formatter, every
     * one of the ten differs from English in at least one of those three locales, including the era
     * G and the am/pm marker a, not only the month and weekday names.
     *
     * <p>The function names here are deliberately upper case. Lower case names would not reach
     * these functions at all under a Turkish default locale, because the engine's keyword lookup
     * upper cases them without a locale and turns the dotless i into a dotted one. That is a
     * separate defect; keeping the names upper case isolates this test to the formatter locale.
     */
    @Test
    public void testFormatDateTimeTextIsEnglishUnderAnyDefaultLocale() {
        Locale[] original = snapshotDefaultLocales();
        try {
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"dt"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
            LocalDateTime dt = LocalDateTime.of(2024, 6, 15, 14, 30, 45);

            for (Locale locale :
                    new Locale[] {
                        Locale.ENGLISH,
                        Locale.forLanguageTag("tr-TR"),
                        Locale.forLanguageTag("ja-JP"),
                        Locale.forLanguageTag("de-DE")
                    }) {
                Locale.setDefault(locale);
                String tag = locale.toLanguageTag();

                SeaTunnelRow outRow =
                        runSql(
                                "select FORMATDATETIME(dt, 'dd MMM yyyy') as a,"
                                        + " FORMATDATETIME(dt, 'EEEE') as b,"
                                        + " FORMATDATETIME(dt, 'MMMM') as c,"
                                        + " DAYNAME(dt) as d,"
                                        + " MONTHNAME(dt) as e,"
                                        + " FORMATDATETIME(dt, 'yyyy-MM-dd') as f,"
                                        + " FORMATDATETIME(dt, 'G') as g,"
                                        + " FORMATDATETIME(dt, 'EEE') as h,"
                                        + " FORMATDATETIME(dt, 'ccc') as i,"
                                        + " FORMATDATETIME(dt, 'a') as j,"
                                        + " FORMATDATETIME(dt, 'QQQ') as k,"
                                        + " FORMATDATETIME(dt, 'QQQQ') as m,"
                                        + " FORMATDATETIME(dt, 'LLLL') as n from dual",
                                rowType,
                                dt);

                Assertions.assertEquals("15 Jun 2024", outRow.getField(0), tag);
                Assertions.assertEquals("Saturday", outRow.getField(1), tag);
                Assertions.assertEquals("June", outRow.getField(2), tag);
                // the whole point: the two ways of naming a day must not disagree
                Assertions.assertEquals(outRow.getField(3), outRow.getField(1), tag);
                Assertions.assertEquals(outRow.getField(4), outRow.getField(2), tag);
                // control: a numeric only pattern was never locale dependent
                Assertions.assertEquals("2024-06-15", outRow.getField(5), tag);
                // the rest of the text bearing letters, so the guarantee covers all of them and
                // not only the month and weekday names that prompted the report
                Assertions.assertEquals("AD", outRow.getField(6), tag);
                Assertions.assertEquals("Sat", outRow.getField(7), tag);
                Assertions.assertEquals("Sat", outRow.getField(8), tag);
                Assertions.assertEquals("PM", outRow.getField(9), tag);
                Assertions.assertEquals("Q2", outRow.getField(10), tag);
                Assertions.assertEquals("2nd quarter", outRow.getField(11), tag);
                Assertions.assertEquals("June", outRow.getField(12), tag);
            }
        } finally {
            restoreDefaultLocales(original);
        }
    }

    /** FROM_UNIXTIME builds its formatter the same way, so it needs the same guarantee. */
    @Test
    public void testFromUnixTimeTextIsEnglishUnderAnyDefaultLocale() {
        Locale[] original = snapshotDefaultLocales();
        try {
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"ts"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE});

            for (Locale locale :
                    new Locale[] {
                        Locale.ENGLISH,
                        Locale.forLanguageTag("tr-TR"),
                        Locale.forLanguageTag("ja-JP")
                    }) {
                Locale.setDefault(locale);
                String tag = locale.toLanguageTag();

                // 1672545600 = 2023-01-01 10:00:00 at UTC+6, a Sunday
                SeaTunnelRow outRow =
                        runSql(
                                "select FROM_UNIXTIME(ts, 'dd MMM yyyy', 'UTC+6') as a,"
                                        + " FROM_UNIXTIME(ts, 'EEEE', 'UTC+6') as b,"
                                        + " FROM_UNIXTIME(ts, 'yyyy-MM-dd', 'UTC+6') as c"
                                        + " from dual",
                                rowType,
                                1672545600L);

                Assertions.assertEquals("01 Jan 2023", outRow.getField(0), tag);
                Assertions.assertEquals("Sunday", outRow.getField(1), tag);
                Assertions.assertEquals("2023-01-01", outRow.getField(2), tag);
            }
        } finally {
            restoreDefaultLocales(original);
        }
    }

    /**
     * The formatter locale does not only choose names, it also chooses the week rules, because
     * DateTimeFormatter resolves the localized fields Y (week based year), w, W, e and c through
     * WeekFields.of(locale). Pinning Locale.ENGLISH therefore pins the first day of the week to
     * Sunday and the minimal days in the first week to 1 on every worker. These patterns produce
     * digits, so a numeric result is not by itself evidence that a pattern is locale independent.
     *
     * <p>1 January 2021 is a Friday, which makes the difference visible: under en-GB and de-DE
     * (Monday, 4) that day belongs to week 53 of 2020, while under Locale.ENGLISH (Sunday, 1) it is
     * week 1 of 2021. en-GB is included deliberately, because an English language default is not
     * interchangeable with Locale.ENGLISH for these fields.
     */
    @Test
    public void testFormatDateTimeWeekFieldsUseEnglishWeekRulesUnderAnyDefaultLocale() {
        Locale[] original = snapshotDefaultLocales();
        try {
            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"dt"},
                            new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
            LocalDateTime dt = LocalDateTime.of(2021, 1, 1, 10, 0, 0);

            for (Locale locale :
                    new Locale[] {
                        Locale.ENGLISH,
                        Locale.UK,
                        Locale.forLanguageTag("de-DE"),
                        Locale.forLanguageTag("tr-TR")
                    }) {
                Locale.setDefault(locale);
                String tag = locale.toLanguageTag();

                SeaTunnelRow outRow =
                        runSql(
                                "select FORMATDATETIME(dt, 'YYYY') as a,"
                                        + " FORMATDATETIME(dt, 'ww') as b,"
                                        + " FORMATDATETIME(dt, 'W') as c,"
                                        + " FORMATDATETIME(dt, 'e') as d,"
                                        + " FORMATDATETIME(dt, 'D') as f from dual",
                                rowType,
                                dt);

                Assertions.assertEquals("2021", outRow.getField(0), tag);
                Assertions.assertEquals("01", outRow.getField(1), tag);
                Assertions.assertEquals("1", outRow.getField(2), tag);
                Assertions.assertEquals("6", outRow.getField(3), tag);
                // control: day of year is not a localized field, it never followed the locale
                Assertions.assertEquals("1", outRow.getField(4), tag);
            }
        } finally {
            restoreDefaultLocales(original);
        }
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
}
