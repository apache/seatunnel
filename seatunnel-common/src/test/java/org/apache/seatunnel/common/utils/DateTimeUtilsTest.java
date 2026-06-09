/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.utils.DateTimeUtils.Formatter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DateTimeUtilsTest {

    @Test
    public void testParseDateString() {
        final String datetime = "2023-12-22 00:00:00";
        LocalDateTime parse = DateTimeUtils.parse(datetime, Formatter.YYYY_MM_DD_HH_MM_SS);
        Assertions.assertEquals(0, parse.getMinute());
        Assertions.assertEquals(0, parse.getHour());
        Assertions.assertEquals(0, parse.getSecond());
        Assertions.assertEquals(22, parse.getDayOfMonth());
        Assertions.assertEquals(12, parse.getMonth().getValue());
        Assertions.assertEquals(2023, parse.getYear());
        Assertions.assertEquals(22, parse.getDayOfMonth());
    }

    @Test
    public void testConvertDateTimeWithLocalTimeZone() {
        String datetimeStr = "2024-12-16T15:33:45";
        TemporalAccessor parsedTimestamp =
                DateTimeUtils.matchDateTimeFormatter(datetimeStr).parse(datetimeStr);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        LocalDateTime dateTime = LocalDateTime.of(localDate, localTime);
        Assertions.assertEquals("2024-12-16T15:33:45", dateTime.toString());
    }

    @Test
    public void testParseTimestamp() {
        // 2023-12-22 12:55:20
        final long timestamp = 1703220920013L;
        LocalDateTime parse = DateTimeUtils.parse(timestamp, ZoneId.of("Asia/Shanghai"));

        Assertions.assertEquals(55, parse.getMinute());
        Assertions.assertEquals(12, parse.getHour());
        Assertions.assertEquals(20, parse.getSecond());
        Assertions.assertEquals(22, parse.getDayOfMonth());
        Assertions.assertEquals(12, parse.getMonth().getValue());
        Assertions.assertEquals(2023, parse.getYear());
        Assertions.assertEquals(22, parse.getDayOfMonth());
    }

    @Test
    public void testAutoDateTimeFormatter() {
        String datetimeStr = "2020-10-10 10:10:10";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020-10-10T10:10:10";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020/10/10 10:10:10";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020/1/1 10:10";
        Assertions.assertEquals("2020-01-01T10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2024/12/2 10:10";
        Assertions.assertEquals("2024-12-02T10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2024/12/1 10:10";
        Assertions.assertEquals("2024-12-01T10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020年10月10日10时10分10秒";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020.10.10 10:10:10";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "20201010101010";
        Assertions.assertEquals("2020-10-10T10:10:10", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020-10-10 10:10:10.201";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020-10-10 10:10:10.201111";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201111", DateTimeUtils.parse(datetimeStr).toString());

        datetimeStr = "2020-10-10 10:10:10.201111001";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201111001", DateTimeUtils.parse(datetimeStr).toString());
    }

    @Test
    public void testMatchDateTimeFormatter() {
        String datetimeStr = "2020-10-10 10:10:10";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020-10-10T10:10:10";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020/10/10 10:10:10";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020年10月10日 10时10分10秒";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020.10.10 10:10:10";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "20201010101010";
        Assertions.assertEquals(
                "2020-10-10T10:10:10",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020-10-10 10:10:10.201";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020-10-10 10:10:10.201111";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201111",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());

        datetimeStr = "2020-10-10 10:10:10.201111001";
        Assertions.assertEquals(
                "2020-10-10T10:10:10.201111001",
                DateTimeUtils.parse(datetimeStr, DateTimeUtils.matchDateTimeFormatter(datetimeStr))
                        .toString());
    }

    @Test
    @Disabled(
            "Performance testing has been split into 5 methods: "
                    + "testParsePerformanceAutoFormatPatternFirst, testParsePerformanceAutoFormatPatternFirstLast, "
                    + "testParsePerformanceFormatterEnum, testParsePerformanceCustomFormat, testToStringPerformance")
    public void testPerformance() {
        String datetimeStr = "2020-10-10 10:10:10";
        DateTimeFormatter dateTimeFormatter = DateTimeUtils.matchDateTimeFormatter(datetimeStr);
        String datetimeStr1 = "20201010101010";
        DateTimeFormatter dateTimeFormatter1 = DateTimeUtils.matchDateTimeFormatter(datetimeStr1);
        String datetimeStr2 = "2020.10.10 10:10:10.100";
        DateTimeFormatter dateTimeFormatter2 = DateTimeUtils.matchDateTimeFormatter(datetimeStr2);
        String datetimeStr3 = "2020.10.10 10:10:10";
        DateTimeFormatter dateTimeFormatter3 = DateTimeUtils.matchDateTimeFormatter(datetimeStr3);
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr, dateTimeFormatter);
        }
        long t2 = System.currentTimeMillis();
        // Use an explicit time format 'yyyy-MM-dd HH:mm:ss' for processing, use time: 4552ms
        System.out.println((t2 - t1) + "");

        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr);
        }
        long t3 = System.currentTimeMillis();
        // If format is not specified, the system automatically obtains the format 'yyyy-MM-dd
        // HH:mm:ss' for processing, use time: 6082ms
        System.out.println((t3 - t2) + "");

        long t4 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr1, dateTimeFormatter1);
        }
        long t5 = System.currentTimeMillis();
        // Use an explicit time format 'yyyyMMddHHmmss' for processing, use time: 4610ms
        System.out.println((t5 - t4) + "");

        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr1);
        }
        long t6 = System.currentTimeMillis();
        // If format is not specified, the system automatically obtains the format 'yyyyMMddHHmmss'
        // for processing, use time: 4842ms

        System.out.println((t6 - t5) + "");

        long t7 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr2, dateTimeFormatter2);
        }
        long t8 = System.currentTimeMillis();
        // Use an explicit time format 'yyyy.MM.dd HH:mm:ss.SSS' for processing, use time: 8162ms
        System.out.println((t8 - t7) + "");

        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr2);
        }
        long t9 = System.currentTimeMillis();
        // If format is not specified, the system automatically obtains the format 'yyyy.MM.dd
        // HH:mm:ss.SSS' for processing, use time: 11366ms
        System.out.println((t9 - t8) + "");

        long t10 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr3, dateTimeFormatter3);
        }
        long t11 = System.currentTimeMillis();
        // Use an explicit time format 'yyyy.MM.dd HH:mm:ss' for processing, use time: 4405ms
        System.out.println((t11 - t10) + "");

        for (int i = 0; i < 10000000; i++) {
            DateTimeUtils.parse(datetimeStr3);
        }
        long t12 = System.currentTimeMillis();
        // If format is not specified, the system automatically obtains the format 'yyyy.MM.dd
        // HH:mm:ss' for processing, use time: 7771ms
        System.out.println((t12 - t11) + "");
    }

    @Test
    public void testDateTimeFormat() {

        Assertions.assertEquals(
                "2024-01-01T09:30:45", DateTimeUtils.parse("2024/1/1 9:30:45").toString());

        Assertions.assertEquals(
                "2024-10-01T09:30:45", DateTimeUtils.parse("2024/10/1 9:30:45").toString());

        Assertions.assertEquals(
                "2024-10-10T09:30:45", DateTimeUtils.parse("2024/10/10 9:30:45").toString());

        Assertions.assertEquals(
                "2024-01-01T09:30:45", DateTimeUtils.parse("2024-1-1 9:30:45").toString());

        Assertions.assertEquals(
                "2024-10-01T09:30:45", DateTimeUtils.parse("2024-10-1 9:30:45").toString());

        Assertions.assertEquals(
                "2024-10-10T09:30:45", DateTimeUtils.parse("2024-10-10 9:30:45").toString());

        Assertions.assertEquals(
                "2024-10-10T09:30", DateTimeUtils.parse("2024-10-10 09:30").toString());
    }

    @Test
    public void testParseWithAutoFormat() {
        // Test auto-detecting date time format
        // 1. Basic formats
        LocalDateTime dateTime1 = DateTimeUtils.parse("2023-12-25 15:30:45");
        assertEquals(2023, dateTime1.getYear());
        assertEquals(12, dateTime1.getMonthValue());
        assertEquals(25, dateTime1.getDayOfMonth());
        assertEquals(15, dateTime1.getHour());
        assertEquals(30, dateTime1.getMinute());
        assertEquals(45, dateTime1.getSecond());

        // 2. No split format (14 digits)
        LocalDateTime dateTime2 = DateTimeUtils.parse("20231225153045");
        assertEquals(2023, dateTime2.getYear());
        assertEquals(12, dateTime2.getMonthValue());
        assertEquals(25, dateTime2.getDayOfMonth());
        assertEquals(15, dateTime2.getHour());
        assertEquals(30, dateTime2.getMinute());
        assertEquals(45, dateTime2.getSecond());

        // 3. Slash format
        LocalDateTime dateTime3 = DateTimeUtils.parse("2023/12/25 15:30:45");
        assertEquals(2023, dateTime3.getYear());
        assertEquals(12, dateTime3.getMonthValue());
        assertEquals(25, dateTime3.getDayOfMonth());
        assertEquals(15, dateTime3.getHour());
        assertEquals(30, dateTime3.getMinute());
        assertEquals(45, dateTime3.getSecond());

        // 4. Dot format
        LocalDateTime dateTime4 = DateTimeUtils.parse("2023.12.25 15:30:45");
        assertEquals(2023, dateTime4.getYear());
        assertEquals(12, dateTime4.getMonthValue());
        assertEquals(25, dateTime4.getDayOfMonth());
        assertEquals(15, dateTime4.getHour());
        assertEquals(30, dateTime4.getMinute());
        assertEquals(45, dateTime4.getSecond());

        // 5. ISO8601 format
        LocalDateTime dateTime5 = DateTimeUtils.parse("2023-12-25T15:30:45");
        assertEquals(2023, dateTime5.getYear());
        assertEquals(12, dateTime5.getMonthValue());
        assertEquals(25, dateTime5.getDayOfMonth());

        // 6. Single-digit month, day and hour (ISO8601 style  15 o'clock)
        LocalDateTime dateTime6 = DateTimeUtils.parse("2023-1-5 15:30:45");
        assertEquals(2023, dateTime6.getYear());
        assertEquals(1, dateTime6.getMonthValue());
        assertEquals(5, dateTime6.getDayOfMonth());
        assertEquals(15, dateTime6.getHour());
        assertEquals(30, dateTime6.getMinute());
        assertEquals(45, dateTime6.getSecond());

        // 7. Single-digit month, day and hour (slash style  15 o'clock)
        LocalDateTime dateTime7 = DateTimeUtils.parse("2023/1/5 15:30:45");
        assertEquals(2023, dateTime7.getYear());
        assertEquals(1, dateTime7.getMonthValue());
        assertEquals(5, dateTime7.getDayOfMonth());
        assertEquals(15, dateTime7.getHour());
        assertEquals(30, dateTime7.getMinute());
        assertEquals(45, dateTime7.getSecond());

        // 8. Single-digit month, day and hour (ISO8601 style 6 o'clock)
        LocalDateTime dateTime8 = DateTimeUtils.parse("2023-1-5 6:30:45");
        assertEquals(2023, dateTime8.getYear());
        assertEquals(1, dateTime8.getMonthValue());
        assertEquals(5, dateTime8.getDayOfMonth());
        assertEquals(6, dateTime8.getHour());
        assertEquals(30, dateTime8.getMinute());
        assertEquals(45, dateTime8.getSecond());

        // 9. Single-digit month, day and hour (slash style  6 o'clock)
        LocalDateTime dateTime9 = DateTimeUtils.parse("2023/1/5 6:30:45");
        assertEquals(2023, dateTime9.getYear());
        assertEquals(1, dateTime9.getMonthValue());
        assertEquals(5, dateTime9.getDayOfMonth());
        assertEquals(6, dateTime9.getHour());
        assertEquals(30, dateTime9.getMinute());
        assertEquals(45, dateTime9.getSecond());

        // 10. No seconds (ISO8601 style with single-digit)
        LocalDateTime dateTime10 = DateTimeUtils.parse("2023-1-5 15:30");
        assertEquals(2023, dateTime10.getYear());
        assertEquals(1, dateTime10.getMonthValue());
        assertEquals(5, dateTime10.getDayOfMonth());
        assertEquals(15, dateTime10.getHour());
        assertEquals(30, dateTime10.getMinute());
        assertEquals(0, dateTime10.getSecond());

        // 11. No seconds (ISO8601 style with double-digit)
        LocalDateTime dateTime11 = DateTimeUtils.parse("2023-12-25 15:30");
        assertEquals(2023, dateTime11.getYear());
        assertEquals(12, dateTime11.getMonthValue());
        assertEquals(25, dateTime11.getDayOfMonth());
        assertEquals(15, dateTime11.getHour());
        assertEquals(30, dateTime11.getMinute());
        assertEquals(0, dateTime11.getSecond());

        // 12. No seconds (slash style with single-digit)
        LocalDateTime dateTime12 = DateTimeUtils.parse("2023/1/5 15:30");
        assertEquals(2023, dateTime12.getYear());
        assertEquals(1, dateTime12.getMonthValue());
        assertEquals(5, dateTime12.getDayOfMonth());
        assertEquals(15, dateTime12.getHour());
        assertEquals(30, dateTime12.getMinute());
        assertEquals(0, dateTime12.getSecond());

        // 13. With milliseconds - dash format
        LocalDateTime dateTime13 = DateTimeUtils.parse("2023-12-25 15:30:45.123");
        assertEquals(2023, dateTime13.getYear());
        assertEquals(12, dateTime13.getMonthValue());
        assertEquals(25, dateTime13.getDayOfMonth());
        assertEquals(15, dateTime13.getHour());
        assertEquals(30, dateTime13.getMinute());
        assertEquals(45, dateTime13.getSecond());
        assertEquals(123000000, dateTime13.getNano());

        // 14. With milliseconds - ISO8601 format
        LocalDateTime dateTime14 = DateTimeUtils.parse("2023-12-25T15:30:45.123");
        assertEquals(2023, dateTime14.getYear());
        assertEquals(12, dateTime14.getMonthValue());
        assertEquals(25, dateTime14.getDayOfMonth());
        assertEquals(15, dateTime14.getHour());
        assertEquals(30, dateTime14.getMinute());
        assertEquals(45, dateTime14.getSecond());
        assertEquals(123000000, dateTime14.getNano());

        // 15. With milliseconds - slash format
        LocalDateTime dateTime15 = DateTimeUtils.parse("2023/12/25 15:30:45.123");
        assertEquals(2023, dateTime15.getYear());
        assertEquals(12, dateTime15.getMonthValue());
        assertEquals(25, dateTime15.getDayOfMonth());
        assertEquals(15, dateTime15.getHour());
        assertEquals(30, dateTime15.getMinute());
        assertEquals(45, dateTime15.getSecond());
        assertEquals(123000000, dateTime15.getNano());

        // 16. With milliseconds - dot format
        LocalDateTime dateTime16 = DateTimeUtils.parse("2023.12.25 15:30:45.123");
        assertEquals(2023, dateTime16.getYear());
        assertEquals(12, dateTime16.getMonthValue());
        assertEquals(25, dateTime16.getDayOfMonth());
        assertEquals(15, dateTime16.getHour());
        assertEquals(30, dateTime16.getMinute());
        assertEquals(45, dateTime16.getSecond());
        assertEquals(123000000, dateTime16.getNano());

        // 17. With microseconds
        LocalDateTime dateTime17 = DateTimeUtils.parse("2023-12-25 15:30:45.123456");
        assertEquals(2023, dateTime17.getYear());
        assertEquals(12, dateTime17.getMonthValue());
        assertEquals(25, dateTime17.getDayOfMonth());
        assertEquals(15, dateTime17.getHour());
        assertEquals(30, dateTime17.getMinute());
        assertEquals(45, dateTime17.getSecond());
        assertEquals(123456000, dateTime17.getNano());

        // 18. With nanoseconds
        LocalDateTime dateTime18 = DateTimeUtils.parse("2023-12-25 15:30:45.123456789");
        assertEquals(2023, dateTime18.getYear());
        assertEquals(12, dateTime18.getMonthValue());
        assertEquals(25, dateTime18.getDayOfMonth());
        assertEquals(15, dateTime18.getHour());
        assertEquals(30, dateTime18.getMinute());
        assertEquals(45, dateTime18.getSecond());
        assertEquals(123456789, dateTime18.getNano());

        // 19. T separator for date-time, 9-digit nanoseconds with UTC time zone Z identifier
        LocalDateTime dateTime19 = DateTimeUtils.parse("2023-12-25T15:30:45.123456789Z");
        assertEquals(2023, dateTime19.getYear());
        assertEquals(12, dateTime19.getMonthValue());
        assertEquals(25, dateTime19.getDayOfMonth());
        assertEquals(15, dateTime19.getHour());
        assertEquals(30, dateTime19.getMinute());
        assertEquals(45, dateTime19.getSecond());
        assertEquals(123456789, dateTime19.getNano());

        // 20. T separator for date-time, 9-digit nanoseconds with +08:00 time zone Z identifier
        LocalDateTime dateTime20 = DateTimeUtils.parse("2023/12/25 15:30:45.123456789+08:00");
        assertEquals(2023, dateTime20.getYear());
        assertEquals(12, dateTime20.getMonthValue());
        assertEquals(25, dateTime20.getDayOfMonth());
        assertEquals(15, dateTime20.getHour());
        assertEquals(30, dateTime20.getMinute());
        assertEquals(45, dateTime20.getSecond());
        assertEquals(123456789, dateTime20.getNano());
    }

    @Test
    public void testParseWithCustomFormat() {
        // Test parsing with custom format
        LocalDateTime dateTime1 =
                DateTimeUtils.parse("2023-12-25T15:30:45", "yyyy-MM-dd'T'HH:mm:ss");
        assertEquals(2023, dateTime1.getYear());
        assertEquals(12, dateTime1.getMonthValue());
        assertEquals(25, dateTime1.getDayOfMonth());
        assertEquals(15, dateTime1.getHour());
        assertEquals(30, dateTime1.getMinute());
        assertEquals(45, dateTime1.getSecond());

        LocalDateTime dateTime2 =
                DateTimeUtils.parse("2023年12月25日15时30分45秒", "yyyy年MM月dd日HH时mm分ss秒");
        assertEquals(2023, dateTime2.getYear());
        assertEquals(12, dateTime2.getMonthValue());
        assertEquals(25, dateTime2.getDayOfMonth());
        assertEquals(15, dateTime1.getHour());
        assertEquals(30, dateTime1.getMinute());
        assertEquals(45, dateTime1.getSecond());
    }

    @Test
    public void testChineseDateFormat() {
        LocalDateTime dateTime1 = DateTimeUtils.parse("2023年12月25日15时30分45秒");
        assertEquals(2023, dateTime1.getYear());
        assertEquals(12, dateTime1.getMonthValue());
        assertEquals(25, dateTime1.getDayOfMonth());
        assertEquals(15, dateTime1.getHour());
        assertEquals(30, dateTime1.getMinute());
        assertEquals(45, dateTime1.getSecond());

        LocalDateTime dateTime2 = DateTimeUtils.parse("2023年1月2日1时3分4秒");
        assertEquals(2023, dateTime2.getYear());
        assertEquals(1, dateTime2.getMonthValue());
        assertEquals(2, dateTime2.getDayOfMonth());
        assertEquals(1, dateTime2.getHour());
        assertEquals(3, dateTime2.getMinute());
        assertEquals(4, dateTime2.getSecond());

        LocalDateTime dateTime3 = DateTimeUtils.parse("2023年12月2日1时3分4秒");
        assertEquals(2023, dateTime3.getYear());
        assertEquals(12, dateTime3.getMonthValue());
        assertEquals(2, dateTime3.getDayOfMonth());
        assertEquals(1, dateTime3.getHour());
        assertEquals(3, dateTime3.getMinute());
        assertEquals(4, dateTime3.getSecond());

        LocalDateTime dateTime4 = DateTimeUtils.parse("2023年12月21日1时3分4秒");
        assertEquals(2023, dateTime4.getYear());
        assertEquals(12, dateTime4.getMonthValue());
        assertEquals(21, dateTime4.getDayOfMonth());
        assertEquals(1, dateTime4.getHour());
        assertEquals(3, dateTime4.getMinute());
        assertEquals(4, dateTime4.getSecond());

        LocalDateTime dateTime5 = DateTimeUtils.parse("2023年12月21日17时3分4秒");
        assertEquals(2023, dateTime5.getYear());
        assertEquals(12, dateTime5.getMonthValue());
        assertEquals(21, dateTime5.getDayOfMonth());
        assertEquals(17, dateTime5.getHour());
        assertEquals(3, dateTime5.getMinute());
        assertEquals(4, dateTime5.getSecond());

        LocalDateTime dateTime6 = DateTimeUtils.parse("2023年12月21日17时31分4秒");
        assertEquals(2023, dateTime6.getYear());
        assertEquals(12, dateTime6.getMonthValue());
        assertEquals(21, dateTime6.getDayOfMonth());
        assertEquals(17, dateTime6.getHour());
        assertEquals(31, dateTime6.getMinute());
        assertEquals(4, dateTime6.getSecond());
    }

    @Test
    public void testParseWithFormatterEnum() {
        // Test parsing with Formatter enum
        LocalDateTime dateTime1 =
                DateTimeUtils.parse(
                        "2023-12-25 15:30:45", DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        assertEquals(2023, dateTime1.getYear());
        assertEquals(12, dateTime1.getMonthValue());
        assertEquals(25, dateTime1.getDayOfMonth());
        assertEquals(15, dateTime1.getHour());
        assertEquals(30, dateTime1.getMinute());
        assertEquals(45, dateTime1.getSecond());
    }

    @Test
    public void testReverseFormatter() {
        LocalDateTime dateTime1 = DateTimeUtils.parse("1/2/2026 12:01:30");
        assertEquals(2026, dateTime1.getYear());
        assertEquals(1, dateTime1.getMonthValue());
        assertEquals(2, dateTime1.getDayOfMonth());
        assertEquals(12, dateTime1.getHour());
        assertEquals(1, dateTime1.getMinute());
        assertEquals(30, dateTime1.getSecond());

        LocalDateTime dateTime2 = DateTimeUtils.parse("12/2/2026 12:01:30");
        assertEquals(2026, dateTime2.getYear());
        assertEquals(12, dateTime2.getMonthValue());
        assertEquals(2, dateTime2.getDayOfMonth());
        assertEquals(12, dateTime2.getHour());
        assertEquals(1, dateTime2.getMinute());
        assertEquals(30, dateTime2.getSecond());

        LocalDateTime dateTime3 = DateTimeUtils.parse("1/2/2026 1:2:3");
        assertEquals(2026, dateTime3.getYear());
        assertEquals(1, dateTime3.getMonthValue());
        assertEquals(2, dateTime3.getDayOfMonth());
        assertEquals(1, dateTime3.getHour());
        assertEquals(2, dateTime3.getMinute());
        assertEquals(3, dateTime3.getSecond());

        LocalDateTime dateTime4 = DateTimeUtils.parse("01/02/2026 01:02:03");
        assertEquals(2026, dateTime4.getYear());
        assertEquals(1, dateTime4.getMonthValue());
        assertEquals(2, dateTime4.getDayOfMonth());
        assertEquals(1, dateTime4.getHour());
        assertEquals(2, dateTime4.getMinute());
        assertEquals(3, dateTime4.getSecond());

        // T separator should also work
        LocalDateTime dateTime5 = DateTimeUtils.parse("1/2/2026T12:01:30");
        assertEquals(2026, dateTime5.getYear());
        assertEquals(1, dateTime5.getMonthValue());
        assertEquals(2, dateTime5.getDayOfMonth());
        assertEquals(12, dateTime5.getHour());
        assertEquals(1, dateTime5.getMinute());
        assertEquals(30, dateTime5.getSecond());
    }

    @Test
    void testDateWithMillisecondFormat() {
        LocalDateTime dateTime1 = DateTimeUtils.parse("2026-03-11T00:23:47.1");
        assertEquals(2026, dateTime1.getYear());
        assertEquals(3, dateTime1.getMonthValue());
        assertEquals(11, dateTime1.getDayOfMonth());
        assertEquals(0, dateTime1.getHour());
        assertEquals(23, dateTime1.getMinute());
        assertEquals(47, dateTime1.getSecond());
        assertEquals(100000000, dateTime1.getNano());
        LocalDateTime dateTime2 = DateTimeUtils.parse("2026-03-11T00:23:47.12");
        assertEquals(120000000, dateTime2.getNano());
        LocalDateTime dateTime3 = DateTimeUtils.parse("2026-03-11T00:23:47.123");
        assertEquals(123000000, dateTime3.getNano());
    }

    @Test
    public void testToString() {
        LocalDateTime dateTime = LocalDateTime.of(2023, 12, 25, 15, 30, 45);

        // Test formatting with Formatter enum
        String formatted =
                DateTimeUtils.toString(dateTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        assertEquals("2023-12-25 15:30:45", formatted);

        // Test formatting with custom format string
        String formatted2 = DateTimeUtils.toString(dateTime, "yyyy/MM/dd HH:mm:ss");
        assertEquals("2023/12/25 15:30:45", formatted2);
    }

    @Test
    public void testParseUnsupportedFormat() {
        // Test parsing with unsupported format
        IllegalArgumentException assertThrows =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> DateTimeUtils.parse("2023/12/251 15:30:45"));
        assertEquals(
                "Unsupported datetime format: 2023/12/251 15:30:45", assertThrows.getMessage());
    }

    @Test
    public void testParsePerformanceAutoFormatNormalPattern() {
        final int iterations = 10000000;
        String dateTimeStr = "2023-12-25 15:30:45";
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf(
                "Auto-format[%s] parsing: %d iterations in %d ms%n",
                dateTimeStr, iterations, durationMs);
        assertTrue(durationMs > 0);
    }

    @Test
    public void testParsePerformanceAutoFormatShortPattern() {
        final int iterations = 10000000;
        String dateTimeStr = "2023/1/25 8:30:45";
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf(
                "Auto-format[%s] parsing: %d iterations in %d ms%n",
                dateTimeStr, iterations, durationMs);
        assertTrue(durationMs > 0);
    }

    @Test
    public void testParsePerformanceAutoFormatCNPattern() {
        final int iterations = 10000000;
        String dateTimeStr = "2023年12月25日15时30分45秒";
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.parse(dateTimeStr);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf(
                "Auto-format[%s] parsing: %d iterations in %d ms%n",
                dateTimeStr, iterations, durationMs);
        assertTrue(durationMs > 0);
    }

    @Test
    public void testParsePerformanceFormatterEnum() {
        final int iterations = 10000000;
        String dateTimeStr = "2023-12-25 15:30:45";
        DateTimeUtils.Formatter formatter = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.parse(dateTimeStr, formatter);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.parse(dateTimeStr, formatter);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf(
                "Auto-format-enum parsing: %d iterations in %d ms%n", iterations, durationMs);
        assertTrue(durationMs > 0);
    }

    @Test
    public void testParsePerformanceCustomFormat() {
        final int iterations = 10000000;
        String dateTimeStr = "2023-12-25 15:30:45";
        String formatStr = "yyyy-MM-dd HH:mm:ss";
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.parse(dateTimeStr, formatStr);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.parse(dateTimeStr, formatStr);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf(
                "Auto-format-custom parsing: %d iterations in %d ms%n", iterations, durationMs);
        assertTrue(durationMs > 0);
    }

    @Test
    public void testToStringPerformance() {
        final int iterations = 10000000;
        LocalDateTime dateTime = LocalDateTime.of(2023, 12, 25, 15, 30, 45);
        DateTimeUtils.Formatter formatter = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        for (int i = 0; i < iterations / 1000; i++) {
            DateTimeUtils.toString(dateTime, formatter);
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            DateTimeUtils.toString(dateTime, formatter);
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.printf("ToString performance: %d iterations in %d ms%n", iterations, durationMs);
        assertTrue(durationMs > 0);
    }
}
