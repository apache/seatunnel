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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DateUtilsTest {

    @Test
    public void testAutoDateFormatter() {
        String datetimeStr = "2020-10-10";
        Assertions.assertEquals("2020-10-10", DateUtils.parse(datetimeStr).toString());

        datetimeStr = "2020年10月10日";
        Assertions.assertEquals("2020-10-10", DateUtils.parse(datetimeStr).toString());

        datetimeStr = "2020/10/10";
        Assertions.assertEquals("2020-10-10", DateUtils.parse(datetimeStr).toString());

        datetimeStr = "2020.10.10";
        Assertions.assertEquals("2020-10-10", DateUtils.parse(datetimeStr).toString());

        datetimeStr = "20201010";
        Assertions.assertEquals("2020-10-10", DateUtils.parse(datetimeStr).toString());
    }

    @Test
    public void testMatchDateTimeFormatter() {
        String datetimeStr = "2020-10-10";
        Assertions.assertEquals(
                "2020-10-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());

        datetimeStr = "2020年10月10日";
        Assertions.assertEquals(
                "2020-10-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());

        datetimeStr = "2020/10/10";
        Assertions.assertEquals(
                "2020-10-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());

        datetimeStr = "2020.10.10";
        Assertions.assertEquals(
                "2020-10-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());

        datetimeStr = "20201010";
        Assertions.assertEquals(
                "2020-10-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());
        datetimeStr = "2024/1/1";
        Assertions.assertEquals(
                "2024-01-01",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());
        datetimeStr = "2024/10/1";
        Assertions.assertEquals(
                "2024-10-01",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());
        datetimeStr = "2024/1/10";
        Assertions.assertEquals(
                "2024-01-10",
                DateUtils.parse(datetimeStr, DateUtils.matchDateFormatter(datetimeStr)).toString());
    }

    @Test
    public void testConvertDateTimeWithLocalTimeZone() {
        String datetimeStr = "2024-12-16T15:33:45";
        TemporalAccessor parsedTimestamp =
                DateUtils.matchDateFormatter(datetimeStr).parse(datetimeStr);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        LocalDateTime dateTime = LocalDateTime.of(localDate, localTime);
        Assertions.assertEquals("2024-12-16T15:33:45", dateTime.toString());
    }

    @Test
    public void testParseWithAutoFormat() {
        // Test auto-detecting date format
        LocalDate date1 = DateUtils.parse("2023-12-25");
        assertEquals(2023, date1.getYear());
        assertEquals(12, date1.getMonthValue());
        assertEquals(25, date1.getDayOfMonth());

        LocalDate date2 = DateUtils.parse("20231225");
        assertEquals(2023, date2.getYear());
        assertEquals(12, date2.getMonthValue());
        assertEquals(25, date2.getDayOfMonth());

        LocalDate date3 = DateUtils.parse("2023/12/25");
        assertEquals(2023, date3.getYear());
        assertEquals(12, date3.getMonthValue());
        assertEquals(25, date3.getDayOfMonth());

        LocalDate date4 = DateUtils.parse("2023年12月25日");
        assertEquals(2023, date4.getYear());
        assertEquals(12, date4.getMonthValue());
        assertEquals(25, date4.getDayOfMonth());

        LocalDate date5 = DateUtils.parse("2023.12.25");
        assertEquals(2023, date5.getYear());
        assertEquals(12, date5.getMonthValue());
        assertEquals(25, date5.getDayOfMonth());

        LocalDate date6 = DateUtils.parse("2023-1-5");
        assertEquals(2023, date6.getYear());
        assertEquals(1, date6.getMonthValue());
        assertEquals(5, date6.getDayOfMonth());
    }

    @Test
    public void testParseWithCustomFormat() {
        // Test parsing with custom format
        LocalDate date1 = DateUtils.parse("2023/12/25", "yyyy/MM/dd");
        assertEquals(2023, date1.getYear());
        assertEquals(12, date1.getMonthValue());
        assertEquals(25, date1.getDayOfMonth());

        LocalDate date2 = DateUtils.parse("2023.12.25", "yyyy.MM.dd");
        assertEquals(2023, date2.getYear());
        assertEquals(12, date2.getMonthValue());
        assertEquals(25, date2.getDayOfMonth());
    }

    @Test
    public void testParseWithFormatterEnum() {
        // Test parsing with Formatter enum
        LocalDate date1 = DateUtils.parse("2023-12-25", DateUtils.Formatter.YYYY_MM_DD);
        assertEquals(2023, date1.getYear());
        assertEquals(12, date1.getMonthValue());
        assertEquals(25, date1.getDayOfMonth());

        LocalDate date2 = DateUtils.parse("2023/12/25", DateUtils.Formatter.YYYY_MM_DD_SLASH);
        assertEquals(2023, date2.getYear());
        assertEquals(12, date2.getMonthValue());
        assertEquals(25, date2.getDayOfMonth());

        LocalDate date3 = DateUtils.parse("2023.12.25", DateUtils.Formatter.YYYY_MM_DD_SPOT);
        assertEquals(2023, date3.getYear());
        assertEquals(12, date3.getMonthValue());
        assertEquals(25, date3.getDayOfMonth());
    }

    @Test
    public void testToString() {
        LocalDate date = LocalDate.of(2023, 12, 25);

        // Test formatting with Formatter enum
        String formatted1 = DateUtils.toString(date, DateUtils.Formatter.YYYY_MM_DD);
        assertEquals("2023-12-25", formatted1);

        String formatted2 = DateUtils.toString(date, DateUtils.Formatter.YYYY_MM_DD_SLASH);
        assertEquals("2023/12/25", formatted2);

        String formatted3 = DateUtils.toString(date, DateUtils.Formatter.YYYY_MM_DD_SPOT);
        assertEquals("2023.12.25", formatted3);

        // Test formatting with custom format string
        String formatted4 = DateUtils.toString(date, "yyyy年MM月dd日");
        assertEquals("2023年12月25日", formatted4);
    }

    @Test
    public void testParseUnsupportedFormat() {
        // Test parsing with unsupported format
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    try {
                        DateUtils.parse("2023-12");
                    } catch (Exception e) {
                        assertEquals("Unsupported date format: 2023-12", e.getMessage());
                        throw e;
                    }
                });
    }
}
