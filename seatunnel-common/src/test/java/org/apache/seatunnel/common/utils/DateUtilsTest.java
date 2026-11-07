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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

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
    public void testParseIsoDateTimeWithFractionalSeconds() {
        // zero, three, six and nine fractional digits, with and without the optional 'Z'
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45", 0);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45Z", 0);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123", 123000000);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123Z", 123000000);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123456", 123456000);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123456Z", 123456000);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123456789", 123456789);
        assertIsoDateTimeWithFraction("2024-12-16T15:33:45.123456789Z", 123456789);
    }

    @Test
    public void testParseIsoLocalTimeWithFractionalSeconds() {
        assertIsoLocalTime("15:33:45", 0);
        assertIsoLocalTime("15:33:45.123", 123000000);
        assertIsoLocalTime("15:33:45.123456", 123456000);
        assertIsoLocalTime("15:33:45.123456789", 123456789);
    }

    @Test
    public void testRejectUnsupportedFractionalSeconds() {
        // Empty fractions and fractions longer than nine digits are unsupported.
        Assertions.assertNull(DateUtils.matchDateFormatter("2024-12-16T15:33:45."));
        Assertions.assertNull(DateUtils.matchDateFormatter("2024-12-16T15:33:45.1234567890"));
        // seconds are required and ',' is not accepted as the fraction separator
        Assertions.assertNull(DateUtils.matchDateFormatter("2024-12-16T15:33"));
        Assertions.assertNull(DateUtils.matchDateFormatter("2024-12-16T15:33:45,123"));
        // Out-of-range date and time fields are rejected even when the pattern matches.
        Assertions.assertThrows(
                DateTimeException.class, () -> DateUtils.parse("2024-13-45T25:99:99.123"));
    }

    private static void assertIsoDateTimeWithFraction(String dateTime, int expectedNano) {
        DateTimeFormatter formatter = DateUtils.matchDateFormatter(dateTime);
        Assertions.assertNotNull(formatter, dateTime);
        Assertions.assertEquals("2024-12-16", DateUtils.parse(dateTime).toString(), dateTime);
        Assertions.assertEquals(
                "2024-12-16", DateUtils.parse(dateTime, formatter).toString(), dateTime);

        TemporalAccessor parsed = formatter.parse(dateTime);
        Assertions.assertEquals(
                LocalDate.of(2024, 12, 16), parsed.query(TemporalQueries.localDate()), dateTime);
        Assertions.assertEquals(
                LocalTime.of(15, 33, 45, expectedNano),
                parsed.query(TemporalQueries.localTime()),
                dateTime);
    }

    private static void assertIsoLocalTime(String time, int expectedNano) {
        DateTimeFormatter formatter = DateUtils.matchDateFormatter(time);
        Assertions.assertNotNull(formatter, time);
        Assertions.assertEquals(
                LocalTime.of(15, 33, 45, expectedNano),
                formatter.parse(time).query(TemporalQueries.localTime()),
                time);
    }
}
