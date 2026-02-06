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

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeUtilsTest {
    @Test
    public void testMatchTimeFormatter() {
        String timeStr = "12:12:12";
        Assertions.assertEquals(
                "12:12:12",
                TimeUtils.parse(timeStr, TimeUtils.matchTimeFormatter(timeStr)).toString());

        timeStr = "12:12:12.123";
        Assertions.assertEquals(
                "12:12:12.123",
                TimeUtils.parse(timeStr, TimeUtils.matchTimeFormatter(timeStr)).toString());
    }

    @Test
    public void testParseWithAutoFormat() {
        // Test auto-detecting time format
        LocalTime time1 = TimeUtils.parse("15:30:45");
        assertEquals(15, time1.getHour());
        assertEquals(30, time1.getMinute());
        assertEquals(45, time1.getSecond());

        LocalTime time2 = TimeUtils.parse("15:30:45.123");
        assertEquals(15, time2.getHour());
        assertEquals(30, time2.getMinute());
        assertEquals(45, time2.getSecond());
        assertEquals(123, time2.getNano() / 1000000);

        LocalTime time3 = TimeUtils.parse("15:30:45.123456");
        assertEquals(15, time3.getHour());
        assertEquals(30, time3.getMinute());
        assertEquals(45, time3.getSecond());
        assertEquals(123456, time3.getNano() / 1000);

        LocalTime time4 = TimeUtils.parse("15:30:45.123456789");
        assertEquals(15, time4.getHour());
        assertEquals(30, time4.getMinute());
        assertEquals(45, time4.getSecond());
        assertEquals(123456789, time4.getNano());

        LocalTime time5 = TimeUtils.parse("9:30:45");
        assertEquals(9, time5.getHour());
        assertEquals(30, time5.getMinute());
        assertEquals(45, time5.getSecond());

        LocalTime time6 = TimeUtils.parse("9:30:45.123");
        assertEquals(9, time6.getHour());
        assertEquals(30, time6.getMinute());
        assertEquals(45, time6.getSecond());
        assertEquals(123, time6.getNano() / 1000000);

        LocalTime time7 = TimeUtils.parse("9:30:45.123456");
        assertEquals(9, time7.getHour());
        assertEquals(30, time7.getMinute());
        assertEquals(45, time7.getSecond());
        assertEquals(123456, time7.getNano() / 1000);

        LocalTime time8 = TimeUtils.parse("9:30:45.123456789");
        assertEquals(9, time8.getHour());
        assertEquals(30, time8.getMinute());
        assertEquals(45, time8.getSecond());
        assertEquals(123456789, time8.getNano());
    }

    @Test
    public void testParseWithCustomFormat() {
        // Test parsing with custom format
        LocalTime time1 = TimeUtils.parse("15:30", "HH:mm");
        assertEquals(15, time1.getHour());
        assertEquals(30, time1.getMinute());
        assertEquals(0, time1.getSecond());

        LocalTime time2 = TimeUtils.parse("15:30:45.123456", "HH:mm:ss.SSSSSS");
        assertEquals(15, time2.getHour());
        assertEquals(30, time2.getMinute());
        assertEquals(45, time2.getSecond());
        assertEquals(123456000, time2.getNano());
    }

    @Test
    public void testParseWithFormatterEnum() {
        // Test parsing with Formatter enum
        LocalTime time1 = TimeUtils.parse("15:30:45", TimeUtils.Formatter.HH_MM_SS);
        assertEquals(15, time1.getHour());
        assertEquals(30, time1.getMinute());
        assertEquals(45, time1.getSecond());

        LocalTime time2 = TimeUtils.parse("15:30:45.123", TimeUtils.Formatter.HH_MM_SS_SSS);
        assertEquals(15, time2.getHour());
        assertEquals(30, time2.getMinute());
        assertEquals(45, time2.getSecond());
        assertEquals(123, time2.getNano() / 1000000);

        LocalTime time3 = TimeUtils.parse("9:30:45", TimeUtils.Formatter.H_MM_SS);
        assertEquals(9, time3.getHour());
        assertEquals(30, time3.getMinute());
        assertEquals(45, time3.getSecond());

        LocalTime time4 = TimeUtils.parse("9:30:45.123", TimeUtils.Formatter.H_MM_SS_SSS);
        assertEquals(9, time4.getHour());
        assertEquals(30, time4.getMinute());
        assertEquals(45, time4.getSecond());
        assertEquals(123, time4.getNano() / 1000000);
    }

    @Test
    public void testToString() {
        LocalTime time = LocalTime.of(15, 30, 45, 123000000);

        // Test formatting with Formatter enum
        String formatted1 = TimeUtils.toString(time, TimeUtils.Formatter.HH_MM_SS);
        assertEquals("15:30:45", formatted1);

        String formatted2 = TimeUtils.toString(time, TimeUtils.Formatter.HH_MM_SS_SSS);
        assertEquals("15:30:45.123", formatted2);

        String formatted3 = TimeUtils.toString(time, TimeUtils.Formatter.H_MM_SS);
        assertEquals("15:30:45", formatted3);

        String formatted4 = TimeUtils.toString(time, TimeUtils.Formatter.H_MM_SS_SSS);
        assertEquals("15:30:45.123", formatted4);

        // Test formatting with custom format string
        String formatted5 = TimeUtils.toString(time, "HH:mm");
        assertEquals("15:30", formatted5);
    }

    @Test
    public void testParseUnsupportedFormat() {
        // Test parsing with unsupported format
        IllegalArgumentException assertThrows =
                assertThrows(IllegalArgumentException.class, () -> TimeUtils.parse("15:301"));
        assertEquals("Unsupported time format: 15:301", assertThrows.getMessage());
    }
}
