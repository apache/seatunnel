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
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

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

        datetimeStr = "2020年10月10日 10时10分10秒";
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
}
