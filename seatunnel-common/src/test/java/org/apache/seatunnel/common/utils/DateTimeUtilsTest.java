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
}
