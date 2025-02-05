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
}
