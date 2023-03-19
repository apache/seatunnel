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

public class StringFormatUtilsTest {
    @Test
    public void testStringFormat() {
        String s =
                StringFormatUtils.formatTable(
                        "Job Statistic Information",
                        "Start Time",
                        "2023-01-11 00:00:00",
                        "End Time",
                        "2023-01-11 00:00:00",
                        "Total Time(s)",
                        0,
                        "Total Read Count",
                        0,
                        "Total Write Count",
                        0,
                        "Total Failed Count",
                        0);
        Assertions.assertEquals(
                s,
                "\n"
                        + "***********************************************\n"
                        + "           Job Statistic Information\n"
                        + "***********************************************\n"
                        + "Start Time                : 2023-01-11 00:00:00\n"
                        + "End Time                  : 2023-01-11 00:00:00\n"
                        + "Total Time(s)             :                   0\n"
                        + "Total Read Count          :                   0\n"
                        + "Total Write Count         :                   0\n"
                        + "Total Failed Count        :                   0\n"
                        + "***********************************************\n");
    }
}
