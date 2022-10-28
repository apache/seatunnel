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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class VariablesSubstituteTest {

    @Test
    public void testSubstitute() {
        String timeFormat = "yyyyMMddHHmmss";
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String path = "data_${now}_${uuid}.parquet";
        String newPath = VariablesSubstitute.substitute(path, timeFormat);
        String now = newPath.substring(5, 19);
        LocalDateTime.parse(now, df);

        String text = "${var1} is a distributed, high-performance data integration platform for " +
                "the synchronization and ${var2} of massive data (offline & real-time).";

        HashMap<String, String> valuesMap = new HashMap<>();
        valuesMap.put("var1", "SeaTunnel");
        valuesMap.put("var2", "transformation");
        String newText = VariablesSubstitute.substitute(text, valuesMap);
        Assertions.assertTrue(newText.contains("SeaTunnel") && newText.contains("transformation"));
    }
}
