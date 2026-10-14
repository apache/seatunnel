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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class PlaceholderUtilsTest {

    @Test
    void testMultiplePlaceholdersWithDefault() {
        String input = "select * from ${resName:fake_test}_table where name = '${nameValForEnv}'";

        String result =
                PlaceholderUtils.processPlaceholders(
                        input,
                        key -> false,
                        new LinkedHashMap<String, String>(),
                        new LinkedHashMap<String, String>());
        assertEquals("select * from fake_test_table where name = '${nameValForEnv}'", result);
    }

    @Test
    void testNestedJsonPlaceholdersWithDefault() {
        String input =
                "${table_filter:{\"plugin_input\":\"mysql_source\",\"plugin_output\":\"table_filter\",\"include_fields\":[movie_id,unix_time]}}";

        String result =
                PlaceholderUtils.processPlaceholders(
                        input,
                        key -> false,
                        new LinkedHashMap<String, String>(),
                        new LinkedHashMap<String, String>());
        assertEquals(
                "{\"plugin_input\":\"mysql_source\",\"plugin_output\":\"table_filter\",\"include_fields\":[movie_id,unix_time]}",
                result);
    }

    @Test
    void testJsonWithPlaceholdersInsideNotSupported() {
        String input =
                "${table_list:[{\"table_path\":\"${mysql_db}.*_test\",\"use_regex\":\"${use_regex_flag}\"},{\"table_path\":\"${mysql_db}.tags_test2\"}]}";

        Map<String, String> userConfigMap = new LinkedHashMap<>();
        userConfigMap.put("mysql_db", "seatunnel_test");
        userConfigMap.put("use_regex_flag", "true");

        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, userConfigMap, new LinkedHashMap<String, String>());
        System.out.println(result);
        assertNotEquals(
                "[{\"table_path\":\"seatunnel_test.*_test\",\"use_regex\":\"true\"},{\"table_path\":\"seatunnel_test.tags_test2\"}]",
                result);
    }

    @Test
    void testMixedPlaceholdersWithArrayDefault() {
        String input = "${hosts:host}_${list:[a,b,c]}:${port:3306}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, Collections.emptyMap(), new LinkedHashMap<>());
        assertEquals("host_[a,b,c]:3306", result);
    }
}
