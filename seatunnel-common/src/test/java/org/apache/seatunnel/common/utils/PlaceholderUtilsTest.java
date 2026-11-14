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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        String defaultValueString =
                "[{\"table_path\":\"${mysql_db}.*_test\",\"use_regex\":\"${use_regex_flag}\"},{\"table_path\":\"${mysql_db}.tags_test2\"}]";

        String input = String.format("${table_list:%s}", defaultValueString);

        Map<String, String> userConfigMap = new LinkedHashMap<>();
        userConfigMap.put("mysql_db", "seatunnel_test");
        userConfigMap.put("use_regex_flag", "true");

        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, userConfigMap, new LinkedHashMap<String, String>());

        String expected = defaultValueString;
        for (String key : userConfigMap.keySet()) {
            expected = expected.replace("${" + key + "}", userConfigMap.get(key));
        }

        assertNotEquals(expected, result);
        assertEquals(defaultValueString, result);
    }

    @Test
    void testMixedPlaceholdersWithArrayDefault() {
        String input = "${hosts:host}_${list:[a,b,c]}:${port:3306}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, Collections.emptyMap(), new LinkedHashMap<>());
        assertEquals("host_[a,b,c]:3306", result);
    }

    @Test
    void testPlaceholderWithEscapedQuotes() {
        // config file content: a="""${msg:\"5 screen\"}""";
        String input = "${msg:5\\\" screen}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>());

        assertEquals("5\\\" screen", result);
    }

    @Test
    void testPlaceholderWithQuotesWrapped() {
        // config file content: a="""${msg:"5 screen"}""";
        String input = "${msg:\"5 screen\"}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>());

        assertEquals("\"5 screen\"", result);
    }

    @Test
    void testMissingClosingBraceThrows() {
        String input = "${key:default";
        assertThrows(
                ConfigException.BadValue.class,
                () ->
                        PlaceholderUtils.processPlaceholders(
                                input, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>()));
    }

    @Test
    void testDefaultListString() {
        String input = "${key:[0-9]}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>());

        assertEquals("[0-9]", result);
    }

    @Test
    void testDefaultRegexString() {
        // config file content pattern="""${key:"[0-9]"}"""
        // in order to distinguash list,string must be inside quotes
        String input = "${key:\"[0-9]\"}";
        String result =
                PlaceholderUtils.processPlaceholders(
                        input, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>());

        assertEquals("\"[0-9]\"", result);
    }

    @Test
    void testDefaultListValueWithNotResolvedPlaceholder() {
        String value = "${k:[v1,v2,${v3}_aaa]}";

        Assertions.assertThrows(
                ConfigException.BadValue.class,
                () ->
                        PlaceholderUtils.processPlaceholders(
                                value, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>()));
    }

    @Test
    void testDefaultListValueWithQuotedPlaceholder() {
        String value = "${\"k\":[v1,v2,\"${v3}\",\"${v4}_aaa\"]}";
        Object result =
                PlaceholderUtils.processPlaceholders(
                        value, key -> false, new LinkedHashMap<>(), new LinkedHashMap<>());

        Assertions.assertEquals("[v1,v2,\"${v3}\",\"${v4}_aaa\"]", result);
    }

    @Test
    void testDefaultWithExternalPlaceholderThrows() {
        assertThrows(
                ConfigException.BadValue.class,
                () ->
                        PlaceholderUtils.processPlaceholders(
                                "${k:${x}}",
                                key -> false,
                                new LinkedHashMap<>(),
                                new LinkedHashMap<>()));
    }

    @Test
    void testDefaultWithSystemPlaceholderThrows() {
        assertThrows(
                ConfigException.BadValue.class,
                () ->
                        PlaceholderUtils.processPlaceholders(
                                "${k:${table_name}}",
                                key -> false,
                                new LinkedHashMap<>(),
                                new LinkedHashMap<>()));
    }

    @Test
    void testWithWithoutDefaultThrows() {
        assertThrows(
                ConfigException.BadValue.class,
                () ->
                        PlaceholderUtils.processPlaceholders(
                                "${x:def} ${x}",
                                key -> false,
                                new LinkedHashMap<>(),
                                new LinkedHashMap<>()));
    }

    @Test
    void testEmptyUserValueFallsBackToDefault() {
        Map<String, String> userVars = Map.of("k", "");
        assertEquals(
                "default",
                PlaceholderUtils.processPlaceholders(
                        "${k:default}", userVars::containsKey, userVars, new LinkedHashMap<>()));
    }

    @Test
    void testHasOnlyOnePlaceholder() {
        assertTrue(PlaceholderUtils.hasOnlyOnePlaceholder("${k}"));
        assertTrue(PlaceholderUtils.hasOnlyOnePlaceholder("${k:v}"));
        assertFalse(PlaceholderUtils.hasOnlyOnePlaceholder("prefix_${k}_suffix"));
        assertFalse(PlaceholderUtils.hasOnlyOnePlaceholder("${k1},${k2}"));
    }
}
