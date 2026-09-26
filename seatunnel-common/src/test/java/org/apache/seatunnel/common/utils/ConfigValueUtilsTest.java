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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigValueUtilsTest {

    @Test
    void testParseValueWithEmptyString() {
        Object result = ConfigValueUtils.parseValue("");
        assertNotNull(result);
    }

    @Test
    void testParseValueWithNull() {
        assertDoesNotThrow(
                () -> {
                    ConfigValueUtils.parseValue(null);
                });
    }

    @Test
    void testParseValueWithBlankString() {
        Object result = ConfigValueUtils.parseValue(" ");
        assertNotNull(result);
    }

    @Test
    void testParseValueWithNormalString() {
        Object result = ConfigValueUtils.parseValue("\"hello\"").unwrapped();
        assertEquals("hello", result);
    }

    @Test
    void testParseValueWithNumber() {
        Object result = ConfigValueUtils.parseValue("235.60").unwrapped();
        assertEquals("235.60", result);
    }

    @Test
    void testParseValueWithNumber2() {
        Object result = ConfigValueUtils.parseValue("007").unwrapped();
        assertEquals("007", result);
    }

    @Test
    void testParseValueWithNumber3() {
        Object result = ConfigValueUtils.parseValue("1e3").unwrapped();
        assertEquals("1e3", result);
    }

    @Test
    void testParseValueWithBoolean() {
        Object result = ConfigValueUtils.parseValue("true").unwrapped();
        assertEquals("true", result);
    }

    @Test
    void testParseValueWithArray() {
        Object result = ConfigValueUtils.parseValue("[\"a\",\"b\"]");
        assertTrue(result instanceof List);
    }

    @Test
    void testParseValueWithJsonObject() {
        Object result = ConfigValueUtils.parseValue("{\"k1\":\"v1\",\"k2\":\"v2\"}");
        assertTrue(result instanceof Map);
    }

    @Test
    void testParseValueWithWrongJsonFormat() {
        // right json format should end with '}]',not ']}'
        String value =
                "[{\"table_path\":\"testdb.t_*\",\"use_regex\":\"true\"},{\"table_path\":\"testdb.tt\"]}";
        Assertions.assertThrows(ConfigException.class, () -> ConfigValueUtils.parseValue(value));
    }

    @Test
    void testParseValueAsStringWithUnclosedJsonFormat() {
        // right json format should end with '}'
        String value = "{\"k1\":\"v1\",\"k2\":\"v2\"";
        Object result = ConfigValueUtils.parseValue(value).unwrapped();
        Assertions.assertInstanceOf(String.class, result);
    }

    @Test
    void testParseValueAsStringWithJsonFormatLike() {
        // right json format should end with '}'
        String value = "{k1,k2,[k3,k4]}";
        Assertions.assertThrows(ConfigException.class, () -> ConfigValueUtils.parseValue(value));
    }

    @Test
    void testParseValueAsStringWithJsonStringFormatLike() {
        // right json format should end with '}'
        String value = "\"{k1,k2,[k3,k4]}\"";
        Object result = ConfigValueUtils.parseValue(value).unwrapped();
        Assertions.assertEquals(result, "{k1,k2,[k3,k4]}");
    }

    @Test
    void testParseValueWithNestedJson() {
        String assert_filed_rules =
                "["
                        + "{"
                        + "\"field_name\":\"movie_id\","
                        + "\"field_type\":\"bigint\","
                        + "\"field_value\":[{\"rule_type\":\"NOT_NULL\"}]"
                        + "},"
                        + "{"
                        + "\"field_name\":\"unix_time\","
                        + "\"field_type\":\"bigint\","
                        + "\"field_value\":[{\"rule_type\":\"NOT_NULL\"}]"
                        + "}"
                        + "]";

        Object result = ConfigValueUtils.parseValue(assert_filed_rules).unwrapped();
        Assertions.assertInstanceOf(List.class, result);
    }
}
