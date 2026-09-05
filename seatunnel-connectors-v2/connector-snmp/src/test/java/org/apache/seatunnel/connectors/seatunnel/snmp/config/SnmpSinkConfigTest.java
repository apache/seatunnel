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

package org.apache.seatunnel.connectors.seatunnel.snmp.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SnmpSinkConfigTest {

    @Test
    void testDefaultsAndSharedTargetOptions() {
        SnmpSinkConfig config = new SnmpSinkConfig(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals("127.0.0.1", config.getHost());
        Assertions.assertEquals(161, config.getPort());
        Assertions.assertEquals("unit-test-community", config.getCommunity());
        Assertions.assertEquals(5000L, config.getTimeoutMillis());
        Assertions.assertEquals(1, config.getRetries());
        Assertions.assertEquals("oid", config.getOidField());
        Assertions.assertEquals("value", config.getValueField());
        Assertions.assertEquals("value_type", config.getValueTypeField());
    }

    @Test
    void testCustomFieldMappingIsTrimmed() {
        Map<String, Object> values = baseConfig();
        values.put("oid_field", " target_oid ");
        values.put("value_field", " target_value ");
        values.put("value_type_field", " target_type ");

        SnmpSinkConfig config = new SnmpSinkConfig(ReadonlyConfig.fromMap(values));

        Assertions.assertEquals("target_oid", config.getOidField());
        Assertions.assertEquals("target_value", config.getValueField());
        Assertions.assertEquals("target_type", config.getValueTypeField());
    }

    @Test
    void testInvalidTargetOptionsDoNotDiscloseCommunity() {
        Map<String, Object> invalidPort = baseConfig();
        invalidPort.put("port", 65536);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new SnmpSinkConfig(ReadonlyConfig.fromMap(invalidPort)));

        Assertions.assertFalse(exception.getMessage().contains("unit-test-community"));

        Map<String, Object> invalidTimeout = baseConfig();
        invalidTimeout.put("timeout_millis", 0L);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSinkConfig(ReadonlyConfig.fromMap(invalidTimeout)));

        Map<String, Object> invalidRetries = baseConfig();
        invalidRetries.put("retries", -1);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSinkConfig(ReadonlyConfig.fromMap(invalidRetries)));
    }

    @Test
    void testMappedFieldsMustBeNonBlankAndDistinct() {
        Map<String, Object> blank = baseConfig();
        blank.put("oid_field", " ");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSinkConfig(ReadonlyConfig.fromMap(blank)));

        Map<String, Object> duplicate = baseConfig();
        duplicate.put("value_field", "oid");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSinkConfig(ReadonlyConfig.fromMap(duplicate)));
    }

    public static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "unit-test-community");
        return values;
    }
}
