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

package org.apache.seatunnel.connectors.seatunnel.snmp.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class SnmpSourceFactoryTest {

    @Test
    void testFactoryIdentityAndOptions() {
        SnmpSourceFactory factory = new SnmpSourceFactory();

        Assertions.assertEquals(SnmpSourceOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        Assertions.assertEquals(SnmpSource.class, factory.getSourceClass());

        OptionRule rule = factory.optionRule();
        List<Option<?>> required =
                rule.getRequiredOptions().stream()
                        .flatMap(group -> group.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(required.contains(SnmpSourceOptions.HOST));
        Assertions.assertTrue(required.contains(SnmpSourceOptions.COMMUNITY));
        Assertions.assertTrue(required.contains(SnmpSourceOptions.OIDS));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSourceOptions.PORT));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSourceOptions.TIMEOUT_MILLIS));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSourceOptions.RETRIES));
        Assertions.assertTrue(
                rule.getOptionalOptions().contains(SnmpSourceOptions.POLL_INTERVAL_MILLIS));
    }

    @Test
    void testOptionRuleAcceptsDefaultsAndNumericBoundaries() {
        validate(baseConfig());
        Map<String, Object> values = baseConfig();
        values.put("port", 1);
        values.put("timeout_millis", 1L);
        values.put("retries", 0);
        values.put("poll_interval_millis", 1L);
        validate(values);
        values.put("port", 65535);
        values.put("timeout_millis", Long.MAX_VALUE);
        values.put("retries", Integer.MAX_VALUE);
        values.put("poll_interval_millis", Long.MAX_VALUE);
        validate(values);
    }

    @Test
    void testOptionRuleRejectsMissingRequiredOptions() {
        for (String key : new String[] {"host", "community", "oids"}) {
            Map<String, Object> values = baseConfig();
            values.remove(key);
            Assertions.assertThrows(OptionValidationException.class, () -> validate(values), key);
        }
    }

    @Test
    void testOptionRuleRejectsBlankStrings() {
        for (String key : new String[] {"host", "community"}) {
            assertInvalidOption(key, "");
            assertInvalidOption(key, " \t\n");
        }
    }

    @Test
    void testOptionRuleRejectsInvalidNumericValuesWithoutDisclosingCommunity() {
        assertInvalidOption("port", 0);
        assertInvalidOption("port", 65536);
        assertInvalidOption("timeout_millis", 0L);
        assertInvalidOption("timeout_millis", -1L);
        assertInvalidOption("retries", -1);
        assertInvalidOption("poll_interval_millis", 0L);
    }

    @Test
    void testOptionRulePreservesWhitespaceForRuntimeNormalization() {
        Map<String, Object> values = baseConfig();
        values.put("host", " 127.0.0.1 ");
        values.put("community", " monitoring-community ");
        ReadonlyConfig config = ReadonlyConfig.fromMap(values);
        ConfigValidator.of(config).validate(new SnmpSourceFactory().optionRule());
        Assertions.assertEquals(" monitoring-community ", config.get(SnmpSourceOptions.COMMUNITY));
    }

    private static void assertInvalidOption(String key, Object value) {
        Map<String, Object> values = baseConfig();
        values.put(key, value);
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(values));
        Assertions.assertTrue(exception.getMessage().contains(key));
        Assertions.assertFalse(exception.getMessage().contains("monitoring-community"));
    }

    private static void validate(Map<String, Object> values) {
        ConfigValidator.of(ReadonlyConfig.fromMap(values))
                .validate(new SnmpSourceFactory().optionRule());
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "monitoring-community");
        values.put("oids", Arrays.asList(".1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.5.0"));
        return values;
    }
}
