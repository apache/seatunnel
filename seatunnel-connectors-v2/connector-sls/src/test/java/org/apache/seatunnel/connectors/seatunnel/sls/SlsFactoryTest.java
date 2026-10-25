/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.sls.config.SlsBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.sls.sink.SlsSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sls.source.SlsSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlsFactoryTest {

    private static final OptionRule SOURCE_OPTION_RULE = new SlsSourceFactory().optionRule();
    private static final OptionRule SINK_OPTION_RULE = new SlsSinkFactory().optionRule();
    private static final List<String> REQUIRED_STRING_KEYS =
            Arrays.asList(
                    SlsBaseOptions.ENDPOINT.key(),
                    SlsBaseOptions.PROJECT.key(),
                    SlsBaseOptions.LOGSTORE.key(),
                    SlsBaseOptions.ACCESS_KEY_ID.key(),
                    SlsBaseOptions.ACCESS_KEY_SECRET.key());

    @Test
    void testValidSourceConfiguration() {
        assertValidRequiredStrings(SOURCE_OPTION_RULE, "SlsSource");
    }

    @Test
    void testInvalidSourceConfiguration() {
        assertInvalidRequiredStrings(SOURCE_OPTION_RULE, "SlsSource");
    }

    @Test
    void testValidSinkConfiguration() {
        assertValidRequiredStrings(SINK_OPTION_RULE, "SlsSink");
    }

    @Test
    void testInvalidSinkConfiguration() {
        assertInvalidRequiredStrings(SINK_OPTION_RULE, "SlsSink");
    }

    private void assertValidRequiredStrings(OptionRule optionRule, String factoryName) {
        Assertions.assertDoesNotThrow(() -> validate(validConfig(), optionRule, factoryName));

        Map<String, Object> valuesWithSurroundingSpaces = validConfig();
        for (String key : REQUIRED_STRING_KEYS) {
            valuesWithSurroundingSpaces.put(key, " " + valuesWithSurroundingSpaces.get(key) + " ");
        }
        Assertions.assertDoesNotThrow(
                () -> validate(valuesWithSurroundingSpaces, optionRule, factoryName));
    }

    private void assertInvalidRequiredStrings(OptionRule optionRule, String factoryName) {
        for (String key : REQUIRED_STRING_KEYS) {
            Map<String, Object> missing = validConfig();
            missing.remove(key);
            assertInvalid(missing, optionRule, factoryName, key);

            Map<String, Object> empty = validConfig();
            empty.put(key, "");
            assertInvalid(empty, optionRule, factoryName, key);

            Map<String, Object> whitespaceOnly = validConfig();
            whitespaceOnly.put(key, " \t\n ");
            assertInvalid(whitespaceOnly, optionRule, factoryName, key);
        }
    }

    private void assertInvalid(
            Map<String, Object> config,
            OptionRule optionRule,
            String factoryName,
            String optionKey) {
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(config, optionRule, factoryName),
                optionKey);
    }

    private void validate(Map<String, Object> config, OptionRule optionRule, String factoryName) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, factoryName);
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SlsBaseOptions.ENDPOINT.key(), "cn-hangzhou.log.aliyuncs.com");
        config.put(SlsBaseOptions.PROJECT.key(), "seatunnel-project");
        config.put(SlsBaseOptions.LOGSTORE.key(), "seatunnel-logstore");
        config.put(SlsBaseOptions.ACCESS_KEY_ID.key(), "access-key-id");
        config.put(SlsBaseOptions.ACCESS_KEY_SECRET.key(), "access-key-secret");
        return config;
    }
}
