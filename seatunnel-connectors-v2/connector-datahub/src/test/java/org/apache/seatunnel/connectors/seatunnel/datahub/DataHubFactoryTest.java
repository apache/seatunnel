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

package org.apache.seatunnel.connectors.seatunnel.datahub;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.datahub.sink.DataHubSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DataHubFactoryTest {

    private final OptionRule optionRule = new DataHubSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testMissingRequiredOptionsRejected() {
        for (String key : requiredKeys()) {
            Map<String, Object> config = validConfig();
            config.remove(key);
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config), key);
        }
    }

    @Test
    void testEmptyRequiredOptionsRejected() {
        assertInvalidRequiredValue("");
    }

    @Test
    void testWhitespaceOnlyRequiredOptionsRejected() {
        assertInvalidRequiredValue("   \t");
    }

    private void assertInvalidRequiredValue(String value) {
        for (String key : requiredKeys()) {
            Map<String, Object> config = validConfig();
            config.put(key, value);
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config), key);
        }
    }

    private String[] requiredKeys() {
        return new String[] {
            DataHubSinkOptions.ENDPOINT.key(),
            DataHubSinkOptions.ACCESS_ID.key(),
            DataHubSinkOptions.ACCESS_KEY.key(),
            DataHubSinkOptions.PROJECT.key(),
            DataHubSinkOptions.TOPIC.key()
        };
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "DataHubSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(DataHubSinkOptions.ENDPOINT.key(), "https://datahub.example.com");
        config.put(DataHubSinkOptions.ACCESS_ID.key(), "access-id");
        config.put(DataHubSinkOptions.ACCESS_KEY.key(), "access-key");
        config.put(DataHubSinkOptions.PROJECT.key(), "project");
        config.put(DataHubSinkOptions.TOPIC.key(), "topic");
        return config;
    }
}
