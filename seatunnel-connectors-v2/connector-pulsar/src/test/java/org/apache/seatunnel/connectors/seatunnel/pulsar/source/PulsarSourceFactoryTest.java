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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PulsarSourceFactoryTest {

    @Test
    void factoryIdentifier() {
        PulsarSourceFactory pulsarSourceFactory = new PulsarSourceFactory();
        Assertions.assertEquals(
                PulsarSourceOptions.IDENTIFIER, pulsarSourceFactory.factoryIdentifier());
    }

    @Test
    void optionRule() {
        PulsarSourceFactory pulsarSourceFactory = new PulsarSourceFactory();
        OptionRule optionRule = pulsarSourceFactory.optionRule();
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidSourceConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validSourceConfig()));
    }

    @Test
    void testMissingClientServiceUrlFails() {
        Map<String, Object> config = validSourceConfig();
        config.remove(PulsarSourceOptions.CLIENT_SERVICE_URL.key());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.CLIENT_SERVICE_URL.key()));
    }

    @Test
    void testMissingAdminServiceUrlFails() {
        Map<String, Object> config = validSourceConfig();
        config.remove(PulsarSourceOptions.ADMIN_SERVICE_URL.key());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.ADMIN_SERVICE_URL.key()));
    }

    @Test
    void testTopicAndTopicPatternAreExclusive() {
        Map<String, Object> config = validSourceConfig();
        config.put(PulsarSourceOptions.TOPIC_PATTERN.key(), "test-topic-.*");
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(exception.getMessage().contains("mutually exclusive"));
    }

    @Test
    void testExactlyOneOfTopicSourceMustBeSet() {
        Map<String, Object> config = validSourceConfig();
        config.remove(PulsarSourceOptions.TOPIC.key());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(exception.getMessage().contains("exactly one option must be set"));
    }

    @Test
    void testStartupModeTimestampRequiresTimestamp() {
        Map<String, Object> config = validSourceConfig();
        config.put(
                PulsarSourceOptions.CURSOR_STARTUP_MODE.key(),
                PulsarSourceOptions.StartMode.TIMESTAMP.name());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(PulsarSourceOptions.CURSOR_STARTUP_TIMESTAMP.key()));
    }

    @Test
    void testStartupModeSubscriptionRequiresResetMode() {
        Map<String, Object> config = validSourceConfig();
        config.put(
                PulsarSourceOptions.CURSOR_STARTUP_MODE.key(),
                PulsarSourceOptions.StartMode.SUBSCRIPTION.name());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.CURSOR_RESET_MODE.key()));
    }

    @Test
    void testStopModeTimestampRequiresTimestamp() {
        Map<String, Object> config = validSourceConfig();
        config.put(
                PulsarSourceOptions.CURSOR_STOP_MODE.key(),
                PulsarSourceOptions.StopMode.TIMESTAMP.name());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.CURSOR_STOP_TIMESTAMP.key()));
    }

    @Test
    void testAuthOptionsMustBeBundled() {
        Map<String, Object> config = validSourceConfig();
        config.put(
                PulsarSourceOptions.AUTH_PLUGIN_CLASS.key(),
                "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(exception.getMessage().contains("bundled"));
    }

    @Test
    void testValidSourceConfigWithBundledAuthOptions() {
        Map<String, Object> config = validSourceConfig();
        config.put(
                PulsarSourceOptions.AUTH_PLUGIN_CLASS.key(),
                "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        config.put(PulsarSourceOptions.AUTH_PARAMS.key(), "token:dummy");
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testBlankClientServiceUrlFails() {
        Map<String, Object> config = validSourceConfig();
        config.put(PulsarSourceOptions.CLIENT_SERVICE_URL.key(), "");
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.CLIENT_SERVICE_URL.key()));
    }

    @Test
    void testBlankAdminServiceUrlFails() {
        Map<String, Object> config = validSourceConfig();
        config.put(PulsarSourceOptions.ADMIN_SERVICE_URL.key(), "   ");
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.ADMIN_SERVICE_URL.key()));
    }

    @Test
    void testBlankSubscriptionNameFails() {
        Map<String, Object> config = validSourceConfig();
        config.put(PulsarSourceOptions.SUBSCRIPTION_NAME.key(), "");
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(
                exception.getMessage().contains(PulsarSourceOptions.SUBSCRIPTION_NAME.key()));
    }

    private Map<String, Object> validSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(PulsarSourceOptions.CLIENT_SERVICE_URL.key(), "pulsar://localhost:6650");
        config.put(PulsarSourceOptions.ADMIN_SERVICE_URL.key(), "http://localhost:8080");
        config.put(PulsarSourceOptions.SUBSCRIPTION_NAME.key(), "seatunnel-subscription");
        config.put(PulsarSourceOptions.TOPIC.key(), "test-topic");
        return config;
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new PulsarSourceFactory().optionRule());
    }
}
