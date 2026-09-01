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

package org.apache.seatunnel.connectors.seatunnel.activemq;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.activemq.config.ActivemqSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.activemq.sink.ActivemqSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ActivemqFactoryTest {

    private final OptionRule optionRule = new ActivemqSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(requiredConfig()));
    }

    @Test
    void testBlankRequiredOptionsRejected() {
        Map<String, Object> blankUriConfig = requiredConfig();
        blankUriConfig.put(ActivemqSinkOptions.URI.key(), " ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(blankUriConfig));

        Map<String, Object> blankQueueNameConfig = requiredConfig();
        blankQueueNameConfig.put(ActivemqSinkOptions.QUEUE_NAME.key(), "\t");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(blankQueueNameConfig));
    }

    @Test
    void testBlankClientIdRejected() {
        Map<String, Object> config = requiredConfig();
        config.put(ActivemqSinkOptions.CLIENT_ID.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testCredentialsMustBeBundled() {
        Map<String, Object> usernameOnlyConfig = requiredConfig();
        usernameOnlyConfig.put(ActivemqSinkOptions.USERNAME.key(), "user");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(usernameOnlyConfig));

        Map<String, Object> passwordOnlyConfig = requiredConfig();
        passwordOnlyConfig.put(ActivemqSinkOptions.PASSWORD.key(), "password");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(passwordOnlyConfig));
    }

    @Test
    void testSupportedOptionalOptions() {
        Map<String, Object> config = requiredConfig();
        config.put(ActivemqSinkOptions.CLIENT_ID.key(), "client-id");
        config.put(ActivemqSinkOptions.CLOSE_TIMEOUT.key(), 1000);
        config.put(ActivemqSinkOptions.CONSUMER_EXPIRY_CHECK_ENABLED.key(), true);
        config.put(ActivemqSinkOptions.WARN_ABOUT_UNSTARTED_CONNECTION_TIMEOUT.key(), -1);
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "ActiveMQSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ActivemqSinkOptions.URI.key(), "tcp://localhost:61616");
        config.put(ActivemqSinkOptions.QUEUE_NAME.key(), "test-queue");
        return config;
    }
}
