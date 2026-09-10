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

package org.apache.seatunnel.connectors.seatunnel.slack.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class SlackFactoryTest {

    @Test
    void validConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validOptions()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"webhooks_url", "oauth_token", "slack_channel"})
    void missingRequiredOption(String key) {
        Map<String, Object> options = validOptions();
        options.remove(key);
        assertInvalid(options, key);
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("blankRequiredOptions")
    void blankRequiredOption(String key, String description, String value) {
        Map<String, Object> options = validOptions();
        options.put(key, value);
        assertInvalid(options, key);
    }

    private static Stream<Arguments> blankRequiredOptions() {
        return Stream.of("webhooks_url", "oauth_token", "slack_channel")
                .flatMap(
                        key ->
                                Stream.of(
                                        Arguments.of(key, "empty", ""),
                                        Arguments.of(key, "space", " "),
                                        Arguments.of(key, "tab", "\t"),
                                        Arguments.of(key, "newline", "\n"),
                                        Arguments.of(key, "mixed whitespace", " \t\r\n ")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"webhooks_url", "oauth_token", "slack_channel"})
    void preserveNonblankValues(String key) {
        Map<String, Object> options = validOptions();
        options.put(key, " arbitrary nonblank value ");
        Assertions.assertDoesNotThrow(() -> validate(options));
    }

    private void assertInvalid(Map<String, Object> options, String key) {
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(options));
        Assertions.assertTrue(exception.getMessage().contains(key));
    }

    private void validate(Map<String, Object> options) {
        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                .validate(new SlackSinkFactory().optionRule());
    }

    private Map<String, Object> validOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("webhooks_url", "https://hooks.slack.com/services/test/test/test");
        options.put("oauth_token", "test-oauth-token");
        options.put("slack_channel", "seatunnel-alerts");
        return options;
    }
}
