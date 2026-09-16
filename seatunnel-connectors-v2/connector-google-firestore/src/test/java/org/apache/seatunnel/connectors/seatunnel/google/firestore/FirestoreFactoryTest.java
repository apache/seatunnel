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

package org.apache.seatunnel.connectors.seatunnel.google.firestore;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.config.FirestoreSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.google.firestore.sink.FirestoreSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

class FirestoreFactoryTest {

    private final OptionRule optionRule = new FirestoreSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidConfigWithoutCredentials() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testValidConfigWithCredentials() {
        Map<String, Object> config = validConfig();
        config.put(FirestoreSinkOptions.CREDENTIALS.key(), "encoded-credentials");

        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testMissingProjectIdRejected() {
        Map<String, Object> config = validConfig();
        config.remove(FirestoreSinkOptions.PROJECT_ID.key());

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testMissingCollectionRejected() {
        Map<String, Object> config = validConfig();
        config.remove(FirestoreSinkOptions.COLLECTION.key());

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankProjectIdRejected(String value) {
        assertInvalidOption(FirestoreSinkOptions.PROJECT_ID.key(), value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankCollectionRejected(String value) {
        assertInvalidOption(FirestoreSinkOptions.COLLECTION.key(), value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankCredentialsRejected(String value) {
        assertInvalidOption(FirestoreSinkOptions.CREDENTIALS.key(), value);
    }

    @Test
    void testUnknownOptionRejected() {
        Map<String, Object> config = validConfig();
        config.put("unknown_option", "value");

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void assertInvalidOption(String key, String value) {
        Map<String, Object> config = validConfig();
        config.put(key, value);

        OptionValidationException error =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        Assertions.assertTrue(error.getMessage().contains(key), error.getMessage());
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FirestoreSinkOptions.PROJECT_ID.key(), "test-project");
        config.put(FirestoreSinkOptions.COLLECTION.key(), "test-collection");
        return config;
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "GoogleFirestoreSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }
}
