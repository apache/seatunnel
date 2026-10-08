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

package org.apache.seatunnel.connectors.seatunnel.lance;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.lance.sink.LanceSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceFactoryTest {

    private final OptionRule optionRule = new LanceSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testNonblankValuesAccepted() {
        Map<String, Object> config = validConfig();
        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testNonblankValuesWithSurroundingWhitespaceAreNotTrimmed() {
        Map<String, Object> config = validConfig();
        config.put("dataset_path", " /tmp/test.lance ");
        config.put("namespace_type", " dir ");

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(
                " /tmp/test.lance ", readonlyConfig.get(LanceCommonOptions.KEY_DATASET_PATH));
        Assertions.assertEquals(" dir ", readonlyConfig.get(LanceCommonOptions.KEY_NAMESPACE_TYPE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankDatasetPathRejected(String value) {
        assertInvalidOption("dataset_path", value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void testBlankNamespaceTypeRejected(String value) {
        assertInvalidOption("namespace_type", value);
    }

    @Test
    void testOmittedDatasetPathUsesDefault() {
        ReadonlyConfig config =
                Assertions.assertDoesNotThrow(
                        () -> validate(Collections.singletonMap("namespace_type", "dir")));

        Assertions.assertEquals("/test.lance", config.get(LanceCommonOptions.KEY_DATASET_PATH));
    }

    @Test
    void testOmittedNamespaceTypeUsesDefault() {
        ReadonlyConfig config =
                Assertions.assertDoesNotThrow(
                        () ->
                                validate(
                                        Collections.singletonMap(
                                                "dataset_path", "/tmp/test.lance")));

        Assertions.assertEquals("dir", config.get(LanceCommonOptions.KEY_NAMESPACE_TYPE));
    }

    @Test
    void testOmittedOptionsUseDefaults() {
        ReadonlyConfig config =
                Assertions.assertDoesNotThrow(() -> validate(Collections.emptyMap()));

        Assertions.assertEquals("/test.lance", config.get(LanceCommonOptions.KEY_DATASET_PATH));
        Assertions.assertEquals("dir", config.get(LanceCommonOptions.KEY_NAMESPACE_TYPE));
        Assertions.assertEquals("", config.get(LanceCommonOptions.KEY_NAMESPACE_ID));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " \t ", "root"})
    void testOptionalNamespaceIdPreserved(String value) {
        ReadonlyConfig config =
                Assertions.assertDoesNotThrow(
                        () -> validate(Collections.singletonMap("namespace_id", value)));

        Assertions.assertEquals(value, config.get(LanceCommonOptions.KEY_NAMESPACE_ID));
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
        config.put("dataset_path", "/tmp/test.lance");
        config.put("namespace_type", "dir");
        return config;
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "Lance");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }
}
