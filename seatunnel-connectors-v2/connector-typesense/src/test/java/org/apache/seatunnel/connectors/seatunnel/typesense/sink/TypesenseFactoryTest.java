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

package org.apache.seatunnel.connectors.seatunnel.typesense.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.typesense.source.TypesenseSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TypesenseFactoryTest {

    private final OptionRule sourceOptionRule = new TypesenseSourceFactory().optionRule();
    private final OptionRule sinkOptionRule = new TypesenseSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceOptionRule);
        Assertions.assertNotNull(sinkOptionRule);
    }

    @Test
    void testValidSourceConfiguration() {
        Assertions.assertDoesNotThrow(
                () -> validate(sourceConfig(), sourceOptionRule, "TypesenseSource"));
    }

    @Test
    void testInvalidSourceConnectionOptions() {
        assertMissingRejected(sourceConfig(), TypesenseSourceOptions.HOSTS.key(), sourceOptionRule);
        assertMissingRejected(
                sourceConfig(), TypesenseSourceOptions.APIKEY.key(), sourceOptionRule);

        Map<String, Object> emptyHosts = sourceConfig();
        emptyHosts.put(TypesenseSourceOptions.HOSTS.key(), Collections.emptyList());
        assertInvalid(emptyHosts, sourceOptionRule);

        assertStringValueRejected(
                sourceConfig(), TypesenseSourceOptions.APIKEY.key(), "", sourceOptionRule);
        assertStringValueRejected(
                sourceConfig(), TypesenseSourceOptions.APIKEY.key(), "   \t", sourceOptionRule);
    }

    @Test
    void testValidSinkConfiguration() {
        Assertions.assertDoesNotThrow(
                () -> validate(sinkConfig(), sinkOptionRule, "TypesenseSink"));
    }

    @Test
    void testInvalidSinkConnectionOptions() {
        assertMissingRejected(sinkConfig(), TypesenseSinkOptions.HOSTS.key(), sinkOptionRule);
        assertMissingRejected(sinkConfig(), TypesenseSinkOptions.COLLECTION.key(), sinkOptionRule);
        assertMissingRejected(sinkConfig(), TypesenseSinkOptions.APIKEY.key(), sinkOptionRule);

        Map<String, Object> emptyHosts = sinkConfig();
        emptyHosts.put(TypesenseSinkOptions.HOSTS.key(), Collections.emptyList());
        assertInvalid(emptyHosts, sinkOptionRule);

        for (String key :
                new String[] {
                    TypesenseSinkOptions.COLLECTION.key(), TypesenseSinkOptions.APIKEY.key()
                }) {
            assertStringValueRejected(sinkConfig(), key, "", sinkOptionRule);
            assertStringValueRejected(sinkConfig(), key, "   \t", sinkOptionRule);
        }
    }

    private void assertMissingRejected(
            Map<String, Object> config, String key, OptionRule optionRule) {
        config.remove(key);
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(config, optionRule, "Typesense"),
                key);
    }

    private void assertStringValueRejected(
            Map<String, Object> config, String key, String value, OptionRule optionRule) {
        config.put(key, value);
        assertInvalid(config, optionRule);
    }

    private void assertInvalid(Map<String, Object> config, OptionRule optionRule) {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule, "Typesense"));
    }

    private void validate(Map<String, Object> config, OptionRule optionRule, String name) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, name);
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(TypesenseSourceOptions.HOSTS.key(), Collections.singletonList("localhost:8108"));
        config.put(TypesenseSourceOptions.APIKEY.key(), "source-api-key");
        return config;
    }

    private Map<String, Object> sinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(TypesenseSinkOptions.HOSTS.key(), Collections.singletonList("localhost:8108"));
        config.put(TypesenseSinkOptions.COLLECTION.key(), "collection");
        config.put(TypesenseSinkOptions.APIKEY.key(), "sink-api-key");
        return config;
    }
}
