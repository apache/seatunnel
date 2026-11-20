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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class TiDBSourceFactoryTest {
    private static final String DATABASE_NAME = "database-name";
    private static final String TABLE_NAME = "table-name";
    private static final String PD_ADDRESSES = "pd-addresses";

    private final OptionRule optionRule = new TiDBSourceFactory().optionRule();

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidConfigAcceptedWithoutChangingValues() {
        Map<String, Object> config = validConfig();

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals("inventory", readonlyConfig.get(TiDBSourceOptions.DATABASE_NAME));
        Assertions.assertEquals("orders", readonlyConfig.get(TiDBSourceOptions.TABLE_NAME));
        Assertions.assertEquals(
                "127.0.0.1:2379", readonlyConfig.get(TiDBSourceOptions.PD_ADDRESSES));
    }

    @ParameterizedTest
    @ValueSource(strings = {DATABASE_NAME, TABLE_NAME, PD_ADDRESSES})
    void testSurroundingWhitespaceIsPreserved(String key) {
        Map<String, Object> config = validConfig();
        String padded = " \t" + config.get(key) + " \r\n";
        config.put(key, padded);

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        // Validation does not trim a supplied value.
        Assertions.assertEquals(padded, readonlyConfig.getOptional(optionFor(key)).orElse(null));
    }

    @ParameterizedTest
    @MethodSource("blankValues")
    void testBlankRequiredOptionRejected(String key, String value) {
        Map<String, Object> config = validConfig();
        config.put(key, value);

        assertInvalid(config, key);
    }

    private static Stream<Arguments> blankValues() {
        return Stream.of(DATABASE_NAME, TABLE_NAME, PD_ADDRESSES)
                .flatMap(
                        key ->
                                Stream.of("", " ", "\t", "\n", " \t\r\n ")
                                        .map(value -> Arguments.of(key, value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {DATABASE_NAME, TABLE_NAME, PD_ADDRESSES})
    void testMissingRequiredOptionRejected(String key) {
        Map<String, Object> config = validConfig();
        config.remove(key);

        assertInvalid(config, key);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(DATABASE_NAME, "inventory");
        config.put(TABLE_NAME, "orders");
        config.put(PD_ADDRESSES, "127.0.0.1:2379");
        return config;
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "TiDB-CDC");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }

    private void assertInvalid(Map<String, Object> config, String key) {
        OptionValidationException error =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(error.getMessage().contains(key), error.getMessage());
    }

    private static org.apache.seatunnel.api.configuration.Option<String> optionFor(String key) {
        switch (key) {
            case DATABASE_NAME:
                return TiDBSourceOptions.DATABASE_NAME;
            case TABLE_NAME:
                return TiDBSourceOptions.TABLE_NAME;
            case PD_ADDRESSES:
                return TiDBSourceOptions.PD_ADDRESSES;
            default:
                throw new IllegalArgumentException("unexpected key " + key);
        }
    }
}
