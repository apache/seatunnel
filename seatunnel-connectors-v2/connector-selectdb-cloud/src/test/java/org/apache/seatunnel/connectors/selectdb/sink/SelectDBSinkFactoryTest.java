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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SelectDBSinkFactoryTest {

    private static final OptionRule OPTION_RULE = new SelectDBSinkFactory().optionRule();
    private static final List<String> REQUIRED_STRING_KEYS =
            Arrays.asList(
                    SelectDBSinkOptions.JDBC_URL.key(),
                    SelectDBSinkOptions.LOAD_URL.key(),
                    SelectDBSinkOptions.CLUSTER_NAME.key(),
                    SelectDBSinkOptions.USERNAME.key(),
                    SelectDBSinkOptions.TABLE_IDENTIFIER.key());

    @Test
    void testValidConfiguration() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));

        Map<String, Object> valuesWithSurroundingSpaces = validConfig();
        for (String key : REQUIRED_STRING_KEYS) {
            valuesWithSurroundingSpaces.put(key, " " + valuesWithSurroundingSpaces.get(key) + " ");
        }
        Assertions.assertDoesNotThrow(() -> validate(valuesWithSurroundingSpaces));
    }

    @Test
    void testMissingRequiredStringsRejected() {
        for (String key : REQUIRED_STRING_KEYS) {
            Map<String, Object> config = validConfig();
            config.remove(key);

            Assertions.assertThrows(OptionValidationException.class, () -> validate(config), key);
        }
    }

    @Test
    void testEmptyRequiredStringsRejected() {
        assertRequiredStringsRejected("");
    }

    @Test
    void testWhitespaceOnlyRequiredStringsRejected() {
        assertRequiredStringsRejected("  \t");
    }

    @Test
    void testPasswordRemainsOptional() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));

        Map<String, Object> emptyPassword = validConfig();
        emptyPassword.put(SelectDBSinkOptions.PASSWORD.key(), "");
        Assertions.assertDoesNotThrow(() -> validate(emptyPassword));
    }

    private void assertRequiredStringsRejected(String value) {
        for (String key : REQUIRED_STRING_KEYS) {
            Map<String, Object> config = validConfig();
            config.put(key, value);

            Assertions.assertThrows(OptionValidationException.class, () -> validate(config), key);
        }
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(
                readonlyConfig, OPTION_RULE, SelectDBSinkOptions.IDENTIFIER);
        ConfigValidator.of(readonlyConfig).validate(OPTION_RULE);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(SelectDBSinkOptions.JDBC_URL.key(), "selectdb.example.com:9030");
        config.put(SelectDBSinkOptions.LOAD_URL.key(), "selectdb.example.com:8080");
        config.put(SelectDBSinkOptions.CLUSTER_NAME.key(), "selectdb-cluster");
        config.put(SelectDBSinkOptions.USERNAME.key(), "selectdb-user");
        config.put(SelectDBSinkOptions.TABLE_IDENTIFIER.key(), "database.table");
        return config;
    }
}
