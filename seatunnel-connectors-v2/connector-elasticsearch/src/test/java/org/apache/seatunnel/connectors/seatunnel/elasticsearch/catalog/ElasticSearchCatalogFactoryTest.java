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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class ElasticSearchCatalogFactoryTest {

    private OptionRule rule;

    @BeforeEach
    void setUp() {
        rule = new ElasticSearchCatalogFactory().optionRule();
    }

    @Test
    void testValidConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testMissingHostsFails() {
        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("hosts"));
    }

    @Test
    void testUsernameWithoutPasswordFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("username", "admin");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("username") || ex.getMessage().contains("password"));
    }

    @Test
    void testValidBasicAuth() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("username", "admin");
        config.put("password", "secret");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testBasicAuthBlankUsernameFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("username", "   ");
        config.put("password", "secret");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("username") || ex.getMessage().contains("blank"));
    }

    @Test
    void testBasicAuthBlankPasswordFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("username", "admin");
        config.put("password", "   ");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("password") || ex.getMessage().contains("blank"));
    }

    @Test
    void testApiKeyAuthMissingKeysFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("auth_type", "API_KEY");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("api_key_id") || ex.getMessage().contains("api_key"));
    }

    @Test
    void testApiKeyEncodedInvalidFormatFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("auth_type", "API_KEY_ENCODED");
        config.put("auth.api_key_encoded", "not-base64-!@#");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("Base64"));
    }
}
