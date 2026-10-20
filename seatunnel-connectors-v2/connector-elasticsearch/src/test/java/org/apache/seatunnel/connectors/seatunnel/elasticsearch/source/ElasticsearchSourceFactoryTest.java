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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

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

class ElasticsearchSourceFactoryTest {

    private OptionRule rule;

    @BeforeEach
    void setUp() {
        rule = new ElasticsearchSourceFactory().optionRule();
    }

    @Test
    void testValidConfigWithIndex() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testValidConfigWithIndexList() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put(
                "index_list",
                Arrays.asList(
                        new HashMap<String, Object>() {
                            {
                                put("index", "idx1");
                            }
                        }));
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testMissingHostsFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("index", "test_index");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("hosts"));
    }

    @Test
    void testMissingIndexAndIndexListFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("index") || ex.getMessage().contains("index_list"));
    }

    @Test
    void testSqlSearchTypeMissingSqlQueryFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("search_type", "SQL");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("sql_query"));
    }

    @Test
    void testSqlSearchTypeBlankSqlQueryFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("search_type", "SQL");
        config.put("sql_query", "   ");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("sql_query"));
    }

    @Test
    void testSqlSearchTypeWithValidSqlQuery() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("search_type", "SQL");
        config.put("sql_query", "SELECT * FROM test_index");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testUsernameWithoutPasswordFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("username", "admin");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("username") || ex.getMessage().contains("password"));
    }

    @Test
    void testPasswordWithoutUsernameFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("password", "secret");
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
        config.put("index", "test_index");
        config.put("username", "admin");
        config.put("password", "secret");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testBasicAuthBlankUsernameFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
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
        config.put("index", "test_index");
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
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("api_key_id") || ex.getMessage().contains("api_key"));
    }

    @Test
    void testApiKeyAuthBlankKeyIdFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "   ");
        config.put("auth.api_key", "my_secret");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("api_key_id"));
    }

    @Test
    void testApiKeyEncodedAuthMissingFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY_ENCODED");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("api_key_encoded"));
    }

    @Test
    void testApiKeyEncodedAuthBlankFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY_ENCODED");
        config.put("auth.api_key_encoded", "  ");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("api_key_encoded"));
    }

    @Test
    void testApiKeyEncodedInvalidBase64Fails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY_ENCODED");
        config.put("auth.api_key_encoded", "not-valid-base64!@#");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("Base64"));
    }

    @Test
    void testApiKeyEncodedBase64WithoutColonFails() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY_ENCODED");
        // "noColonHere" Base64-encoded
        config.put("auth.api_key_encoded", "bm9Db2xvbkhlcmU=");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("id:key"));
    }

    @Test
    void testApiKeyEncodedValidFormatPasses() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY_ENCODED");
        // "id:key" Base64-encoded
        config.put("auth.api_key_encoded", "aWQ6a2V5");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testValidApiKeyAuth() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "my_key_id");
        config.put("auth.api_key", "my_secret");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    /**
     * Verify the error-aggregation contract: when multiple independent rules fail in one config,
     * all errors are reported together in a single exception message instead of fail-fast on the
     * first one. Triggers (1) username-without-password (auth_type=BASIC), (2) search_type=SQL
     * without sql_query.
     */
    @Test
    void testMultipleValidationErrorsAreAggregated() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("username", "admin");
        config.put("search_type", "SQL");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(
                msg.contains("password") || msg.contains("username"),
                "expected basic-auth pair error in: " + msg);
        Assertions.assertTrue(msg.contains("sql_query"), "expected sql_query error in: " + msg);
    }

    @Test
    void testApiKeyAuthWithResidualUsernameDoesNotFail() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "my_key_id");
        config.put("auth.api_key", "my_secret");
        config.put("username", "residual_user");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }
}
