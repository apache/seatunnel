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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ElasticsearchSinkFactoryTest {

    private OptionRule rule;

    @BeforeEach
    void setUp() {
        rule = new ElasticsearchSinkFactory().optionRule();
    }

    private Map<String, Object> validSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Arrays.asList("localhost:9200"));
        config.put("index", "test_index");
        config.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        config.put("data_save_mode", "APPEND_DATA");
        return config;
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(validSinkConfig())).validate(rule));
    }

    @Test
    void testSupportsSinkDryRunValidation() {
        Assertions.assertTrue(
                new ElasticsearchSinkFactory() instanceof SupportSinkDryRunValidation);
    }

    @Test
    void testDryRunChecksClusterAndStaticIndex() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(validSinkConfig());
        EsRestClient client = mock(EsRestClient.class);
        when(client.checkIndexExist("test_index")).thenReturn(true);

        Assertions.assertDoesNotThrow(
                () -> ElasticsearchSinkFactory.validateConnection(config, client));

        verify(client).getClusterInfo();
        verify(client).checkIndexExist("test_index");
    }

    @Test
    void testDryRunSkipsDynamicIndexExistenceCheck() {
        Map<String, Object> sinkConfig = validSinkConfig();
        sinkConfig.put("index", "events-${now}");
        ReadonlyConfig config = ReadonlyConfig.fromMap(sinkConfig);
        EsRestClient client = mock(EsRestClient.class);

        ElasticsearchSinkFactory.validateConnection(config, client);

        verify(client).getClusterInfo();
        verify(client, org.mockito.Mockito.never())
                .checkIndexExist(org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void testDryRunFailsWhenStaticIndexIsNotAccessible() {
        Map<String, Object> sinkConfig = validSinkConfig();
        sinkConfig.put("schema_save_mode", "ERROR_WHEN_SCHEMA_NOT_EXIST");
        ReadonlyConfig config = ReadonlyConfig.fromMap(sinkConfig);
        EsRestClient client = mock(EsRestClient.class);
        when(client.checkIndexExist("test_index")).thenReturn(false);

        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> ElasticsearchSinkFactory.validateConnection(config, client));
    }

    @Test
    void testDryRunAllowsMissingIndexWhenSchemaCanBeCreated() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(validSinkConfig());
        EsRestClient client = mock(EsRestClient.class);
        when(client.checkIndexExist("test_index")).thenReturn(false);

        Assertions.assertDoesNotThrow(
                () -> ElasticsearchSinkFactory.validateConnection(config, client));
    }

    @Test
    void testMissingHostsFails() {
        Map<String, Object> config = validSinkConfig();
        config.remove("hosts");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("hosts"));
    }

    @Test
    void testMissingIndexFails() {
        Map<String, Object> config = validSinkConfig();
        config.remove("index");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("index"));
    }

    @Test
    void testUsernameWithoutPasswordFails() {
        Map<String, Object> config = validSinkConfig();
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
        Map<String, Object> config = validSinkConfig();
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
        Map<String, Object> config = validSinkConfig();
        config.put("username", "admin");
        config.put("password", "secret");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testBasicAuthBlankUsernameFails() {
        Map<String, Object> config = validSinkConfig();
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
        Map<String, Object> config = validSinkConfig();
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
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(
                ex.getMessage().contains("api_key_id") || ex.getMessage().contains("api_key"));
    }

    @Test
    void testApiKeyAuthBlankValueFails() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "valid_id");
        config.put("auth.api_key", "  ");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("api_key"));
    }

    @Test
    void testApiKeyEncodedAuthBlankFails() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY_ENCODED");
        config.put("auth.api_key_encoded", "  ");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("api_key_encoded"));
    }

    @Test
    void testApiKeyEncodedInvalidFormatFails() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY_ENCODED");
        // valid Base64 but decoded value lacks the required ':' separator
        config.put("auth.api_key_encoded", "bm9Db2xvbkhlcmU=");
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        Assertions.assertTrue(ex.getMessage().contains("id:key"));
    }

    @Test
    void testApiKeyEncodedValidFormatPasses() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY_ENCODED");
        config.put("auth.api_key_encoded", "aWQ6a2V5");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testValidApiKeyAuth() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "my_key_id");
        config.put("auth.api_key", "my_secret");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }

    @Test
    void testApiKeyAuthWithResidualUsernameDoesNotFail() {
        Map<String, Object> config = validSinkConfig();
        config.put("auth_type", "API_KEY");
        config.put("auth.api_key_id", "my_key_id");
        config.put("auth.api_key", "my_secret");
        config.put("username", "residual_user");
        Assertions.assertDoesNotThrow(
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
    }
}
