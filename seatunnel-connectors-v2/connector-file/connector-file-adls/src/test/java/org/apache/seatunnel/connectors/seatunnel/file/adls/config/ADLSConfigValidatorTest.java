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

package org.apache.seatunnel.connectors.seatunnel.file.adls.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ADLSConfigValidatorTest {

    @Test
    void rejectsCredentialsFromAnotherAuthenticationMode() {
        Map<String, Object> values = sharedKeyConfig();
        values.put("client_secret", "wrong-mode");

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void rejectsConnectorOwnedAdvancedProperties() {
        Map<String, Object> values = sharedKeyConfig();
        Map<String, String> advanced = new HashMap<>();
        advanced.put("fs.azure.account.key.other.dfs.core.windows.net", "override");
        values.put("hadoop_adls_properties", advanced);

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void acceptsNonConflictingAdvancedProperties() {
        Map<String, Object> values = sharedKeyConfig();
        Map<String, String> advanced = new HashMap<>();
        advanced.put("fs.azure.io.retry.max.retries", "8");
        values.put("hadoop_adls_properties", advanced);

        Assertions.assertDoesNotThrow(
                () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void rejectsCredentialAndClassLoadingAdvancedProperties() {
        String[] keys = {
            "fs.azure.sas.token.provider.type",
            " fs.azure.sas.token.provider.type ",
            "fs.azure.delegation.token.provider.type",
            "fs.azure.enable.delegation.token",
            "fs.azure.identity.transformer.class",
            "fs.azure.shellkeyprovider.script"
        };
        for (String key : keys) {
            Map<String, Object> values = sharedKeyConfig();
            Map<String, String> advanced = new HashMap<>();
            advanced.put(key, "override");
            values.put("hadoop_adls_properties", advanced);
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)),
                    key);
        }
    }

    @Test
    void rejectsTenantIdContainingUrlPathCharacters() {
        Map<String, Object> values = sharedKeyConfig();
        values.remove("account_key");
        values.put("auth_type", "OAUTH_CLIENT_CREDENTIALS");
        values.put("tenant_id", "example.onmicrosoft.com/other");
        values.put("client_id", "client");
        values.put("client_secret", "secret");

        FileConnectorException error =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)));
        Assertions.assertTrue(error.getMessage().contains("tenant_id"));
    }

    private static Map<String, Object> sharedKeyConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("account_name", "testaccount");
        values.put("container", "files");
        values.put("account_key", "secret-key");
        return values;
    }
}
