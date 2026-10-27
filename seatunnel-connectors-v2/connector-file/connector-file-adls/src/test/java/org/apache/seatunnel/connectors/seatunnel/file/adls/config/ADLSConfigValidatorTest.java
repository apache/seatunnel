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
import java.util.Locale;
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
        advanced.put("fs.azure.read.request.size", "4194304");
        advanced.put("fs.azure.write.request.size.testaccount.dfs.core.windows.net", "4194304");
        advanced.put("fs.azure.enable.autothrottling", "true");
        values.put("hadoop_adls_properties", advanced);

        Assertions.assertDoesNotThrow(
                () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)));
    }

    @Test
    void rejectsBlankAdvancedPropertyKeys() {
        for (String key : new String[] {"", " \t\n"}) {
            Map<String, Object> values = sharedKeyConfig();
            Map<String, String> advanced = new HashMap<>();
            advanced.put(key, "sentinel-sensitive-value");
            values.put("hadoop_adls_properties", advanced);

            FileConnectorException error =
                    Assertions.assertThrows(
                            FileConnectorException.class,
                            () ->
                                    ADLSHadoopConf.buildWithReadOnlyConfig(
                                            ReadonlyConfig.fromMap(values)));
            Assertions.assertTrue(error.getMessage().contains("hadoop_adls_properties"));
            Assertions.assertTrue(error.getMessage().contains("blank keys"));
            Assertions.assertFalse(error.getMessage().contains("sentinel-sensitive-value"));
        }
    }

    @Test
    void rejectsCredentialAndClassLoadingAdvancedProperties() {
        String[] keys = {
            "fs.azure.sas.token.provider.type",
            "fs.azure.sas.fixed.token",
            "fs.azure.account.oauth.provider.type",
            "fs.azure.account.keyprovider",
            "fs.azure.delegation.token.provider.type",
            "fs.azure.enable.delegation.token",
            "fs.azure.identity.transformer.class",
            "fs.azure.shellkeyprovider.script",
            "abfs.external.authorization.class",
            "fs.azure.abfs.endpoint",
            "fs.azure.custom.token.provider.class",
            "fs.azure.encryption.context.provider.type",
            "fs.azure.encryption.encoded.client-provided-key",
            "fs.azure.metrics.account.key",
            "hadoop.security.credential.provider.path"
        };
        for (String key : keys) {
            for (String variant :
                    new String[] {
                        key,
                        key + ".testaccount.dfs.core.windows.net",
                        " " + key.toUpperCase(Locale.ROOT) + " "
                    }) {
                Map<String, Object> values = sharedKeyConfig();
                Map<String, String> advanced = new HashMap<>();
                advanced.put(variant, "sentinel-sensitive-value");
                values.put("hadoop_adls_properties", advanced);
                FileConnectorException error =
                        Assertions.assertThrows(
                                FileConnectorException.class,
                                () -> ADLSConfigValidator.validate(ReadonlyConfig.fromMap(values)),
                                variant);
                Assertions.assertTrue(error.getMessage().contains("hadoop_adls_properties"));
                Assertions.assertTrue(error.getMessage().contains(variant));
                Assertions.assertFalse(error.getMessage().contains("sentinel-sensitive-value"));
            }
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
