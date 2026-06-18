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

package org.apache.seatunnel.config.shade.azure;

import org.apache.seatunnel.api.configuration.ConfigShade;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

import java.util.Map;

public class AzureKeyVaultConfigShade implements ConfigShade {

    private static final String IDENTIFIER = "azure-kv";
    private static final String SECRET_REFERENCE_PREFIX = "${keyvault:azure:";

    private SecretClient secretClient;

    public AzureKeyVaultConfigShade() {}

    AzureKeyVaultConfigShade(SecretClient secretClient) {
        this.secretClient = secretClient;
    }

    @Override
    public void open(Map<String, Object> props) {
        Object vaultUrl = props.get("vault.url");

        if (vaultUrl == null || vaultUrl.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Missing vault.url");
        }

        this.secretClient =
                new SecretClientBuilder()
                        .vaultUrl(vaultUrl.toString())
                        .credential(new DefaultAzureCredentialBuilder().build())
                        .buildClient();
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public String encrypt(String content) {
        return content;
    }

    @Override
    public String decrypt(String content) {
        if (content == null) {
            return null;
        }

        if (content.startsWith(SECRET_REFERENCE_PREFIX) && content.endsWith("}")) {
            String secretName =
                    content.substring(SECRET_REFERENCE_PREFIX.length(), content.length() - 1);

            if (secretName.contains("/")) {
                throw new IllegalArgumentException(
                        "Only plain Azure Key Vault secret names are supported");
            }

            return secretClient.getSecret(secretName).getValue();
        }

        return content;
    }
}
