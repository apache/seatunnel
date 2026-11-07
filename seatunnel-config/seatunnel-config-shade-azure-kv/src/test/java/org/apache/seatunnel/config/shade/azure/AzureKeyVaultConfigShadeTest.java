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

import org.junit.jupiter.api.Test;

import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;

import java.util.Collections;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class AzureKeyVaultConfigShadeTest {

    @Test
    void testProviderIsDiscoverable() {
        assertTrue(
                StreamSupport.stream(ServiceLoader.load(ConfigShade.class).spliterator(), false)
                        .anyMatch(configShade -> "azure-kv".equals(configShade.getIdentifier())));
    }

    @Test
    void testVaultUrlIsRequired() {
        AzureKeyVaultConfigShade configShade = new AzureKeyVaultConfigShade();

        assertThrows(
                IllegalArgumentException.class, () -> configShade.open(Collections.emptyMap()));
    }

    @Test
    void testNonSecretValuesPassThrough() {
        SecretClient secretClient = mock(SecretClient.class);
        AzureKeyVaultConfigShade configShade = new AzureKeyVaultConfigShade(secretClient);

        assertNull(configShade.decrypt(null));
        assertEquals("plain-value", configShade.decrypt("plain-value"));
        verifyNoInteractions(secretClient);
    }

    @Test
    void testSecretReferenceIsResolved() {
        SecretClient secretClient = mock(SecretClient.class);
        when(secretClient.getSecret("database-password"))
                .thenReturn(new KeyVaultSecret("database-password", "resolved-value"));
        AzureKeyVaultConfigShade configShade = new AzureKeyVaultConfigShade(secretClient);

        assertEquals("resolved-value", configShade.decrypt("${keyvault:azure:database-password}"));
        verify(secretClient).getSecret("database-password");
    }

    @Test
    void testFullSecretIdentifierIsRejected() {
        SecretClient secretClient = mock(SecretClient.class);
        AzureKeyVaultConfigShade configShade = new AzureKeyVaultConfigShade(secretClient);

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                configShade.decrypt(
                                        "${keyvault:azure:https://example.vault.azure.net/secrets/database-password/secret-version}"));

        assertEquals(
                "Only plain Azure Key Vault secret names are supported", exception.getMessage());
        verifyNoInteractions(secretClient);
    }
}
