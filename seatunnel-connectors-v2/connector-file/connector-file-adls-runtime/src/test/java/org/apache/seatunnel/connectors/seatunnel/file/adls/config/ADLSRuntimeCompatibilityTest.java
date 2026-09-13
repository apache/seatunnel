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

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ADLSRuntimeCompatibilityTest {
    @Test
    void validatesSecureAbfsDriverAndBaseConfiguration() {
        ADLSRuntimeCompatibility.validateDriverAvailable();
        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration("examplestorage", "analytics");

        Assertions.assertEquals(
                "abfss://analytics@examplestorage.dfs.core.windows.net",
                configuration.get("fs.defaultFS"));
        Assertions.assertEquals(
                ADLSRuntimeCompatibility.SECURE_ABFS_IMPLEMENTATION,
                configuration.get("fs.abfss.impl"));
        Assertions.assertTrue(configuration.getBoolean("fs.abfss.impl.disable.cache", false));
    }

    @Test
    void configuresAccountScopedSharedKeyWithoutChangingTheBaseUri() {
        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration("examplestorage", "analytics");
        ADLSRuntimeCompatibility.configureSharedKey(
                configuration, "examplestorage", "sentinel-key");

        Assertions.assertEquals(
                "SharedKey",
                configuration.get(
                        "fs.azure.account.auth.type.examplestorage.dfs.core.windows.net"));
        Assertions.assertEquals(
                "sentinel-key",
                configuration.get("fs.azure.account.key.examplestorage.dfs.core.windows.net"));
        Assertions.assertEquals(
                "abfss://analytics@examplestorage.dfs.core.windows.net",
                configuration.get("fs.defaultFS"));
    }

    @Test
    void configuresAccountScopedOAuthClientCredentials() {
        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration("examplestorage", "analytics");
        ADLSRuntimeCompatibility.configureClientCredentials(
                configuration, "examplestorage", "tenant", "client", "secret");

        Assertions.assertEquals(
                "OAuth",
                configuration.get(
                        "fs.azure.account.auth.type.examplestorage.dfs.core.windows.net"));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                configuration.get(
                        "fs.azure.account.oauth.provider.type.examplestorage.dfs.core.windows.net"));
        Assertions.assertEquals(
                "https://login.microsoftonline.com/tenant/oauth2/token",
                configuration.get(
                        "fs.azure.account.oauth2.client.endpoint.examplestorage.dfs.core.windows.net"));
    }

    @Test
    void rejectsInvalidStorageLabelsAndMissingCredentials() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ADLSRuntimeCompatibility.newConfiguration("Storage", "analytics"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        ADLSRuntimeCompatibility.configureSharedKey(
                                ADLSRuntimeCompatibility.newConfiguration("account", "container"),
                                "account",
                                ""));
    }
}
