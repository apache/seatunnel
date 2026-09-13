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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ADLSHadoopConfTest {

    @Test
    void configuresSharedKeyForSecureAbfs() {
        Map<String, Object> values = baseConfig();
        values.put("account_key", "secret-key");

        ADLSHadoopConf conf =
                ADLSHadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(values));

        Assertions.assertEquals(
                "abfss://files@testaccount.dfs.core.windows.net", conf.getHdfsNameKey());
        Assertions.assertEquals("abfss", conf.getSchema());
        Assertions.assertEquals(
                "SharedKey",
                conf.getExtraOptions()
                        .get("fs.azure.account.auth.type.testaccount.dfs.core.windows.net"));
        Assertions.assertEquals(
                "secret-key",
                conf.getExtraOptions()
                        .get("fs.azure.account.key.testaccount.dfs.core.windows.net"));
    }

    @Test
    void configuresOAuthClientCredentials() {
        Map<String, Object> values = baseConfig();
        values.put("auth_type", "OAUTH_CLIENT_CREDENTIALS");
        values.put("tenant_id", "tenant");
        values.put("client_id", "client");
        values.put("client_secret", "secret");

        ADLSHadoopConf conf =
                ADLSHadoopConf.buildWithReadOnlyConfig(ReadonlyConfig.fromMap(values));
        String account = "testaccount.dfs.core.windows.net";

        Assertions.assertEquals(
                "OAuth", conf.getExtraOptions().get("fs.azure.account.auth.type." + account));
        Assertions.assertEquals(
                "https://login.microsoftonline.com/tenant/oauth2/token",
                conf.getExtraOptions().get("fs.azure.account.oauth2.client.endpoint." + account));
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("account_name", "testaccount");
        values.put("container", "files");
        return values;
    }
}
