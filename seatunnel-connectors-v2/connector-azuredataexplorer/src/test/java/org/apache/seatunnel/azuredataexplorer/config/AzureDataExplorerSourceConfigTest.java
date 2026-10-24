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

package org.apache.seatunnel.azuredataexplorer.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureDataExplorerSourceConfigTest {

    @Test
    public void testFromSourceConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("cluster_uri", "https://example.kusto.windows.net");
        configMap.put("database", "db");
        configMap.put("query", "MyTable | take 10");
        configMap.put("client_id", "client");
        configMap.put("client_secret", "secret");
        configMap.put("tenant_id", "tenant");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        AzureDataExplorerSourceConfig sourceConfig =
                AzureDataExplorerSourceConfig.fromSourceConfig(config);

        Assertions.assertEquals("https://example.kusto.windows.net", sourceConfig.getClusterUri());
        Assertions.assertEquals("db", sourceConfig.getDatabase());
        Assertions.assertEquals("MyTable | take 10", sourceConfig.getQuery());
        Assertions.assertEquals("client", sourceConfig.getClientId());
        Assertions.assertEquals("secret", sourceConfig.getClientSecret());
        Assertions.assertEquals("tenant", sourceConfig.getTenantId());
    }
}
