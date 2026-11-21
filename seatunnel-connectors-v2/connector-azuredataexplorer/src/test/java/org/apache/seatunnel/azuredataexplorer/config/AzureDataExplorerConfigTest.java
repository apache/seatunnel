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

public class AzureDataExplorerConfigTest {

    @Test
    public void testFromSinkConfigUsesDefaults() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("cluster_uri", "https://example.kusto.windows.net");
        configMap.put("database", "db");
        configMap.put("table", "table_a");
        configMap.put("client_id", "client");
        configMap.put("client_secret", "secret");
        configMap.put("tenant_id", "tenant");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        AzureDataExplorerConfig sinkConfig = AzureDataExplorerConfig.fromSinkConfig(config);

        Assertions.assertEquals("https://example.kusto.windows.net", sinkConfig.getClusterUri());
        Assertions.assertEquals("db", sinkConfig.getDatabase());
        Assertions.assertEquals("table_a", sinkConfig.getTable());
        Assertions.assertEquals("client", sinkConfig.getClientId());
        Assertions.assertEquals("secret", sinkConfig.getClientSecret());
        Assertions.assertEquals("tenant", sinkConfig.getTenantId());
        Assertions.assertEquals("", sinkConfig.getIngestionMappingReference());
        Assertions.assertEquals(
                AzureDataExplorerSinkOptions.IngestionType.QUEUED, sinkConfig.getIngestionType());
        Assertions.assertEquals(1000, sinkConfig.getBatchSize());
        Assertions.assertEquals(30_000L, sinkConfig.getFlushIntervalMs());
        Assertions.assertEquals(
                "https://ingest-example.kusto.windows.net", sinkConfig.getQueuedIngestUri());
    }

    @Test
    public void testQueuedIngestUriPreservesNonHttps() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("cluster_uri", "http://example");
        configMap.put("database", "db");
        configMap.put("table", "table_a");
        configMap.put("client_id", "client");
        configMap.put("client_secret", "secret");
        configMap.put("tenant_id", "tenant");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        AzureDataExplorerConfig sinkConfig = AzureDataExplorerConfig.fromSinkConfig(config);

        Assertions.assertEquals("http://example", sinkConfig.getQueuedIngestUri());
    }
}
