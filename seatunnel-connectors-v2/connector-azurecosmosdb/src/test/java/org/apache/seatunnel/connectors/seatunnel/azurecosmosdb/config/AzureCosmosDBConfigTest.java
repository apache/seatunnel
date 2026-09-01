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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureCosmosDBConfigTest {

    @Test
    public void testResolveFromUriAndPrimaryKey() {
        AzureCosmosDBConfig config =
                new AzureCosmosDBConfig(ReadonlyConfig.fromMap(buildBaseOptions()));

        Assertions.assertEquals(
                "https://account.documents.azure.com:443/", config.getResolvedEndpoint());
        Assertions.assertEquals("primary-key", config.getResolvedKey());
    }

    @Test
    public void testResolveFromConnectionString() {
        Map<String, Object> options = buildBaseOptions();
        options.remove("uri");
        options.remove("primary_key");
        options.put(
                "primary_connection_string",
                "AccountEndpoint=https://primary.documents.azure.com:443/;AccountKey=primary-connection-key;");

        AzureCosmosDBConfig config = new AzureCosmosDBConfig(ReadonlyConfig.fromMap(options));

        Assertions.assertEquals(
                "https://primary.documents.azure.com:443/", config.getResolvedEndpoint());
        Assertions.assertEquals("primary-connection-key", config.getResolvedKey());
    }

    private Map<String, Object> buildBaseOptions() {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put("uri", "https://account.documents.azure.com:443/");
        options.put("primary_key", "primary-key");
        options.put("database", "test-db");
        options.put("container", "test-container");
        options.put("query", "SELECT * FROM c");
        options.put("schema", schema);
        return options;
    }
}
