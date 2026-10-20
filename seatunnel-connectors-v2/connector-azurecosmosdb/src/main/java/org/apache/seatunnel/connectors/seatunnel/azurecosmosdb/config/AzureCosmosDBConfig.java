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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AzureCosmosDBConfig implements Serializable {

    private final String uri;
    private final String endpoint;
    private final String key;
    private final String primaryKey;
    private final String secondaryKey;
    private final String primaryConnectionString;
    private final String secondaryConnectionString;
    private final String database;
    private final String container;
    private final String query;
    private final int maxItemCount;
    private final Config schema;

    public AzureCosmosDBConfig(ReadonlyConfig config) {
        this.uri = config.getOptional(AzureCosmosDBSourceOptions.URI).orElse(null);
        this.endpoint = config.getOptional(AzureCosmosDBSourceOptions.ENDPOINT).orElse(null);
        this.key = config.getOptional(AzureCosmosDBSourceOptions.KEY).orElse(null);
        this.primaryKey = config.getOptional(AzureCosmosDBSourceOptions.PRIMARY_KEY).orElse(null);
        this.secondaryKey =
                config.getOptional(AzureCosmosDBSourceOptions.SECONDARY_KEY).orElse(null);
        this.primaryConnectionString =
                config.getOptional(AzureCosmosDBSourceOptions.PRIMARY_CONNECTION_STRING)
                        .orElse(null);
        this.secondaryConnectionString =
                config.getOptional(AzureCosmosDBSourceOptions.SECONDARY_CONNECTION_STRING)
                        .orElse(null);
        this.database = config.get(AzureCosmosDBSourceOptions.DATABASE);
        this.container = config.get(AzureCosmosDBSourceOptions.CONTAINER);
        this.query = config.get(AzureCosmosDBSourceOptions.QUERY);
        this.maxItemCount = config.get(AzureCosmosDBSourceOptions.MAX_ITEM_COUNT);
        this.schema =
                config.getOptional(ConnectorCommonOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .map(ReadonlyConfig::toConfig)
                        .orElse(null);

        if (getResolvedEndpoint() == null) {
            throw new IllegalArgumentException(
                    "AzureCosmosDB requires uri, endpoint, or connection string to resolve the endpoint");
        }
        if (getResolvedKey() == null) {
            throw new IllegalArgumentException(
                    "AzureCosmosDB requires key, primary_key, secondary_key, or a connection string to resolve the key");
        }
    }

    public String getResolvedEndpoint() {
        String resolvedEndpoint = firstNonBlank(uri, endpoint);
        if (resolvedEndpoint != null) {
            return resolvedEndpoint;
        }

        return firstNonBlank(
                parseConnectionString(primaryConnectionString).get("endpoint"),
                parseConnectionString(secondaryConnectionString).get("endpoint"));
    }

    public String getResolvedKey() {
        String resolvedKey = firstNonBlank(key, primaryKey, secondaryKey);
        if (resolvedKey != null) {
            return resolvedKey;
        }

        return firstNonBlank(
                parseConnectionString(primaryConnectionString).get("key"),
                parseConnectionString(secondaryConnectionString).get("key"));
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static Map<String, String> parseConnectionString(String connectionString) {
        Map<String, String> values = new HashMap<>();
        if (connectionString == null || connectionString.trim().isEmpty()) {
            return values;
        }
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int equalsIndex = trimmed.indexOf('=');
            if (equalsIndex <= 0 || equalsIndex == trimmed.length() - 1) {
                continue;
            }
            String key = trimmed.substring(0, equalsIndex).trim().toLowerCase();
            String value = trimmed.substring(equalsIndex + 1).trim();
            if ("accountendpoint".equals(key)) {
                values.put("endpoint", value);
            } else if ("accountkey".equals(key)) {
                values.put("key", value);
            }
        }
        return values;
    }

    public String getUri() {
        return uri;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getKey() {
        return key;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getSecondaryKey() {
        return secondaryKey;
    }

    public String getPrimaryConnectionString() {
        return primaryConnectionString;
    }

    public String getSecondaryConnectionString() {
        return secondaryConnectionString;
    }

    public String getDatabase() {
        return database;
    }

    public String getContainer() {
        return container;
    }

    public String getQuery() {
        return query;
    }

    public int getMaxItemCount() {
        return maxItemCount;
    }

    public Config getSchema() {
        return schema;
    }
}
