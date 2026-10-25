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

package org.apache.seatunnel.connectors.seatunnel.deeplake.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.io.Serializable;

public class DeepLakeSinkConfig implements Serializable {

    private final String apiUrl;
    private final String apiKey;
    private final String orgId;
    private final String workspace;
    private final String table;
    private final int batchSize;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final SchemaSaveMode schemaSaveMode;

    public DeepLakeSinkConfig(ReadonlyConfig config, CatalogTable catalogTable) {
        this.apiUrl = removeTrailingSlash(config.get(DeepLakeSinkOptions.API_URL));
        this.apiKey = config.get(DeepLakeSinkOptions.API_KEY);
        this.orgId = config.get(DeepLakeSinkOptions.ORG_ID);
        this.workspace = config.get(DeepLakeSinkOptions.WORKSPACE);
        this.table =
                config.getOptional(DeepLakeSinkOptions.TABLE)
                        .orElse(catalogTable.getTableId().getTableName());
        this.batchSize = config.get(DeepLakeSinkOptions.BATCH_SIZE);
        this.connectTimeoutMs = config.get(DeepLakeSinkOptions.CONNECT_TIMEOUT_MS);
        this.socketTimeoutMs = config.get(DeepLakeSinkOptions.SOCKET_TIMEOUT_MS);
        this.schemaSaveMode = config.get(DeepLakeSinkOptions.SCHEMA_SAVE_MODE);

        requireNonBlank(apiUrl, "api_url");
        requireNonBlank(apiKey, "api_key");
        requireNonBlank(orgId, "org_id");
        requireNonBlank(workspace, "workspace");
        requireNonBlank(table, "table");
        requirePositive(batchSize, "batch_size");
        requirePositive(connectTimeoutMs, "connect_timeout_ms");
        requirePositive(socketTimeoutMs, "socket_timeout_ms");
        if (schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA) {
            throw new IllegalArgumentException(
                    "schema_save_mode RECREATE_SCHEMA is not supported by the DeepLake sink");
        }
    }

    private static String removeTrailingSlash(String value) {
        if (value == null) {
            return null;
        }
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }

    private static void requireNonBlank(String value, String option) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option + " must not be blank");
        }
    }

    private static void requirePositive(int value, String option) {
        if (value <= 0) {
            throw new IllegalArgumentException(option + " must be greater than zero");
        }
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getOrgId() {
        return orgId;
    }

    public String getWorkspace() {
        return workspace;
    }

    public String getTable() {
        return table;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }
}
