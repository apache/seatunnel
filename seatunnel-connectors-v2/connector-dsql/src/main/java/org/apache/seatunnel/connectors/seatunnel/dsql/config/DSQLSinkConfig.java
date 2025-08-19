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

package org.apache.seatunnel.connectors.seatunnel.dsql.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DSQLSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String clusterEndpoint;
    private final String databaseName;
    private final String tableName;
    private final String awsRegion;
    private final String accessKeyId;
    private final String secretAccessKey;

    private final int batchSize;
    private final int maxRetries;
    private final long retryDelayMs;
    private final boolean createTableIfNotExists;

    private final String userName;
    // New fields for enhanced options

    private final int connectionTimeoutMs;
    private final int socketTimeoutMs;

    private final List<String> primaryKeys;

    private final boolean useSsl;
    private final String profileName;
    private final boolean enableMultiTable;

    private final Map<String, String> tableMapping;

    public DSQLSinkConfig(ReadonlyConfig config) {
        // Basic configuration
        this.clusterEndpoint = config.get(DSQLSinkOptions.CLUSTER_ENDPOINT);
        this.databaseName = config.get(DSQLSinkOptions.DATABASE_NAME);
        this.tableName = config.get(DSQLSinkOptions.TABLE_NAME);
        this.awsRegion = config.get(DSQLSinkOptions.AWS_REGION);
        this.accessKeyId = config.getOptional(DSQLSinkOptions.ACCESS_KEY_ID).orElse(null);
        this.secretAccessKey = config.getOptional(DSQLSinkOptions.SECRET_ACCESS_KEY).orElse(null);

        this.batchSize = config.get(DSQLSinkOptions.BATCH_SIZE);
        this.maxRetries = config.get(DSQLSinkOptions.MAX_RETRIES);
        this.retryDelayMs = config.get(DSQLSinkOptions.RETRY_DELAY_MS);
        this.createTableIfNotExists = config.get(DSQLSinkOptions.CREATE_TABLE_IF_NOT_EXISTS);

        // Enhanced configuration

        this.connectionTimeoutMs = config.get(DSQLSinkOptions.CONNECTION_TIMEOUT_MS);
        this.socketTimeoutMs = config.get(DSQLSinkOptions.SOCKET_TIMEOUT_MS);

        this.primaryKeys = config.getOptional(DSQLSinkOptions.PRIMARY_KEYS).orElse(null);

        this.useSsl = config.get(DSQLSinkOptions.USE_SSL);
        this.profileName = config.getOptional(DSQLSinkOptions.PROFILE_NAME).orElse(null);
        this.userName = config.get(DSQLSinkOptions.USER_NAME);
        this.enableMultiTable = config.get(DSQLSinkOptions.ENABLE_MULTI_TABLE);
        this.tableMapping = config.getOptional(DSQLSinkOptions.TABLE_MAPPING).orElse(null);
        // Validate configuration
        validate();
    }

    private void validate() {
        // Check required fields
        if (clusterEndpoint == null || clusterEndpoint.isEmpty()) {
            throw new IllegalArgumentException("cluster_endpoint must be specified");
        }

        if (databaseName == null || databaseName.isEmpty()) {
            throw new IllegalArgumentException("database_name must be specified");
        }

        if (awsRegion == null || awsRegion.isEmpty()) {
            throw new IllegalArgumentException("region must be specified");
        }
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("primary_keys must be specified");
        }

        // Check authentication options
        boolean hasDirectCredentials =
                accessKeyId != null
                        && !accessKeyId.isEmpty()
                        && secretAccessKey != null
                        && !secretAccessKey.isEmpty();
        boolean hasProfileCredentials = profileName != null && !profileName.isEmpty();

        if (!hasDirectCredentials && !hasProfileCredentials) {
            throw new IllegalArgumentException(
                    "Either access_key_id/secret_access_key or profile_name must be specified");
        }

        // Validate batch size
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch_size must be greater than 0");
        }

        // Validate retry parameters
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max_retries must be greater than or equal to 0");
        }

        if (retryDelayMs <= 0) {
            throw new IllegalArgumentException("retry_delay_ms must be greater than 0");
        }
    }

    // Basic getters
    public String getClusterEndpoint() {
        return clusterEndpoint;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public boolean isCreateTableIfNotExists() {
        return createTableIfNotExists;
    }

    // Enhanced getters

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public String getProfileName() {
        return profileName;
    }

    public String getUserName() {
        return userName;
    }

    // Multi-table getters
    public boolean isEnableMultiTable() {
        return enableMultiTable;
    }

    public Map<String, String> getTableMapping() {
        return tableMapping;
    }
}
