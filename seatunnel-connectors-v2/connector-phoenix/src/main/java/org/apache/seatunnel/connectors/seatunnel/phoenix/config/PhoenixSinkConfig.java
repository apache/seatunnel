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

package org.apache.seatunnel.connectors.seatunnel.phoenix.config;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.phoenix.constant.Constant;
import org.apache.seatunnel.connectors.seatunnel.phoenix.constant.NullModeType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PhoenixSinkConfig {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSinkConfig.class);

    public static final String CONNECT_URL = "connect_url";
    public static final String JDBC_USER = "user";
    public static final String JDBC_PASSWORD = "password";
    public static final String NULL_MODE = "nullMode";
    public static final String TRUNCATE = "is_truncate_table";
    public static final String THIN_CLIENT_URL_PREFIX = "jdbc:phoenix:thin";
    public static final String TABLE = "sink_table";
    public static final String COLUMN = "sink_columns";
    public static final String RETRY_BACKOFF_MULTIPLIER_MS = "retry_backoff_multiplier_ms";
    public static final String CONNECTION_CHECK_TIMEOUT_SEC = "connection_check_timeout_sec";
    public static final String MAX_RETRIES = "max_retries";
    public static final String BATCH_SIZE = "batch_size";
    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";
    public static final String MAX_RETRY_BACKOFF_MS = "max_retry_backoff_ms";
    public int maxRetries = DEFAULT_MAX_RETRIES;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_BATCH_SIZE = 256;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 1000;
    private static final int DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC = 30;
    private static final int DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS = 500;
    private static final int DEFAULT_MAX_RETRY_BACKOFF_MS = 3000;

    public int batchSize = DEFAULT_BATCH_SIZE;
    public int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;
    public int connectionCheckTimeoutSeconds = DEFAULT_CONNECTION_CHECK_TIMEOUT_SEC;
    private int retryBackoffMultiplierMs = DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS;
    private int maxRetryBackoffMs = DEFAULT_MAX_RETRY_BACKOFF_MS;
    private String connectionString;
    private String username;
    private String password;
    private String tableName;

    private NullModeType nullMode = NullModeType.getByTypeName(Constant.DEFAULT_NULL_MODE);
    private boolean truncate = false;
    private boolean isThinClient = false;
    private List<String> columns;
    private List<Integer> sinkColumnsIndexInRow;

    private SeaTunnelRowType seaTunnelRowType;

    public String getConnectionString() {
        return connectionString;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public NullModeType getNullMode() {
        return nullMode;
    }

    public boolean isThinClient() {
        return isThinClient;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public int getRetryBackoffMultiplierMs() {
        return retryBackoffMultiplierMs;
    }

    public int getMaxRetryBackoffMs() {
        return maxRetryBackoffMs;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }

    public List<Integer> getSinkColumnsIndexInRow() {
        return sinkColumnsIndexInRow;
    }

    public static PhoenixSinkConfig parse(Config config, SeaTunnelRowType seaTunnelRowType) {
        PhoenixSinkConfig phoenixWriteConfig = new PhoenixSinkConfig();
        phoenixWriteConfig.seaTunnelRowType = seaTunnelRowType;

        if (config.hasPath(NULL_MODE)) {
            phoenixWriteConfig.nullMode = NullModeType.getByTypeName(config.getString(NULL_MODE));
        }
        if (config.hasPath(TRUNCATE)) {
            phoenixWriteConfig.truncate = config.getBoolean(TRUNCATE);
        }

        if (config.hasPath(MAX_RETRIES)) {
            phoenixWriteConfig.maxRetries = config.getInt(MAX_RETRIES);
        }

        if (config.hasPath(BATCH_SIZE)) {
            phoenixWriteConfig.batchSize = config.getInt(BATCH_SIZE);
        }
        if (config.hasPath(BATCH_INTERVAL_MS)) {
            phoenixWriteConfig.batchIntervalMs = config.getInt(BATCH_INTERVAL_MS);
        }
        if (config.hasPath(CONNECTION_CHECK_TIMEOUT_SEC)) {
            phoenixWriteConfig.connectionCheckTimeoutSeconds = config.getInt(CONNECTION_CHECK_TIMEOUT_SEC);
        }

        if (config.hasPath(RETRY_BACKOFF_MULTIPLIER_MS)) {
            int retryBackoffMultiplierMs = config.getInt(RETRY_BACKOFF_MULTIPLIER_MS);
            phoenixWriteConfig.retryBackoffMultiplierMs = retryBackoffMultiplierMs;
        }
        if (config.hasPath(MAX_RETRY_BACKOFF_MS)) {
            phoenixWriteConfig.maxRetryBackoffMs = config.getInt(MAX_RETRY_BACKOFF_MS);
        }

        parseClusterConfig(phoenixWriteConfig, config);

        parseTableConfig(phoenixWriteConfig, config);
        return phoenixWriteConfig;
    }

    private static void parseClusterConfig(PhoenixSinkConfig phoenixWriteConfig, Config config) {
        String thinConnectStr = config.getString(CONNECT_URL);
        phoenixWriteConfig.connectionString = thinConnectStr;

        if (thinConnectStr.contains(THIN_CLIENT_URL_PREFIX)) {
            phoenixWriteConfig.isThinClient = true;
        }
        if (phoenixWriteConfig.isThinClient) {
            if (config.hasPath(JDBC_USER)) {
                phoenixWriteConfig.username = config.getString(JDBC_USER);
            }
            if (config.hasPath(JDBC_PASSWORD)) {
                phoenixWriteConfig.password = config.getString(JDBC_PASSWORD);
            }
        }

    }

    private static void parseTableConfig(PhoenixSinkConfig phoenixWriteConfig, Config config) {
        phoenixWriteConfig.tableName = config.getString(TABLE);
        try {
            TableName tn = TableName.valueOf(phoenixWriteConfig.tableName);
        } catch (Exception e) {
            throw new RuntimeException("config tableName: " + phoenixWriteConfig.tableName + " Contains illegal characters");
        }
        SeaTunnelRowType seaTunnelRowTypeInfo = phoenixWriteConfig.getSeaTunnelRowType();

        if (config.hasPath(COLUMN) && !CollectionUtils.isEmpty(config.getStringList(COLUMN))) {
            phoenixWriteConfig.columns = config.getStringList(COLUMN);
        }
        // if the config sink_columns is empty, all fields in SeaTunnelRowTypeInfo will being write
        if (CollectionUtils.isEmpty(phoenixWriteConfig.columns)) {
            phoenixWriteConfig.columns = Arrays.asList(seaTunnelRowTypeInfo.getFieldNames());
        }
        Map<String, Integer> columnsMap = new HashMap<>(seaTunnelRowTypeInfo.getFieldNames().length);
        String[] fieldNames = seaTunnelRowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            columnsMap.put(fieldNames[i], i);
        }
        // init sink column index and partition field index, we will use the column index to found the data in SeaTunnelRow
        phoenixWriteConfig.sinkColumnsIndexInRow = phoenixWriteConfig.columns.stream()
                .map(columnName -> columnsMap.get(columnName))
                .collect(Collectors.toList());
    }
}
