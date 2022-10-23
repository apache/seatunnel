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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
@ToString
public class SinkConfig {

    public static final String NODE_URLS = "nodeUrls";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String LABEL_PREFIX = "labelPrefix";
    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String STARROCKS_SINK_CONFIG_PREFIX = "sink.properties.";
    private static final String LOAD_FORMAT = "format";
    private static final StreamLoadFormat DEFAULT_LOAD_FORMAT = StreamLoadFormat.CSV;
    private static final String COLUMN_SEPARATOR = "column_separator";
    public static final String BATCH_MAX_SIZE = "batch_max_rows";
    public static final String BATCH_MAX_BYTES = "batch_max_bytes";
    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";
    public static final String MAX_RETRIES = "max_retries";
    public static final String RETRY_BACKOFF_MULTIPLIER_MS = "retry_backoff_multiplier_ms";
    public static final String MAX_RETRY_BACKOFF_MS = "max_retry_backoff_ms";

    public enum StreamLoadFormat {
        CSV, JSON;
        public static StreamLoadFormat parse(String format) {
            if (StreamLoadFormat.JSON.name().equals(format)) {
                return JSON;
            }
            return CSV;
        }
    }

    private List<String> nodeUrls;
    private String username;
    private String password;
    private String database;
    private String table;
    private String labelPrefix;
    private String columnSeparator;
    private StreamLoadFormat loadFormat = DEFAULT_LOAD_FORMAT;
    private static final int DEFAULT_BATCH_MAX_SIZE = 1024;
    private static final long DEFAULT_BATCH_BYTES = 5 * 1024 * 1024;

    private int batchMaxSize = DEFAULT_BATCH_MAX_SIZE;
    private long batchMaxBytes = DEFAULT_BATCH_BYTES;

    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;

    private final Map<String, Object> streamLoadProps = new HashMap<>();

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setNodeUrls(pluginConfig.getStringList(NODE_URLS));
        sinkConfig.setDatabase(pluginConfig.getString(DATABASE));
        sinkConfig.setTable(pluginConfig.getString(TABLE));

        if (pluginConfig.hasPath(USERNAME)) {
            sinkConfig.setUsername(pluginConfig.getString(USERNAME));
        }
        if (pluginConfig.hasPath(PASSWORD)) {
            sinkConfig.setPassword(pluginConfig.getString(PASSWORD));
        }
        if (pluginConfig.hasPath(LABEL_PREFIX)) {
            sinkConfig.setLabelPrefix(pluginConfig.getString(LABEL_PREFIX));
        }
        if (pluginConfig.hasPath(COLUMN_SEPARATOR)) {
            sinkConfig.setColumnSeparator(pluginConfig.getString(COLUMN_SEPARATOR));
        }
        if (pluginConfig.hasPath(BATCH_MAX_SIZE)) {
            sinkConfig.setBatchMaxSize(pluginConfig.getInt(BATCH_MAX_SIZE));
        }
        if (pluginConfig.hasPath(BATCH_MAX_BYTES)) {
            sinkConfig.setBatchMaxBytes(pluginConfig.getLong(BATCH_MAX_BYTES));
        }
        if (pluginConfig.hasPath(BATCH_INTERVAL_MS)) {
            sinkConfig.setBatchIntervalMs(pluginConfig.getInt(BATCH_INTERVAL_MS));
        }
        if (pluginConfig.hasPath(MAX_RETRIES)) {
            sinkConfig.setMaxRetries(pluginConfig.getInt(MAX_RETRIES));
        }
        if (pluginConfig.hasPath(RETRY_BACKOFF_MULTIPLIER_MS)) {
            sinkConfig.setRetryBackoffMultiplierMs(pluginConfig.getInt(RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (pluginConfig.hasPath(MAX_RETRY_BACKOFF_MS)) {
            sinkConfig.setMaxRetryBackoffMs(pluginConfig.getInt(MAX_RETRY_BACKOFF_MS));
        }
        parseSinkStreamLoadProperties(pluginConfig, sinkConfig);
        if (sinkConfig.streamLoadProps.containsKey(COLUMN_SEPARATOR)) {
            sinkConfig.setColumnSeparator((String) sinkConfig.streamLoadProps.get(COLUMN_SEPARATOR));
        }
        if (sinkConfig.streamLoadProps.containsKey(LOAD_FORMAT)) {
            sinkConfig.setLoadFormat(StreamLoadFormat.parse((String) sinkConfig.streamLoadProps.get(LOAD_FORMAT)));
        }
        return sinkConfig;
    }

    private static void parseSinkStreamLoadProperties(Config pluginConfig, SinkConfig sinkConfig) {
        Config starRocksConfig = TypesafeConfigUtils.extractSubConfig(pluginConfig,
                STARROCKS_SINK_CONFIG_PREFIX, false);
        starRocksConfig.entrySet().forEach(entry -> {
            final String configKey = entry.getKey().toLowerCase();
            sinkConfig.streamLoadProps.put(configKey, entry.getValue().unwrapped());
        });
    }
}
