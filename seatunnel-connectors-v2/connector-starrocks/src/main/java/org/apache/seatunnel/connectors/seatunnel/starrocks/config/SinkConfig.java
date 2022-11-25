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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
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

    private static final int DEFAULT_BATCH_MAX_SIZE = 1024;
    private static final long DEFAULT_BATCH_BYTES = 5 * 1024 * 1024;

    private static final String LOAD_FORMAT = "format";
    private static final StreamLoadFormat DEFAULT_LOAD_FORMAT = StreamLoadFormat.CSV;
    private static final String COLUMN_SEPARATOR = "column_separator";

    public static final Option<List<String>> NODE_URLS = Options.key("nodeUrls")
            .listType()
            .noDefaultValue()
            .withDescription("StarRocks cluster address, the format is [\"fe_ip:fe_http_port\", ...]");

    public static final Option<String> USERNAME = Options.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("StarRocks user username");

    public static final Option<String> PASSWORD = Options.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("StarRocks user password");

    public static final Option<String> LABEL_PREFIX = Options.key("labelPrefix")
            .stringType()
            .noDefaultValue()
            .withDescription("The prefix of StarRocks stream load label");

    public static final Option<String> DATABASE = Options.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of StarRocks database");

    public static final Option<String> TABLE = Options.key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of StarRocks table");

    public static final Option<String> STARROCKS_SINK_CONFIG_PREFIX = Options.key("sink.properties.")
            .stringType()
            .noDefaultValue()
            .withDescription("The parameter of the stream load data_desc. " +
                    "The way to specify the parameter is to add the prefix `sink.properties.` to the original stream load parameter name ");

    public static final Option<Integer> BATCH_MAX_SIZE = Options.key("batch_max_rows")
            .intType()
            .defaultValue(DEFAULT_BATCH_MAX_SIZE)
            .withDescription("For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    public static final Option<Long> BATCH_MAX_BYTES = Options.key("batch_max_bytes")
            .longType()
            .defaultValue(DEFAULT_BATCH_BYTES)
            .withDescription("For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    public static final Option<Integer> BATCH_INTERVAL_MS = Options.key("batch_interval_ms")
            .intType()
            .noDefaultValue()
            .withDescription("For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    public static final Option<Integer> MAX_RETRIES = Options.key("max_retries")
            .intType()
            .noDefaultValue()
            .withDescription("The number of retries to flush failed");

    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS = Options.key("retry_backoff_multiplier_ms")
            .intType()
            .noDefaultValue()
            .withDescription("Using as a multiplier for generating the next delay for backoff");

    public static final Option<Integer> MAX_RETRY_BACKOFF_MS = Options.key("max_retry_backoff_ms")
            .intType()
            .noDefaultValue()
            .withDescription("The amount of time to wait before attempting to retry a request to StarRocks");

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

    private int batchMaxSize = DEFAULT_BATCH_MAX_SIZE;
    private long batchMaxBytes = DEFAULT_BATCH_BYTES;

    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;

    private final Map<String, Object> streamLoadProps = new HashMap<>();

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setNodeUrls(pluginConfig.getStringList(NODE_URLS.key()));
        sinkConfig.setDatabase(pluginConfig.getString(DATABASE.key()));
        sinkConfig.setTable(pluginConfig.getString(TABLE.key()));

        if (pluginConfig.hasPath(USERNAME.key())) {
            sinkConfig.setUsername(pluginConfig.getString(USERNAME.key()));
        }
        if (pluginConfig.hasPath(PASSWORD.key())) {
            sinkConfig.setPassword(pluginConfig.getString(PASSWORD.key()));
        }
        if (pluginConfig.hasPath(LABEL_PREFIX.key())) {
            sinkConfig.setLabelPrefix(pluginConfig.getString(LABEL_PREFIX.key()));
        }
        if (pluginConfig.hasPath(BATCH_MAX_SIZE.key())) {
            sinkConfig.setBatchMaxSize(pluginConfig.getInt(BATCH_MAX_SIZE.key()));
        }
        if (pluginConfig.hasPath(BATCH_MAX_BYTES.key())) {
            sinkConfig.setBatchMaxBytes(pluginConfig.getLong(BATCH_MAX_BYTES.key()));
        }
        if (pluginConfig.hasPath(BATCH_INTERVAL_MS.key())) {
            sinkConfig.setBatchIntervalMs(pluginConfig.getInt(BATCH_INTERVAL_MS.key()));
        }
        if (pluginConfig.hasPath(MAX_RETRIES.key())) {
            sinkConfig.setMaxRetries(pluginConfig.getInt(MAX_RETRIES.key()));
        }
        if (pluginConfig.hasPath(RETRY_BACKOFF_MULTIPLIER_MS.key())) {
            sinkConfig.setRetryBackoffMultiplierMs(pluginConfig.getInt(RETRY_BACKOFF_MULTIPLIER_MS.key()));
        }
        if (pluginConfig.hasPath(MAX_RETRY_BACKOFF_MS.key())) {
            sinkConfig.setMaxRetryBackoffMs(pluginConfig.getInt(MAX_RETRY_BACKOFF_MS.key()));
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
                STARROCKS_SINK_CONFIG_PREFIX.key(), false);
        starRocksConfig.entrySet().forEach(entry -> {
            final String configKey = entry.getKey().toLowerCase();
            sinkConfig.streamLoadProps.put(configKey, entry.getValue().unwrapped());
        });
    }
}
