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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
@ToString
public class SinkConfig implements Serializable {

    public enum StreamLoadFormat {
        CSV,
        JSON;
    }

    private List<String> nodeUrls;
    private String jdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    private String labelPrefix;
    private String columnSeparator;
    private StreamLoadFormat loadFormat;
    private int batchMaxSize;
    private long batchMaxBytes;

    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private boolean enableUpsertDelete;

    private String saveModeCreateTemplate;

    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private String customSql;

    private int httpSocketTimeout;

    @Getter private final Map<String, Object> streamLoadProps = new HashMap<>();

    public static SinkConfig of(ReadonlyConfig config) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setNodeUrls(config.get(StarRocksSinkOptions.NODE_URLS));
        sinkConfig.setDatabase(config.get(StarRocksSinkOptions.DATABASE));
        sinkConfig.setJdbcUrl(config.get(StarRocksSinkOptions.BASE_URL));
        config.getOptional(StarRocksSinkOptions.USERNAME).ifPresent(sinkConfig::setUsername);
        config.getOptional(StarRocksSinkOptions.PASSWORD).ifPresent(sinkConfig::setPassword);
        config.getOptional(StarRocksSinkOptions.TABLE).ifPresent(sinkConfig::setTable);
        config.getOptional(StarRocksSinkOptions.LABEL_PREFIX).ifPresent(sinkConfig::setLabelPrefix);
        sinkConfig.setBatchMaxSize(config.get(StarRocksSinkOptions.BATCH_MAX_SIZE));
        sinkConfig.setBatchMaxBytes(config.get(StarRocksSinkOptions.BATCH_MAX_BYTES));
        config.getOptional(StarRocksSinkOptions.MAX_RETRIES).ifPresent(sinkConfig::setMaxRetries);
        config.getOptional(StarRocksSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS)
                .ifPresent(sinkConfig::setRetryBackoffMultiplierMs);
        config.getOptional(StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS)
                .ifPresent(sinkConfig::setMaxRetryBackoffMs);
        config.getOptional(StarRocksSinkOptions.ENABLE_UPSERT_DELETE)
                .ifPresent(sinkConfig::setEnableUpsertDelete);
        sinkConfig.setSaveModeCreateTemplate(
                config.get(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
        config.getOptional(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .ifPresent(sinkConfig::setSaveModeCreateTemplate);
        config.getOptional(StarRocksSinkOptions.STARROCKS_CONFIG)
                .ifPresent(options -> sinkConfig.getStreamLoadProps().putAll(options));
        config.getOptional(StarRocksSinkOptions.COLUMN_SEPARATOR)
                .ifPresent(sinkConfig::setColumnSeparator);
        sinkConfig.setLoadFormat(config.get(StarRocksSinkOptions.LOAD_FORMAT));
        sinkConfig.setSchemaSaveMode(config.get(StarRocksSinkOptions.SCHEMA_SAVE_MODE));
        sinkConfig.setDataSaveMode(config.get(StarRocksSinkOptions.DATA_SAVE_MODE));
        sinkConfig.setCustomSql(config.get(StarRocksSinkOptions.CUSTOM_SQL));
        sinkConfig.setHttpSocketTimeout(config.get(StarRocksSinkOptions.HTTP_SOCKET_TIMEOUT_MS));
        return sinkConfig;
    }
}
