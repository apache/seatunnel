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

package org.apache.seatunnel.connectors.seatunnel.iotdb.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@ToString
public class SinkConfig extends CommonConfig {

    public static final String BATCH_SIZE = "batch_size";
    public static final String BATCH_INTERVAL_MS = "batch_interval_ms";
    public static final String MAX_RETRIES = "max_retries";
    public static final String RETRY_BACKOFF_MULTIPLIER_MS = "retry_backoff_multiplier_ms";
    public static final String MAX_RETRY_BACKOFF_MS = "max_retry_backoff_ms";
    public static final String DEFAULT_THRIFT_BUFFER_SIZE = "default_thrift_buffer_size";
    public static final String MAX_THRIFT_FRAME_SIZE = "max_thrift_frame_size";
    public static final String ZONE_ID = "zone_id";
    public static final String ENABLE_RPC_COMPRESSION = "enable_rpc_compression";
    public static final String CONNECTION_TIMEOUT_IN_MS = "connection_timeout_in_ms";
    public static final String TIMESERIES_OPTIONS = "timeseries_options";
    public static final String TIMESERIES_OPTION_PATH = "path";
    public static final String TIMESERIES_OPTION_DATA_TYPE = "data_type";

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private Integer thriftDefaultBufferSize;
    private Integer thriftMaxFrameSize;
    private ZoneId zoneId;
    private Boolean enableRPCCompression;
    private Integer connectionTimeoutInMs;
    private List<TimeseriesOption> timeseriesOptions;

    public SinkConfig(@NonNull List<String> nodeUrls,
                      @NonNull String username,
                      @NonNull String password) {
        super(nodeUrls, username, password);
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig(
                pluginConfig.getStringList(NODE_URLS),
                pluginConfig.getString(USERNAME),
                pluginConfig.getString(PASSWORD));
        if (pluginConfig.hasPath(BATCH_SIZE)) {
            int batchSize = checkIntArgument(pluginConfig.getInt(BATCH_SIZE));
            sinkConfig.setBatchSize(batchSize);
        }
        if (pluginConfig.hasPath(BATCH_INTERVAL_MS)) {
            int batchIntervalMs = checkIntArgument(pluginConfig.getInt(BATCH_INTERVAL_MS));
            sinkConfig.setBatchIntervalMs(batchIntervalMs);
        }
        if (pluginConfig.hasPath(MAX_RETRIES)) {
            int maxRetries = checkIntArgument(pluginConfig.getInt(MAX_RETRIES));
            sinkConfig.setMaxRetries(maxRetries);
        }
        if (pluginConfig.hasPath(RETRY_BACKOFF_MULTIPLIER_MS)) {
            int retryBackoffMultiplierMs = checkIntArgument(pluginConfig.getInt(RETRY_BACKOFF_MULTIPLIER_MS));
            sinkConfig.setRetryBackoffMultiplierMs(retryBackoffMultiplierMs);
        }
        if (pluginConfig.hasPath(MAX_RETRY_BACKOFF_MS)) {
            int maxRetryBackoffMs = checkIntArgument(pluginConfig.getInt(MAX_RETRY_BACKOFF_MS));
            sinkConfig.setMaxRetryBackoffMs(maxRetryBackoffMs);
        }
        if (pluginConfig.hasPath(DEFAULT_THRIFT_BUFFER_SIZE)) {
            int thriftDefaultBufferSize = checkIntArgument(pluginConfig.getInt(DEFAULT_THRIFT_BUFFER_SIZE));
            sinkConfig.setThriftDefaultBufferSize(thriftDefaultBufferSize);
        }
        if (pluginConfig.hasPath(MAX_THRIFT_FRAME_SIZE)) {
            int thriftMaxFrameSize = checkIntArgument(pluginConfig.getInt(MAX_THRIFT_FRAME_SIZE));
            sinkConfig.setThriftMaxFrameSize(thriftMaxFrameSize);
        }
        if (pluginConfig.hasPath(ZONE_ID)) {
            sinkConfig.setZoneId(ZoneId.of(pluginConfig.getString(ZONE_ID)));
        }
        if (pluginConfig.hasPath(ENABLE_RPC_COMPRESSION)) {
            sinkConfig.setEnableRPCCompression(pluginConfig.getBoolean(ENABLE_RPC_COMPRESSION));
        }
        if (pluginConfig.hasPath(CONNECTION_TIMEOUT_IN_MS)) {
            int connectionTimeoutInMs = checkIntArgument(pluginConfig.getInt(CONNECTION_TIMEOUT_IN_MS));
            checkNotNull(sinkConfig.getEnableRPCCompression());
            sinkConfig.setConnectionTimeoutInMs(connectionTimeoutInMs);
        }
        if (pluginConfig.hasPath(TIMESERIES_OPTIONS)) {
            List<? extends Config> timeseriesConfigs = pluginConfig.getConfigList(TIMESERIES_OPTIONS);
            List<TimeseriesOption> timeseriesOptions = new ArrayList<>(timeseriesConfigs.size());
            for (Config timeseriesConfig : timeseriesConfigs) {
                String timeseriesPath = timeseriesConfig.getString(TIMESERIES_OPTION_PATH);
                String timeseriesDataType = timeseriesConfig.getString(TIMESERIES_OPTION_DATA_TYPE);
                TimeseriesOption timeseriesOption = new TimeseriesOption(
                        timeseriesPath, TSDataType.valueOf(timeseriesDataType));
                timeseriesOptions.add(timeseriesOption);
            }
            sinkConfig.setTimeseriesOptions(timeseriesOptions);
        }
        return sinkConfig;
    }

    private static int checkIntArgument(int args) {
        checkArgument(args > 0);
        return args;
    }

    @Getter
    @ToString
    @AllArgsConstructor
    public static class TimeseriesOption implements Serializable {
        private String path;
        private TSDataType dataType = TSDataType.TEXT;
    }
}
