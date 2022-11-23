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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.time.ZoneId;
import java.util.List;

@Setter
@Getter
@ToString
public class SinkConfig extends CommonConfig {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    public static final Option<String> KEY_TIMESTAMP = Options.key("key_timestamp").stringType().noDefaultValue().withDescription("key timestamp");
    public static final Option<String> KEY_DEVICE = Options.key("key_device").stringType().noDefaultValue().withDescription("key device");
    public static final Option<List<String>> KEY_MEASUREMENT_FIELDS = Options.key("key_measurement_fields").listType().noDefaultValue().withDescription("key measurement fields");
    public static final Option<String> STORAGE_GROUP = Options.key("storage_group").stringType().noDefaultValue().withDescription("store group");
    public static final Option<Integer> BATCH_SIZE =  Options.key("batch_size").intType().defaultValue(DEFAULT_BATCH_SIZE).withDescription("batch size");
    public static final Option<String> BATCH_INTERVAL_MS = Options.key("batch_interval_ms").stringType().noDefaultValue().withDescription("batch interval ms");
    public static final Option<Integer> MAX_RETRIES = Options.key("max_retries").intType().noDefaultValue().withDescription("max retries");
    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS = Options.key("retry_backoff_multiplier_ms").intType().noDefaultValue().withDescription("retry backoff multiplier ms ");
    public static final Option<Integer> MAX_RETRY_BACKOFF_MS = Options.key("max_retry_backoff_ms").intType().noDefaultValue().withDescription("max retry backoff ms ");
    public static final Option<Integer> DEFAULT_THRIFT_BUFFER_SIZE = Options.key("default_thrift_buffer_size").intType().noDefaultValue().withDescription("default thrift buffer size");
    public static final Option<Integer> MAX_THRIFT_FRAME_SIZE = Options.key("max_thrift_frame_size").intType().noDefaultValue().withDescription("max thrift frame size");
    public static final Option<String> ZONE_ID = Options.key("zone_id").stringType().noDefaultValue().withDescription("zone id");
    public static final Option<Boolean> ENABLE_RPC_COMPRESSION = Options.key("enable_rpc_compression").booleanType().noDefaultValue().withDescription("enable rpc comm");
    public static final Option<Integer> CONNECTION_TIMEOUT_IN_MS = Options.key("connection_timeout_in_ms").intType().noDefaultValue().withDescription("connection timeout ms");

    private String keyTimestamp;
    private String keyDevice;
    private List<String> keyMeasurementFields;
    private String storageGroup;
    private int batchSize = BATCH_SIZE.defaultValue();
    private Integer batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private Integer thriftDefaultBufferSize;
    private Integer thriftMaxFrameSize;
    private ZoneId zoneId;
    private Boolean enableRPCCompression;
    private Integer connectionTimeoutInMs;

    public SinkConfig(@NonNull List<String> nodeUrls,
                      @NonNull String username,
                      @NonNull String password) {
        super(nodeUrls, username, password);
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        SinkConfig sinkConfig = new SinkConfig(
                pluginConfig.getStringList(NODE_URLS.key()),
                pluginConfig.getString(USERNAME.key()),
                pluginConfig.getString(PASSWORD.key()));

        sinkConfig.setKeyDevice(pluginConfig.getString(KEY_DEVICE.key()));
        if (pluginConfig.hasPath(KEY_TIMESTAMP.key())) {
            sinkConfig.setKeyTimestamp(pluginConfig.getString(KEY_TIMESTAMP.key()));
        }
        if (pluginConfig.hasPath(KEY_MEASUREMENT_FIELDS.key())) {
            sinkConfig.setKeyMeasurementFields(pluginConfig.getStringList(KEY_MEASUREMENT_FIELDS.key()));
        }
        if (pluginConfig.hasPath(STORAGE_GROUP.key())) {
            sinkConfig.setStorageGroup(pluginConfig.getString(STORAGE_GROUP.key()));
        }
        if (pluginConfig.hasPath(BATCH_SIZE.key())) {
            int batchSize = checkIntArgument(pluginConfig.getInt(BATCH_SIZE.key()));
            sinkConfig.setBatchSize(batchSize);
        }
        if (pluginConfig.hasPath(BATCH_INTERVAL_MS.key())) {
            int batchIntervalMs = checkIntArgument(pluginConfig.getInt(BATCH_INTERVAL_MS.key()));
            sinkConfig.setBatchIntervalMs(batchIntervalMs);
        }
        if (pluginConfig.hasPath(MAX_RETRIES.key())) {
            int maxRetries = checkIntArgument(pluginConfig.getInt(MAX_RETRIES.key()));
            sinkConfig.setMaxRetries(maxRetries);
        }
        if (pluginConfig.hasPath(RETRY_BACKOFF_MULTIPLIER_MS.key())) {
            int retryBackoffMultiplierMs = checkIntArgument(pluginConfig.getInt(RETRY_BACKOFF_MULTIPLIER_MS.key()));
            sinkConfig.setRetryBackoffMultiplierMs(retryBackoffMultiplierMs);
        }
        if (pluginConfig.hasPath(MAX_RETRY_BACKOFF_MS.key())) {
            int maxRetryBackoffMs = checkIntArgument(pluginConfig.getInt(MAX_RETRY_BACKOFF_MS.key()));
            sinkConfig.setMaxRetryBackoffMs(maxRetryBackoffMs);
        }
        if (pluginConfig.hasPath(DEFAULT_THRIFT_BUFFER_SIZE.key())) {
            int thriftDefaultBufferSize = checkIntArgument(pluginConfig.getInt(DEFAULT_THRIFT_BUFFER_SIZE.key()));
            sinkConfig.setThriftDefaultBufferSize(thriftDefaultBufferSize);
        }
        if (pluginConfig.hasPath(MAX_THRIFT_FRAME_SIZE.key())) {
            int thriftMaxFrameSize = checkIntArgument(pluginConfig.getInt(MAX_THRIFT_FRAME_SIZE.key()));
            sinkConfig.setThriftMaxFrameSize(thriftMaxFrameSize);
        }
        if (pluginConfig.hasPath(ZONE_ID.key())) {
            sinkConfig.setZoneId(ZoneId.of(pluginConfig.getString(ZONE_ID.key())));
        }
        if (pluginConfig.hasPath(ENABLE_RPC_COMPRESSION.key())) {
            sinkConfig.setEnableRPCCompression(pluginConfig.getBoolean(ENABLE_RPC_COMPRESSION.key()));
        }
        if (pluginConfig.hasPath(CONNECTION_TIMEOUT_IN_MS.key())) {
            int connectionTimeoutInMs = checkIntArgument(pluginConfig.getInt(CONNECTION_TIMEOUT_IN_MS.key()));
            checkNotNull(sinkConfig.getEnableRPCCompression());
            sinkConfig.setConnectionTimeoutInMs(connectionTimeoutInMs);
        }
        return sinkConfig;
    }

    private static int checkIntArgument(int args) {
        checkArgument(args > 0);
        return args;
    }
}
