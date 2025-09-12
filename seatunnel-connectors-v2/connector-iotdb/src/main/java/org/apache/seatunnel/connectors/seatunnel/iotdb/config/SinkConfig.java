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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.time.ZoneId;
import java.util.List;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Setter
@Getter
@ToString
public class SinkConfig extends CommonConfig {

    private String keyTimestamp;
    private String keyDevice;
    private List<String> keyMeasurementFields;
    private String storageGroup;
    private int batchSize;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private Integer thriftDefaultBufferSize;
    private Integer thriftMaxFrameSize;
    private ZoneId zoneId;
    private Boolean enableRPCCompression;
    private Integer connectionTimeoutInMs;

    public SinkConfig(
            @NonNull List<String> nodeUrls, @NonNull String username, @NonNull String password) {
        super(nodeUrls, username, password);
    }

    public static SinkConfig loadConfig(ReadonlyConfig pluginConfig) {
        SinkConfig sinkConfig =
                new SinkConfig(
                        pluginConfig.toConfig().getStringList(IoTDBSinkOptions.NODE_URLS.key()),
                        pluginConfig.get(IoTDBSinkOptions.USERNAME),
                        pluginConfig.get(IoTDBSinkOptions.PASSWORD));

        sinkConfig.setKeyDevice(pluginConfig.get(IoTDBSinkOptions.KEY_DEVICE));
        sinkConfig.setKeyTimestamp(pluginConfig.get(IoTDBSinkOptions.KEY_TIMESTAMP));
        sinkConfig.setKeyMeasurementFields(
                pluginConfig.get(IoTDBSinkOptions.KEY_MEASUREMENT_FIELDS));
        sinkConfig.setStorageGroup(pluginConfig.get(IoTDBSinkOptions.STORAGE_GROUP));
        if (pluginConfig.getOptional(IoTDBSinkOptions.BATCH_SIZE).isPresent()) {
            sinkConfig.setBatchSize(pluginConfig.get(IoTDBSinkOptions.BATCH_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.MAX_RETRIES).isPresent()) {
            sinkConfig.setMaxRetries(pluginConfig.get(IoTDBSinkOptions.MAX_RETRIES));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS).isPresent()) {
            sinkConfig.setRetryBackoffMultiplierMs(
                    pluginConfig.get(IoTDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.MAX_RETRY_BACKOFF_MS).isPresent()) {
            sinkConfig.setMaxRetryBackoffMs(
                    pluginConfig.get(IoTDBSinkOptions.MAX_RETRY_BACKOFF_MS));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.DEFAULT_THRIFT_BUFFER_SIZE).isPresent()) {
            sinkConfig.setThriftDefaultBufferSize(
                    pluginConfig.get(IoTDBSinkOptions.DEFAULT_THRIFT_BUFFER_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.MAX_THRIFT_FRAME_SIZE).isPresent()) {
            sinkConfig.setThriftMaxFrameSize(
                    pluginConfig.get(IoTDBSinkOptions.MAX_THRIFT_FRAME_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBSinkOptions.ZONE_ID).isPresent()) {
            sinkConfig.setZoneId(ZoneId.of(pluginConfig.get(IoTDBSinkOptions.ZONE_ID)));
        }
        sinkConfig.setEnableRPCCompression(
                pluginConfig.get(IoTDBSinkOptions.ENABLE_RPC_COMPRESSION));
        if (pluginConfig.getOptional(IoTDBSinkOptions.CONNECTION_TIMEOUT_IN_MS).isPresent()) {
            checkNotNull(sinkConfig.getEnableRPCCompression());
            sinkConfig.setConnectionTimeoutInMs(
                    pluginConfig.get(IoTDBSinkOptions.CONNECTION_TIMEOUT_IN_MS));
        }
        return sinkConfig;
    }
}
