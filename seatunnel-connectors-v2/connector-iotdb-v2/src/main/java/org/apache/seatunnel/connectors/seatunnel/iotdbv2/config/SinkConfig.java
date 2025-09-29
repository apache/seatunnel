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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

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

    private String keyTimestamp;
    private String keyDevice;
    private List<String> keyMeasurementFields;
    private List<String> keyTagFields;
    private List<String> keyAttributeFields;
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
                        pluginConfig.get(IoTDBv2SinkOptions.NODE_URLS),
                        pluginConfig.get(IoTDBv2SinkOptions.USERNAME),
                        pluginConfig.get(IoTDBv2SinkOptions.PASSWORD));

        sinkConfig.setKeyDevice(pluginConfig.get(IoTDBv2SinkOptions.KEY_DEVICE));
        sinkConfig.setKeyTimestamp(pluginConfig.get(IoTDBv2SinkOptions.KEY_TIMESTAMP));
        sinkConfig.setKeyMeasurementFields(
                pluginConfig.get(IoTDBv2SinkOptions.KEY_MEASUREMENT_FIELDS));
        sinkConfig.setKeyTagFields(pluginConfig.get(IoTDBv2SinkOptions.KEY_TAG_FIELDS));
        sinkConfig.setKeyAttributeFields(pluginConfig.get(IoTDBv2SinkOptions.KEY_ATTRIBUTE_FIELDS));
        sinkConfig.setStorageGroup(pluginConfig.get(IoTDBv2SinkOptions.STORAGE_GROUP));
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.BATCH_SIZE).isPresent()) {
            sinkConfig.setBatchSize(pluginConfig.get(IoTDBv2SinkOptions.BATCH_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.MAX_RETRIES).isPresent()) {
            sinkConfig.setMaxRetries(pluginConfig.get(IoTDBv2SinkOptions.MAX_RETRIES));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.RETRY_BACKOFF_MULTIPLIER_MS).isPresent()) {
            sinkConfig.setRetryBackoffMultiplierMs(
                    pluginConfig.get(IoTDBv2SinkOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.MAX_RETRY_BACKOFF_MS).isPresent()) {
            sinkConfig.setMaxRetryBackoffMs(
                    pluginConfig.get(IoTDBv2SinkOptions.MAX_RETRY_BACKOFF_MS));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.DEFAULT_THRIFT_BUFFER_SIZE).isPresent()) {
            sinkConfig.setThriftDefaultBufferSize(
                    pluginConfig.get(IoTDBv2SinkOptions.DEFAULT_THRIFT_BUFFER_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.MAX_THRIFT_FRAME_SIZE).isPresent()) {
            sinkConfig.setThriftMaxFrameSize(
                    pluginConfig.get(IoTDBv2SinkOptions.MAX_THRIFT_FRAME_SIZE));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.ZONE_ID).isPresent()) {
            sinkConfig.setZoneId(ZoneId.of(pluginConfig.get(IoTDBv2SinkOptions.ZONE_ID)));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.ENABLE_RPC_COMPRESSION).isPresent()) {
            sinkConfig.setEnableRPCCompression(
                    pluginConfig.get(IoTDBv2SinkOptions.ENABLE_RPC_COMPRESSION));
        }
        if (pluginConfig.getOptional(IoTDBv2SinkOptions.CONNECTION_TIMEOUT_IN_MS).isPresent()) {
            sinkConfig.setConnectionTimeoutInMs(
                    pluginConfig.get(IoTDBv2SinkOptions.CONNECTION_TIMEOUT_IN_MS));
        }
        return sinkConfig;
    }
}
