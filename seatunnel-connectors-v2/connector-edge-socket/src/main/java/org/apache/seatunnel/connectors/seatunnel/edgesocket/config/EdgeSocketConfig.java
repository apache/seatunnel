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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPacketMode;

import lombok.Data;

import java.io.Serializable;
import java.util.Base64;

@Data
public class EdgeSocketConfig implements Serializable {

    private String endpoint;
    private int port;
    private int localQueueCapacity;
    private double queueBackpressureWatermarkRatio;
    private int queueFullRetryAfterMs;
    private int maxNumRetries;
    private int reconnectIntervalMs;
    private int acceptTimeoutMs;
    private EdgeSocketPacketMode packetMode;
    private String secretKey;
    private byte[] secretKeyBytes;
    private String token;
    private EdgeSocketAuthType authType;

    public EdgeSocketConfig(ReadonlyConfig config) {
        this.endpoint = config.getOptional(EdgeSocketCommonOptions.ENDPOINT).orElse(null);
        this.port = config.get(EdgeSocketCommonOptions.PORT);
        this.localQueueCapacity = config.get(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY);
        this.queueBackpressureWatermarkRatio =
                config.get(EdgeSocketSourceOptions.QUEUE_BACKPRESSURE_WATERMARK_RATIO);
        this.queueFullRetryAfterMs = config.get(EdgeSocketSourceOptions.QUEUE_FULL_RETRY_AFTER_MS);
        this.maxNumRetries = config.get(EdgeSocketSourceOptions.MAX_RETRIES);
        this.reconnectIntervalMs = config.get(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS);
        this.acceptTimeoutMs = config.get(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS);
        this.packetMode =
                EdgeSocketPacketMode.from(config.get(EdgeSocketSourceOptions.PACKET_MODE));
        this.secretKey = config.getOptional(EdgeSocketSourceOptions.SECRET_KEY).orElse(null);
        this.authType = EdgeSocketAuthType.from(config.get(EdgeSocketSourceOptions.AUTH_TYPE));
        this.token = config.getOptional(EdgeSocketSourceOptions.TOKEN).orElse(null);
        if (this.endpoint != null && this.endpoint.trim().isEmpty()) {
            this.endpoint = null;
        }
        if (this.endpoint != null) {
            int separatorIndex = this.endpoint.lastIndexOf(':');
            if (separatorIndex <= 0 || separatorIndex >= this.endpoint.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid endpoint: " + this.endpoint + ", expected format host:port");
            }
            String endpointPort = this.endpoint.substring(separatorIndex + 1);
            try {
                Integer.parseInt(endpointPort);
            } catch (NumberFormatException parseException) {
                throw new IllegalArgumentException(
                        "Invalid endpoint port in endpoint: " + this.endpoint, parseException);
            }
        }
        if (this.localQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid local_queue_capacity: %s, it must be greater than 0",
                            this.localQueueCapacity));
        }
        if (this.queueBackpressureWatermarkRatio <= 0 || this.queueBackpressureWatermarkRatio > 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid queue_backpressure_watermark_ratio: %s, "
                                    + "it must be in (0, 1]",
                            this.queueBackpressureWatermarkRatio));
        }
        if (this.queueFullRetryAfterMs <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid queue_full_retry_after_ms: %s, it must be greater than 0",
                            this.queueFullRetryAfterMs));
        }
        if (this.packetMode == EdgeSocketPacketMode.PACKET && this.secretKey != null) {
            this.secretKeyBytes = Base64.getDecoder().decode(this.secretKey);
            if (this.secretKeyBytes.length != 32) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid secret_key: AES-256 requires exactly 32 bytes, "
                                        + "but got %d bytes after Base64 decoding",
                                this.secretKeyBytes.length));
            }
        }
        if (this.authType == EdgeSocketAuthType.TOKEN
                && (this.token == null || this.token.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Invalid token: token is required and cannot be empty when auth_type is TOKEN");
        }
    }
}
