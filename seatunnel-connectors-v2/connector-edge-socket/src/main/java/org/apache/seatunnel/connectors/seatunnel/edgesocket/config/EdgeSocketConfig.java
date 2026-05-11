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
    private String host;
    private int port;
    private int localQueueCapacity;
    private int maxNumRetries;
    private int reconnectIntervalMs;
    private int acceptTimeoutMs;
    private EdgeSocketPacketMode packetMode;
    private String aesSecretKeyBase64;
    private byte[] aesSecretKeyBytes;
    private String authToken;
    private EdgeSocketAuthType authType;

    public EdgeSocketConfig(ReadonlyConfig config) {
        this.host = config.getOptional(EdgeSocketCommonOptions.HOST).orElse(null);
        this.port = config.get(EdgeSocketCommonOptions.PORT);
        this.localQueueCapacity = config.get(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY);
        this.maxNumRetries = config.get(EdgeSocketSourceOptions.MAX_RETRIES);
        this.reconnectIntervalMs = config.get(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS);
        this.acceptTimeoutMs = config.get(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS);
        this.packetMode =
                EdgeSocketPacketMode.from(config.get(EdgeSocketSourceOptions.PACKET_MODE));
        this.aesSecretKeyBase64 =
                config.getOptional(EdgeSocketSourceOptions.AES_SECRET_KEY_BASE64).orElse(null);
        this.authType = EdgeSocketAuthType.from(config.get(EdgeSocketSourceOptions.AUTH_TYPE));
        this.authToken = config.getOptional(EdgeSocketSourceOptions.AUTH_TOKEN).orElse(null);
        if (this.host != null && this.host.trim().isEmpty()) {
            this.host = null;
        }
        if (this.localQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid local_queue_capacity: %s, it must be greater than 0",
                            this.localQueueCapacity));
        }
        if (this.packetMode == EdgeSocketPacketMode.PACKET && this.aesSecretKeyBase64 != null) {
            this.aesSecretKeyBytes = Base64.getDecoder().decode(this.aesSecretKeyBase64);
        }
        if (this.authType == EdgeSocketAuthType.TOKEN
                && (this.authToken == null || this.authToken.trim().isEmpty())) {
            throw new IllegalArgumentException("auth_token is required when auth_type is TOKEN");
        }
    }
}
