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

package org.apache.seatunnel.edge.agent.transport.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketCompressionType;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketEncryptionType;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketMode;

import lombok.Getter;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

@Getter
public class EdgeTransportConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String token;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int maxBatchSendAttempts;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxReconnectCycles;

    public EdgeTransportConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        String rawEndpoint = config.get(EdgeTransportOptions.ENDPOINT);
        if (rawEndpoint == null || rawEndpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("transport.endpoint is required.");
        }
        String trimmedEndpoint = rawEndpoint.trim();
        EdgeTransportEndpoints.validateFormat(trimmedEndpoint);
        this.endpoint = trimmedEndpoint;

        String authType = config.get(EdgeTransportOptions.AUTH_TYPE);
        validateAuthType(authType);
        String rawToken = config.getOptional(EdgeTransportOptions.TOKEN).orElse(null);
        if (rawToken == null || rawToken.trim().isEmpty()) {
            throw new IllegalArgumentException("transport.token is required.");
        }
        this.token = rawToken.trim();

        EdgePacketMode.from(config.get(EdgeTransportOptions.PACKET_MODE));
        EdgePacketCompressionType.from(config.get(EdgeTransportOptions.COMPRESSION));
        EdgePacketEncryptionType encryption =
                EdgePacketEncryptionType.from(config.get(EdgeTransportOptions.ENCRYPTION));
        if (encryption == EdgePacketEncryptionType.AES_GCM) {
            String key =
                    config.getOptional(EdgeTransportOptions.AES_SECRET_KEY_BASE64).orElse(null);
            if (key == null || key.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "transport.aes-secret-key-base64 is required when transport.encryption"
                                + " is \"aes_gcm\".");
            }
        }

        this.connectTimeoutMs = config.get(EdgeTransportOptions.CONNECT_TIMEOUT_MS);
        this.readTimeoutMs = config.get(EdgeTransportOptions.READ_TIMEOUT_MS);
        this.maxBatchSendAttempts = config.get(EdgeTransportOptions.MAX_BATCH_SEND_ATTEMPTS);
        this.initialBackoffMs = config.get(EdgeTransportOptions.INITIAL_BACKOFF_MS);
        this.maxBackoffMs = config.get(EdgeTransportOptions.MAX_BACKOFF_MS);
        this.maxReconnectCycles = config.get(EdgeTransportOptions.MAX_RECONNECT_CYCLES);
    }

    public static EdgeTransportConfig from(ReadonlyConfig config) {
        return new EdgeTransportConfig(config);
    }

    public static long computeBackoffMillis(long attempt, long initial, long max) {
        long doubled = initial << attempt;
        if (doubled <= 0) {
            return max;
        }
        return Math.min(max, doubled);
    }

    public static void sleepQuiet(long millis) throws InterruptedException {
        if (millis <= 0) {
            return;
        }
        Thread.sleep(millis);
    }

    private static void validateAuthType(String authType) {
        if (authType == null || authType.trim().isEmpty()) {
            return;
        }
        String normalized = authType.trim().toLowerCase(Locale.ROOT);
        if (!normalized.equals("token")) {
            throw new IllegalArgumentException(
                    "transport.auth-type must be \"token\" when set; \"none\" is not supported.");
        }
    }
}
