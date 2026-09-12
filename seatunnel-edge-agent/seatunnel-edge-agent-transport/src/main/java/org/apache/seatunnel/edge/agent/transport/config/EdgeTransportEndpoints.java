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

import org.apache.seatunnel.edge.agent.transport.socket.EdgeTransportClient;

import java.net.InetSocketAddress;
import java.util.Objects;

public class EdgeTransportEndpoints {

    /**
     * Validates {@code endpoint} format (same rules as EdgeSocket connector). Throws {@code
     * IllegalArgumentException} when invalid.
     */
    public static void validateFormat(String endpoint) {
        parseHostAndPort(endpoint);
    }

    /** Resolves {@code endpoint} to a socket address for {@link EdgeTransportClient}. */
    public static InetSocketAddress toSocketAddress(String endpoint) {
        HostPort hostPort = parseHostAndPort(endpoint);
        return new InetSocketAddress(hostPort.host, hostPort.port);
    }

    private static HostPort parseHostAndPort(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        String trimmed = endpoint.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("transport.endpoint must be non-empty.");
        }
        int separatorIndex = trimmed.lastIndexOf(':');
        if (separatorIndex <= 0 || separatorIndex >= trimmed.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid endpoint: " + endpoint + ", expected format host:port");
        }
        String host = trimmed.substring(0, separatorIndex);
        String portText = trimmed.substring(separatorIndex + 1);
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException parseException) {
            throw new IllegalArgumentException(
                    "Invalid endpoint port in endpoint: " + endpoint, parseException);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "transport.endpoint port must be a valid TCP port (1-65535), got: " + port);
        }
        return new HostPort(host, port);
    }

    private static final class HostPort {
        private final String host;
        private final int port;

        HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
}
