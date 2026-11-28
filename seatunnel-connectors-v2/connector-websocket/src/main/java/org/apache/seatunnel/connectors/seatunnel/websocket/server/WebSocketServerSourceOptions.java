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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Listener settings for the WebSocketServer source. */
public final class WebSocketServerSourceOptions {
    public static final Option<String> BIND_HOST =
            Options.key("bind_host")
                    .stringType()
                    .defaultValue("127.0.0.1")
                    .withDescription("Network address to bind on the active worker");
    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("TCP listener port on the active worker");
    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .defaultValue("/")
                    .withDescription("HTTP upgrade path. Query parameters are not accepted");
    public static final Option<String> EXPECTED_HOST =
            Options.key("expected_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional exact HTTP Host authority for accepted upgrade requests");
    public static final Option<java.util.List<String>> ALLOWED_ORIGINS =
            Options.key("allowed_origins")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Accepted browser origins. Requests without Origin are allowed");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional bearer token. Provide it in the Authorization header");
    public static final Option<String> KEYSTORE_PATH =
            Options.key("keystore_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TLS server key store on the worker. Omit for a plain listener");
    public static final Option<String> KEYSTORE_TYPE =
            Options.key("keystore_type")
                    .stringType()
                    .defaultValue("PKCS12")
                    .withDescription("TLS key store type: PKCS12 or JKS");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TLS key store and private key password");
    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("raw")
                    .withDescription("Payload format: raw, text or json");
    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Delimiter for text messages when a row schema is supplied");
    public static final Option<Integer> MAX_CONNECTIONS =
            Options.key("max_connections")
                    .intType()
                    .defaultValue(16)
                    .withDescription(
                            "Maximum simultaneous connections, including pending upgrades");
    public static final Option<Integer> MAX_MESSAGE_BYTES =
            Options.key("max_message_bytes")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("Maximum payload bytes in a frame or an assembled message");
    public static final Option<Integer> MAX_QUEUE_MESSAGES =
            Options.key("max_queue_messages")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum queued messages waiting for the engine");
    public static final Option<Integer> MAX_QUEUE_BYTES =
            Options.key("max_queue_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Maximum total queued payload bytes");
    public static final Option<Integer> HANDSHAKE_TIMEOUT_MS =
            Options.key("handshake_timeout_ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Whole TLS and HTTP handshake deadline in milliseconds");
    public static final Option<Integer> IDLE_TIMEOUT_MS =
            Options.key("idle_timeout_ms")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Maximum elapsed idle time before a new frame starts");
    public static final Option<Integer> MESSAGE_TIMEOUT_MS =
            Options.key("message_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Whole WebSocket frame or fragmented message deadline in milliseconds");

    private WebSocketServerSourceOptions() {}

    /** Serializes only configured settings, never the listener, TLS context or received data. */
    static final class Settings implements Serializable {
        private static final long serialVersionUID = 1L;
        final String bindHost;
        final int port;
        final String path;
        final String expectedHost;
        final Set<String> allowedOrigins = new HashSet<>();
        final String token;
        final String keystorePath;
        final String keystoreType;
        final String password;
        final String format;
        final String delimiter;
        final int connections;
        final int messageBytes;
        final int queueMessages;
        final int queueBytes;
        final int handshakeTimeout;
        final int idleTimeout;
        final int messageTimeout;

        Settings(ReadonlyConfig options) {
            bindHost = options.get(BIND_HOST);
            port = bounded(options, PORT, 1, 65535);
            path = options.get(PATH);
            expectedHost = optional(options.get(EXPECTED_HOST));
            for (String origin : options.get(ALLOWED_ORIGINS)) {
                allowedOrigins.add(origin(origin));
            }
            if (bindHost == null
                    || bindHost.isEmpty()
                    || !validPath(path)
                    || expectedHost != null && !validHost(expectedHost)) {
                throw invalid("bind host, path or expected Host");
            }
            token = optional(options.get(TOKEN));
            if (token != null && (token.length() > 2048 || !token.matches("[A-Za-z0-9._~+/=-]+"))) {
                throw invalid("bearer token format");
            }
            keystorePath = optional(options.get(KEYSTORE_PATH));
            keystoreType = options.get(KEYSTORE_TYPE);
            password = optional(options.get(PASSWORD));
            if ((keystorePath == null) != (password == null)
                    || !("PKCS12".equals(keystoreType) || "JKS".equals(keystoreType))) {
                throw invalid("TLS key store requires a password and a supported type");
            }
            format = options.get(FORMAT);
            delimiter = options.get(FIELD_DELIMITER);
            if (!("raw".equals(format) || "text".equals(format) || "json".equals(format))
                    || delimiter == null
                    || delimiter.isEmpty()
                    || delimiter.length() > 16) {
                throw invalid("payload format or text delimiter");
            }
            connections = bounded(options, MAX_CONNECTIONS, 1, 64);
            messageBytes = bounded(options, MAX_MESSAGE_BYTES, 1, 4 * 1024 * 1024);
            queueMessages = bounded(options, MAX_QUEUE_MESSAGES, 1, 10000);
            queueBytes = bounded(options, MAX_QUEUE_BYTES, messageBytes, 32 * 1024 * 1024);
            if ((long) connections * messageBytes > 32L * 1024 * 1024) {
                throw invalid("combined connection and message budget exceeds 32 MiB");
            }
            handshakeTimeout = bounded(options, HANDSHAKE_TIMEOUT_MS, 100, 120000);
            idleTimeout = bounded(options, IDLE_TIMEOUT_MS, 100, 600000);
            messageTimeout = bounded(options, MESSAGE_TIMEOUT_MS, 100, 120000);
        }

        private static int bounded(
                ReadonlyConfig options, Option<Integer> option, int lower, int upper) {
            Integer value = options.get(option);
            if (value == null || value < lower || value > upper) {
                throw invalid(option.key());
            }
            return value;
        }

        private static String optional(String value) {
            return value == null || value.isEmpty() ? null : value;
        }

        private static boolean validPath(String value) {
            if (value == null
                    || value.length() > 4096
                    || value.startsWith("//")
                    || !value.matches("/[a-zA-Z0-9._~!$&'()*+,;=:@/%-]*")) {
                return false;
            }
            try {
                return value.equals(new URI(value).getRawPath());
            } catch (URISyntaxException ex) {
                return false;
            }
        }

        static boolean validHost(String value) {
            try {
                URI uri = new URI("http://" + value);
                return uri.getHost() != null
                        && uri.getRawUserInfo() == null
                        && uri.getRawPath().isEmpty()
                        && uri.getRawQuery() == null
                        && uri.getRawFragment() == null
                        && uri.getPort() <= 65535;
            } catch (URISyntaxException ex) {
                return false;
            }
        }

        static String origin(String value) {
            try {
                URI uri = new URI(value);
                String scheme =
                        uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
                if ((!scheme.equals("http") && !scheme.equals("https"))
                        || uri.getHost() == null
                        || uri.getRawUserInfo() != null
                        || !uri.getRawPath().isEmpty()
                        || uri.getRawQuery() != null
                        || uri.getRawFragment() != null
                        || uri.getPort() > 65535) {
                    throw invalid("origin");
                }
                int port = uri.getPort() < 0 ? (scheme.equals("https") ? 443 : 80) : uri.getPort();
                return scheme + "://" + uri.getHost().toLowerCase(Locale.ROOT) + ":" + port;
            } catch (URISyntaxException ex) {
                throw invalid("origin");
            }
        }

        private static IllegalArgumentException invalid(String setting) {
            return new IllegalArgumentException("Invalid WebSocketServer " + setting);
        }
    }
}
