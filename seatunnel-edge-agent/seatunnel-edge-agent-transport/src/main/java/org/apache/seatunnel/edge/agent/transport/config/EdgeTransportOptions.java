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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class EdgeTransportOptions {

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("EdgeSocket ingress address in host:port form (required).");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Authentication token; must match EdgeSocket source token (required).");

    public static final Option<String> AUTH_TYPE =
            Options.key("auth-type")
                    .stringType()
                    .defaultValue("token")
                    .withDescription("Authentication mode; only \"token\" is supported.");

    public static final Option<String> PACKET_MODE =
            Options.key("packet-mode")
                    .stringType()
                    .defaultValue("RAW")
                    .withDescription("Packet framing mode: \"RAW\" or \"PACKET\".");

    public static final Option<String> COMPRESSION =
            Options.key("compression")
                    .stringType()
                    .defaultValue("gzip")
                    .withDescription(
                            "Compression for PACKET mode: \"none\", \"gzip\", \"zlib\", or \"deflate\".");

    public static final Option<String> ENCRYPTION =
            Options.key("encryption")
                    .stringType()
                    .defaultValue("none")
                    .withDescription("Encryption for PACKET mode: \"none\" or \"aes_gcm\".");

    public static final Option<String> AES_SECRET_KEY_BASE64 =
            Options.key("aes-secret-key-base64")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Base64-encoded AES secret key; required when encryption is \"aes_gcm\".");

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect-timeout-ms")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("TCP connection timeout in milliseconds.");

    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read-timeout-ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("TCP read timeout in milliseconds.");

    public static final Option<Integer> MAX_BATCH_SEND_ATTEMPTS =
            Options.key("max-batch-send-attempts")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Maximum send attempts per batch before reconnect.");

    public static final Option<Long> INITIAL_BACKOFF_MS =
            Options.key("initial-backoff-ms")
                    .longType()
                    .defaultValue(100L)
                    .withDescription("Initial backoff delay for transport reconnection.");

    public static final Option<Long> MAX_BACKOFF_MS =
            Options.key("max-backoff-ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("Maximum backoff delay for transport reconnection.");

    public static final Option<Integer> MAX_RECONNECT_CYCLES =
            Options.key("max-reconnect-cycles")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Maximum reconnect cycles before failing the batch.");
}
