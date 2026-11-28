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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines the Sink-only payload, receive and finite lifecycle configuration contracts.
 *
 * @see WebSocketSinkConfig
 */
public final class WebSocketSinkOptions {
    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "WebSocket endpoint (ws:// or wss://), without user information, query or fragment.");
    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("JSON")
                    .withDescription("One message per row: JSON, TEXT, or BINARY.");
    public static final Option<String> FIELD =
            Options.key("field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Input column for TEXT (string) or BINARY (bytes).");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional bearer token. Requires wss://.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Opening handshake timeout in milliseconds.");
    public static final Option<Integer> WRITE_TIMEOUT =
            Options.key("write_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Socket write and queue admission timeout in milliseconds.");
    public static final Option<Integer> FLUSH_TIMEOUT =
            Options.key("flush_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Local send queue drain timeout in milliseconds.");
    public static final Option<Integer> CLOSE_TIMEOUT =
            Options.key("close_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Normal close handshake timeout in milliseconds.");
    public static final Option<Integer> MAX_FRAME_BYTES =
            Options.key("max_frame_bytes")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription("Maximum uncompressed bytes in one outbound message.");
    public static final Option<Integer> MAX_QUEUE_BYTES =
            Options.key("max_queue_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Maximum outstanding uncompressed payload bytes per writer.");

    public static final Option<Integer> MAX_INBOUND_BYTES =
            Options.key("max_inbound_message_bytes")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription(
                            "Maximum bytes per incoming message, including all continuation fragments (1 to 16777216).");

    private WebSocketSinkOptions() {}
}
