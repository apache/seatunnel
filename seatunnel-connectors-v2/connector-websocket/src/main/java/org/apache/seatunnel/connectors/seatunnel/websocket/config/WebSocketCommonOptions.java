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

package org.apache.seatunnel.connectors.seatunnel.websocket.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class WebSocketCommonOptions {

    public static final String identifier = "WebSocket";

    public static final String WS_SCHEME = "ws://";

    public static final String WSS_SCHEME = "wss://";

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 12000;

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "WebSocket server endpoint, must start with \"ws://\" or \"wss://\"");

    public static final Option<Map<String, String>> HEADERS =
            Options.key("headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Headers attached to the WebSocket handshake request");

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_CONNECT_TIMEOUT_MS)
                    .withDescription(
                            "Handshake connect timeout in milliseconds, default value is "
                                    + DEFAULT_CONNECT_TIMEOUT_MS);
}
