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

import java.util.List;

public class WebSocketSourceOptions extends WebSocketCommonOptions {

    private static final String DEFAULT_FIELD_DELIMITER = ",";

    private static final int DEFAULT_PING_INTERVAL_MS = 0;

    private static final int DEFAULT_MAX_RECONNECT_TIMES = 3;

    private static final int DEFAULT_RECONNECT_INTERVAL_MS = 3000;

    private static final int DEFAULT_QUEUE_CAPACITY = 1024;

    private static final int DEFAULT_POLL_TIMEOUT_MS = 1000;

    private static final long DEFAULT_MAX_RECORDS = -1L;

    private static final int DEFAULT_READ_TIMEOUT_MS = -1;

    public static final Option<List<String>> OPEN_MESSAGES =
            Options.key("open_messages")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Messages sent to the server in order right after the handshake "
                                    + "succeeds, usually subscription or authentication payloads. "
                                    + "They are re-sent on every successful reconnect.");

    public static final Option<WebSocketMessageFormat> FORMAT =
            Options.key("format")
                    .enumType(WebSocketMessageFormat.class)
                    .defaultValue(WebSocketMessageFormat.JSON)
                    .withDescription(
                            "Data format of the received message, only takes effect when \"schema\" "
                                    + "is configured. The default format is json, optional text format. "
                                    + "If you customize the delimiter of the text format, "
                                    + "add the \"field_delimiter\" option.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription(
                            "Customize the field delimiter, only takes effect when format is text. "
                                    + "Default value is \""
                                    + DEFAULT_FIELD_DELIMITER
                                    + "\"");

    public static final Option<Integer> PING_INTERVAL_MS =
            Options.key("ping_interval_ms")
                    .intType()
                    .defaultValue(DEFAULT_PING_INTERVAL_MS)
                    .withDescription(
                            "Interval in milliseconds of the WebSocket ping frame used to keep the "
                                    + "connection alive. Value 0 disables it, default value is "
                                    + DEFAULT_PING_INTERVAL_MS);

    public static final Option<Boolean> ENABLE_RECONNECT =
            Options.key("enable_reconnect")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to reconnect automatically after the connection is broken, "
                                    + "default value is true");

    public static final Option<Integer> MAX_RECONNECT_TIMES =
            Options.key("max_reconnect_times")
                    .intType()
                    .defaultValue(DEFAULT_MAX_RECONNECT_TIMES)
                    .withDescription(
                            "Maximum consecutive reconnect attempts before the task fails, "
                                    + "default value is "
                                    + DEFAULT_MAX_RECONNECT_TIMES);

    public static final Option<Integer> RECONNECT_INTERVAL_MS =
            Options.key("reconnect_interval_ms")
                    .intType()
                    .defaultValue(DEFAULT_RECONNECT_INTERVAL_MS)
                    .withDescription(
                            "Waiting time in milliseconds before each reconnect attempt, "
                                    + "default value is "
                                    + DEFAULT_RECONNECT_INTERVAL_MS);

    public static final Option<Integer> QUEUE_CAPACITY =
            Options.key("queue_capacity")
                    .intType()
                    .defaultValue(DEFAULT_QUEUE_CAPACITY)
                    .withDescription(
                            "Capacity of the local queue buffering received messages. The receiving "
                                    + "thread blocks once the queue is full, which back-pressures the "
                                    + "server. Default value is "
                                    + DEFAULT_QUEUE_CAPACITY);

    public static final Option<Integer> POLL_TIMEOUT_MS =
            Options.key("poll_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_POLL_TIMEOUT_MS)
                    .withDescription(
                            "Maximum time in milliseconds that a single read waits on the local "
                                    + "queue when no message is available, default value is "
                                    + DEFAULT_POLL_TIMEOUT_MS);

    public static final Option<Long> MAX_RECORDS =
            Options.key("max_records")
                    .longType()
                    .defaultValue(DEFAULT_MAX_RECORDS)
                    .withDescription(
                            "Stop reading after this number of rows has been emitted. Only takes "
                                    + "effect in batch mode, value -1 means unlimited. Default value is "
                                    + DEFAULT_MAX_RECORDS);

    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_READ_TIMEOUT_MS)
                    .withDescription(
                            "Stop reading after no message has been received for this many "
                                    + "milliseconds. Only takes effect in batch mode, value -1 means "
                                    + "unlimited. Default value is "
                                    + DEFAULT_READ_TIMEOUT_MS);
}
