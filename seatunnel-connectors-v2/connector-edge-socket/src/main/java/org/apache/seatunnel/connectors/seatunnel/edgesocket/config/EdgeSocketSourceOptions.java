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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class EdgeSocketSourceOptions extends EdgeSocketCommonOptions {
    private static final int DEFAULT_LOCAL_QUEUE_CAPACITY = 1024;
    private static final double DEFAULT_QUEUE_BACKPRESSURE_WATERMARK_RATIO = 0.9;
    private static final int DEFAULT_QUEUE_FULL_RETRY_AFTER_MS = 500;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_RECONNECT_INTERVAL_MS = 1000;
    private static final int DEFAULT_ACCEPT_TIMEOUT_MS = 1000;
    private static final String DEFAULT_PACKET_MODE = "RAW";
    private static final String DEFAULT_AUTH_TYPE = "TOKEN";

    public static final Option<Integer> LOCAL_QUEUE_CAPACITY =
            Options.key("local_queue_capacity")
                    .intType()
                    .defaultValue(DEFAULT_LOCAL_QUEUE_CAPACITY)
                    .withDescription(
                            "Local in-memory queue capacity in source reader, default is "
                                    + DEFAULT_LOCAL_QUEUE_CAPACITY);

    public static final Option<Double> QUEUE_BACKPRESSURE_WATERMARK_RATIO =
            Options.key("queue_backpressure_watermark_ratio")
                    .doubleType()
                    .defaultValue(DEFAULT_QUEUE_BACKPRESSURE_WATERMARK_RATIO)
                    .withDescription(
                            "High-water mark ratio of local_queue_capacity. When queue size "
                                    + "reaches ceil(capacity * ratio), ingress responds QUEUE_FULL "
                                    + "without decoding the payload. Default is "
                                    + DEFAULT_QUEUE_BACKPRESSURE_WATERMARK_RATIO);

    public static final Option<Integer> QUEUE_FULL_RETRY_AFTER_MS =
            Options.key("queue_full_retry_after_ms")
                    .intType()
                    .defaultValue(DEFAULT_QUEUE_FULL_RETRY_AFTER_MS)
                    .withDescription(
                            "Suggested backoff in milliseconds embedded in QUEUE_FULL responses. "
                                    + "Default is "
                                    + DEFAULT_QUEUE_FULL_RETRY_AFTER_MS);

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(DEFAULT_MAX_RETRIES)
                    .withDescription(
                            "Maximum reconnect retries, default is " + DEFAULT_MAX_RETRIES);

    public static final Option<Integer> RECONNECT_INTERVAL_MS =
            Options.key("reconnect_interval_ms")
                    .intType()
                    .defaultValue(DEFAULT_RECONNECT_INTERVAL_MS)
                    .withDescription(
                            "Server socket reopen interval in milliseconds, default is "
                                    + DEFAULT_RECONNECT_INTERVAL_MS);

    public static final Option<Integer> ACCEPT_TIMEOUT_MS =
            Options.key("accept_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_ACCEPT_TIMEOUT_MS)
                    .withDescription(
                            "Socket accept timeout in milliseconds, default is "
                                    + DEFAULT_ACCEPT_TIMEOUT_MS);

    public static final Option<String> PACKET_MODE =
            Options.key("packet_mode")
                    .stringType()
                    .defaultValue(DEFAULT_PACKET_MODE)
                    .withDescription(
                            "Incoming packet mode, supported values: RAW, PACKET. Default is "
                                    + DEFAULT_PACKET_MODE);

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Base64 encoded AES-256 key for decrypting PACKET mode payload when encryption is AES_GCM");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Token value used by TOKEN auth_type. This option is required when auth_type is TOKEN.");

    public static final Option<String> AUTH_TYPE =
            Options.key("auth_type")
                    .stringType()
                    .defaultValue(DEFAULT_AUTH_TYPE)
                    .withDescription(
                            "Authentication type for ingress connection. Currently supported value: TOKEN. Default is "
                                    + DEFAULT_AUTH_TYPE);
}
