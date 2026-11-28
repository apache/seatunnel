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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.Map;

/**
 * Declares the registered-device options used by Factory validation and worker admission. Limits
 * apply to one writer and do not provide endpoint transactions or recovery deduplication.
 */
public final class AzureIoTHubSinkOptions {
    public static final Option<String> HUB_URL =
            Options.key("hub_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted HTTPS IoT Hub origin, with no path, query, or user information.");
    public static final Option<String> DEVICE_ID =
            Options.key("device_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Case-sensitive identifier of the registered device.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Base64-encoded symmetric key of the registered device.");
    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("JSON")
                    .withDescription("Message format: JSON, TEXT, or BINARY.");
    public static final Option<String> FIELD =
            Options.key("field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Input STRING column (TEXT) or BYTES column (BINARY).");
    public static final Option<Map<String, String>> APPLICATION_PROPERTIES =
            Options.key("application_properties")
                    .type(new TypeReference<Map<String, String>>() {})
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Public IoT Hub HTTPS token-alphabet properties; whitespace, slash and colon are invalid.");
    public static final Option<Integer> SAS_TTL =
            Options.key("sas_ttl_seconds")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("Lifetime in seconds of each device SAS token.");
    public static final Option<Integer> MAX_PAYLOAD =
            Options.key("max_payload_bytes")
                    .intType()
                    .defaultValue(262144)
                    .withDescription(
                            "Maximum body bytes, reduced when necessary to fit IoT Hub's full message budget.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection deadline in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and response read inactivity limit in milliseconds.");
    public static final Option<Integer> ROW_TIMEOUT =
            Options.key("row_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total row I/O deadline including connection, write, response and retry waits.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum attempts per row, including the first. Only HTTP 429 permits another attempt.");

    private AzureIoTHubSinkOptions() {}
}
