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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public final class AmplitudeSourceOptions {
    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Amplitude project API key, sent only in HTTP Basic authentication.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Amplitude project secret key. Never include it in URLs or logs.");
    public static final Option<String> START =
            Options.key("start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive UTC server-upload start hour, in strict uuuuMMddTHH format.");
    public static final Option<String> END =
            Options.key("end")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive UTC server-upload end hour. The entire hour must be at least two hours old.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("US")
                    .withDescription("Project data residency: US or EU.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted HTTPS origin override receiving credentials; no path, query or userinfo.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow HTTP for trusted offline fixtures only. Never use real credentials over HTTP.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout in milliseconds, 1 to 300000.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription("Socket inactivity timeout in milliseconds, 1 to 600000.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Retry budget for transport errors, HTTP 429 and 5xx before emitting rows, 0 to 10.");
    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Initial exponential retry delay in milliseconds, 1 to 60000.");
    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Maximum retry wait in milliseconds, 1 to 600000. Longer Retry-After fails instead of retrying early.");
    public static final Option<Integer> MAX_EVENT_BYTES =
            Options.key("max_event_bytes")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription(
                            "Maximum JSONL record bytes, 1 to 16777216. Oversized events fail, never truncate.");
    public static final Option<Long> MAX_ARCHIVE_BYTES =
            Options.key("max_archive_bytes")
                    .longType()
                    .defaultValue(536870912L)
                    .withDescription(
                            "Maximum downloaded ZIP bytes on worker temporary disk, 1 to 68719476736.");
    public static final Option<Long> MAX_UNCOMPRESSED_BYTES =
            Options.key("max_uncompressed_bytes")
                    .longType()
                    .defaultValue(2147483648L)
                    .withDescription(
                            "Maximum total ZIP-entry bytes and total decoded event bytes, separately, 1 to 68719476736.");
    public static final Option<Map<String, String>> FIELD_MAPPING =
            Options.key("field_mapping")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Schema column to RFC 6901 JSON Pointer; an empty pointer selects the whole event.");

    private AmplitudeSourceOptions() {}
}
