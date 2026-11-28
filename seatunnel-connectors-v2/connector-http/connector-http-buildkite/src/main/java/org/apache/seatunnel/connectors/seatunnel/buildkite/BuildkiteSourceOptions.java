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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Defines the Buildkite REST source's batch, authentication and request settings.
 *
 * <p>The access token uses SeaTunnel's default sensitive option name.
 */
public final class BuildkiteSourceOptions {
    public static final Option<String> TOKEN =
            text("token", "Read-scoped Buildkite API access token.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(List.of())
                    .withDescription("Resource names. An empty list selects all twelve.");
    public static final Option<String> START =
            text("start_date", "Inclusive RFC 3339 creation time for dated collections.");
    public static final Option<String> END =
            text("end_date", "Exclusive RFC 3339 cutoff. Defaults to the batch start instant.");
    public static final Option<String> CERTIFICATE =
            text("tls_trust_certificate", "Optional source-local PEM or base64 DER X.509 anchor.");
    public static final Option<Integer> PAGE_SIZE =
            number(
                    "page_size",
                    100,
                    "Requested items per page for standard paginated collections.");
    public static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_collection",
                    10000,
                    "Maximum pages per collection under one parent.");
    public static final Option<Integer> MAX_SCAN_PAGES =
            number("max_pages_per_scan", 100000, "Maximum accepted responses across the scan.");
    public static final Option<Integer> MAX_UNPAGED =
            number("max_unpaginated_items", 10000, "Maximum items in a full-list response.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    public static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across retries of one logical request.");
    public static final Option<Integer> INTERVAL =
            number("request_interval_ms", 100, "Minimum time between request attempts.");
    public static final Option<Integer> RETRIES =
            number("max_retries", 3, "Maximum retries after transient read errors.");
    public static final Option<Integer> MAX_RETRY_DELAY =
            number("max_retry_delay_ms", 60000, "Maximum permitted server retry delay or backoff.");
    public static final Option<Integer> BODY_LIMIT =
            number(
                    "max_response_bytes",
                    8388608,
                    "Maximum response before and after decompression.");

    private BuildkiteSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        RESOURCES,
                        START,
                        END,
                        CERTIFICATE,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_SCAN_PAGES,
                        MAX_UNPAGED,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        MAX_RETRY_DELAY,
                        BODY_LIMIT)
                .build();
    }

    private static Option<String> text(String key, String description) {
        return Options.key(key).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String key, int value, String description) {
        return Options.key(key).intType().defaultValue(value).withDescription(description);
    }
}
