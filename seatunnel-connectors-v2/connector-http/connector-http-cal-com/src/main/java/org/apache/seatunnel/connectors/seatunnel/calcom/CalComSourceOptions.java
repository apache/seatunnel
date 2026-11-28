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
package org.apache.seatunnel.connectors.seatunnel.calcom;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Defines the Cal.com cloud API and batch scan options.
 *
 * <p>The personal API key uses the default sensitive name token.
 */
public final class CalComSourceOptions {
    static final String GROUPED_EVENTS = "2024-04-15";
    static final String FLAT_EVENTS = "2026-06-12";
    static final String OFFSET_BOOKINGS = "2024-08-13";
    static final String CURSOR_BOOKINGS = "2026-05-01";
    static final String SCHEDULE_VERSION = "2024-06-11";

    public static final Option<String> TOKEN = text("token", "Cal.com personal API key.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(List.of())
                    .withDescription("Resource names. An empty list selects all six.");
    public static final Option<String> EVENT_VERSION =
            Options.key("event_types_api_version")
                    .stringType()
                    .defaultValue(GROUPED_EVENTS)
                    .withDescription("Grouped 2024-04-15 or flat 2026-06-12 event types.");
    public static final Option<String> BOOKING_VERSION =
            Options.key("bookings_api_version")
                    .stringType()
                    .defaultValue(CURSOR_BOOKINGS)
                    .withDescription("Cursor 2026-05-01 or offset 2024-08-13 bookings.");
    public static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional Cal.com source-local PEM or base64 DER X.509 anchor.");
    public static final Option<Integer> BOOKING_LIMIT =
            number("bookings_page_size", 100, "Bookings returned in one page, from 1 to 100.");
    public static final Option<Integer> FULL_LIST_LIMIT =
            number("max_items_per_response", 10000, "Maximum records in one unpaginated result.");
    public static final Option<Integer> MAX_PAGES =
            number("max_pages_per_resource", 10000, "Maximum accepted pages per resource.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connection timeout.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    public static final Option<Integer> OPERATION_TIMEOUT =
            number("operation_timeout_ms", 60000, "Deadline across attempts of one logical read.");
    public static final Option<Integer> INTERVAL =
            number("request_interval_ms", 600, "Minimum time between requests.");
    public static final Option<Integer> RETRIES =
            number("max_retries", 3, "Maximum additional attempts after transient read errors.");
    public static final Option<Integer> MAX_RETRY_DELAY =
            number("max_retry_delay_ms", 30000, "Maximum permitted Retry-After or backoff delay.");
    public static final Option<Integer> BODY_LIMIT =
            number(
                    "max_response_bytes",
                    8388608,
                    "Maximum response before and after decompression.");

    private CalComSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        RESOURCES,
                        EVENT_VERSION,
                        BOOKING_VERSION,
                        CERTIFICATE,
                        BOOKING_LIMIT,
                        FULL_LIST_LIMIT,
                        MAX_PAGES,
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

    private static Option<Integer> number(String name, int value, String description) {
        return Options.key(name).intType().defaultValue(value).withDescription(description);
    }
}
