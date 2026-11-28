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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Options for a bounded Basecamp account read.
 *
 * <p>Use a supplied Basecamp bearer token or the Launchpad refresh grant.
 */
public final class BasecampSourceOptions {
    static final Option<String> ACCOUNT = text("account_id", "Basecamp account ID.");
    static final Option<String> AGENT =
            text("user_agent", "Identifiable application name and contact for Basecamp.");
    static final Option<String> TOKEN = text("token", "Existing Basecamp bearer access token.");
    static final Option<String> CLIENT_ID = text("client_id", "Launchpad integration identifier.");
    static final Option<String> CLIENT_SECRET =
            text("client_secret", "Launchpad integration secret.");
    static final Option<String> REFRESH = text("refresh_token", "Launchpad OAuth refresh token.");
    static final Option<String> START =
            text("start_date", "Inclusive updated_at lower bound, required for recordings.");
    static final Option<String> END =
            text("end_date", "Inclusive updated_at upper bound, defaults to the batch clock.");
    static final Option<String> CERTIFICATE =
            text("tls_trust_certificate", "Optional X.509 trust anchor in PEM or base64 DER form.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Selected resources. Empty selects all five.");
    static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_collection",
                    10000,
                    "Maximum pages for the projects/feed scan or one schedule's entries.");
    static final Option<Integer> MAX_PAGE_ITEMS =
            number("max_page_items", 1000, "Maximum accepted items per page or project dock.");
    static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connection timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline for one read across authentication and retries.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Additional attempts for retryable failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum encoded and decoded response bytes.");

    private BasecampSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(ACCOUNT, AGENT)
                .exclusive(TOKEN, CLIENT_ID)
                .optional(
                        CLIENT_SECRET,
                        REFRESH,
                        START,
                        END,
                        CERTIFICATE,
                        RESOURCES,
                        MAX_PAGES,
                        MAX_PAGE_ITEMS,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        BODY_LIMIT)
                .build();
    }
}
