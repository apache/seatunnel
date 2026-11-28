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
package org.apache.seatunnel.connectors.seatunnel.appleads;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * User-facing configuration for the Apple Ads read-only source.
 *
 * <p>Platform and legacy IDs are distinct conditional requirements. The externally signed Apple
 * token-exchange credential uses the log-masked client_secret key.
 */
public final class AppleAdsSourceOptions {
    static final Option<String> MODE =
            Options.key("api_mode")
                    .stringType()
                    .defaultValue("PLATFORM_V1")
                    .withDescription("Apple API protocol: PLATFORM_V1 or explicit LEGACY_V5.");
    static final Option<String> ACCOUNT = text("ad_account_id", "Platform v1 ad account ID.");
    static final Option<String> ORG = text("org_id", "Campaign Management v5 organization ID.");
    static final Option<String> CLIENT_ID = text("client_id", "Apple Ads API user client ID.");
    static final Option<String> CLIENT_SECRET =
            text("client_secret", "Externally signed Apple Ads ES256 client-secret JWT.");
    static final Option<String> API =
            text("api_origin", "Override the HTTPS origin for the selected API mode.");
    static final Option<String> OAUTH =
            Options.key("oauth_origin")
                    .stringType()
                    .defaultValue("https://appleid.apple.com")
                    .withDescription("HTTPS origin for Apple Ads token exchange.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Datasets in requested order. Empty selects all eight.");
    static final Option<String> START_DATE =
            text("start_date", "Inclusive daily report start date.");
    static final Option<String> END_DATE =
            text("end_date", "Inclusive report end date; defaults to yesterday.");
    static final Option<String> CURSOR_DATE =
            text(
                    "report_cursor_date",
                    "Optional previously processed report date for a new batch.");
    static final Option<String> TIMEZONE =
            Options.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("Report calendar used by Apple: UTC or ORTZ.");
    static final Option<String> ORG_ZONE =
            text(
                    "organization_timezone",
                    "IANA zone matching the organization's reporting timezone for ORTZ.");
    static final Option<Integer> LOOKBACK =
            number("lookback_days", 30, "Dates replayed before a supplied report cursor.");
    static final Option<Integer> WINDOW =
            number("report_window_days", 7, "Maximum inclusive days per daily-report request.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 1000, "Page size for a parent relationship or report, up to 1000.");
    static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_partition",
                    10000,
                    "Maximum pages for one parent or report window.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for a complete HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across token acquisition and API attempts.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS requests.");
    static final Option<Integer> RETRIES =
            number(
                    "max_retries",
                    3,
                    "Additional attempts for retryable statuses or safe API reads.");
    static final Option<Integer> BODY_LIMIT =
            number(
                    "max_response_bytes",
                    16777216,
                    "Maximum response transfer or decoded JSON bytes.");

    private AppleAdsSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int defaultValue, String description) {
        return Options.key(name).intType().defaultValue(defaultValue).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(CLIENT_ID, CLIENT_SECRET)
                .optional(
                        MODE,
                        ACCOUNT,
                        ORG,
                        API,
                        OAUTH,
                        RESOURCES,
                        START_DATE,
                        END_DATE,
                        CURSOR_DATE,
                        TIMEZONE,
                        ORG_ZONE,
                        LOOKBACK,
                        WINDOW,
                        PAGE_SIZE,
                        MAX_PAGES,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        BODY_LIMIT)
                .build();
    }
}
