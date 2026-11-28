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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.Collections;
import java.util.List;

/**
 * Stable job settings for App Store Connect Source.
 *
 * <p>The signing key uses a name that SeaTunnel masks in job configuration output.
 */
public final class AppStoreConnectSourceOptions {
    static final Option<String> ISSUER_ID =
            text("issuer_id", "Issuer ID of an App Store Connect team API key.");
    static final Option<String> KEY_ID =
            text("key_id", "ID of the App Store Connect team API key.");
    static final Option<String> SECRET_KEY =
            text("secret_key", "P-256 PKCS#8 private key in PEM or base64 DER form.");
    static final Option<String> VENDOR_NUMBER =
            text("vendor_number", "Vendor number required for Sales and Trends or finance.");
    static final Option<String> API_ORIGIN =
            Options.key("api_origin")
                    .stringType()
                    .defaultValue("https://api.appstoreconnect.apple.com")
                    .withDescription("HTTPS origin of the App Store Connect API.");
    static final Option<List<String>> RESOURCES =
            list(
                    "resources",
                    "Resource names in read order. Empty selects all available resources.");
    static final Option<List<String>> DOWNLOAD_HOSTS =
            list("download_hosts", "Additional exact HTTPS hosts allowed for signed segments.");
    static final Option<String> SALES_START =
            text("sales_reports_start_date", "Inclusive Sales and Trends start date (YYYY-MM-DD).");
    static final Option<String> SUBSCRIPTION_EVENT_START =
            text(
                    "subscription_event_reports_start_date",
                    "Inclusive subscription events start date (YYYY-MM-DD).");
    static final Option<List<String>> SUBSCRIPTION_EVENT_SKIP =
            list("subscription_event_skip_dates", "Subscription event dates to skip locally.");
    static final Option<String> FINANCE_START =
            text("finance_reports_start_date", "Inclusive fiscal report month (YYYY-MM).");
    static final Option<String> ANALYTICS_START =
            text("analytics_reports_start_date", "Inclusive ongoing processing date (YYYY-MM-DD).");
    static final Option<String> REVIEWS_START =
            text(
                    "reviews_start_date",
                    "Inclusive customer review timestamp (ISO-8601 with offset).");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 200, "Requested page size, capped by each Apple endpoint.");
    static final Option<Integer> MAX_PAGES =
            number("max_pages_per_parent", 1000, "Maximum pages per relationship and parent.");
    static final Option<Integer> MIN_INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS requests.");
    static final Option<Integer> TIMEOUT =
            number("request_timeout_ms", 30000, "Total deadline for a single HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Total deadline including retries and URL renewal.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Retries for transient provider/transport errors.");
    static final Option<Integer> RESPONSE_LIMIT =
            number(
                    "max_json_bytes",
                    4194304,
                    "Maximum decoded JSON payload or metadata error size.");
    static final Option<Integer> WIRE_LIMIT =
            number("max_compressed_report_bytes", 33554432, "Maximum compressed report bytes.");
    static final Option<Integer> DECODED_LIMIT =
            number("max_decoded_report_bytes", 134217728, "Maximum decompressed report bytes.");
    static final Option<Integer> ROW_LIMIT =
            number(
                    "max_report_row_chars",
                    262144,
                    "Maximum decoded UTF-16 code units per TSV row.");

    private AppStoreConnectSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<List<String>> list(String name, String description) {
        return Options.key(name)
                .listType(String.class)
                .defaultValue(Collections.emptyList())
                .withDescription(description);
    }

    private static Option<Integer> number(String name, int value, String description) {
        return Options.key(name).intType().defaultValue(value).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(ISSUER_ID, KEY_ID, SECRET_KEY)
                .optional(
                        VENDOR_NUMBER,
                        API_ORIGIN,
                        RESOURCES,
                        DOWNLOAD_HOSTS,
                        SALES_START,
                        SUBSCRIPTION_EVENT_START,
                        SUBSCRIPTION_EVENT_SKIP,
                        FINANCE_START,
                        ANALYTICS_START,
                        REVIEWS_START,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MIN_INTERVAL,
                        TIMEOUT,
                        OPERATION_TIMEOUT,
                        RETRIES,
                        RESPONSE_LIMIT,
                        WIRE_LIMIT,
                        DECODED_LIMIT,
                        ROW_LIMIT)
                .build();
    }
}
