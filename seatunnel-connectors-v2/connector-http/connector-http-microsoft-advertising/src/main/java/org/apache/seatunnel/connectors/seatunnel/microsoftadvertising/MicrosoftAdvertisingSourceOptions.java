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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.Collections;
import java.util.List;

/**
 * Public configuration for one bounded Microsoft Advertising scan.
 *
 * <p>Each limit applies to the actual provider operation it names.
 */
public final class MicrosoftAdvertisingSourceOptions {
    public static final Option<String> DEVELOPER_TOKEN =
            text("developer_token", "Microsoft Advertising developer token.");
    public static final Option<String> CLIENT_ID =
            text("client_id", "Microsoft Entra application client ID.");
    public static final Option<String> REFRESH_TOKEN =
            text(
                    "refresh_token",
                    "Microsoft Entra OAuth refresh token with Microsoft Advertising consent.");
    public static final Option<String> CLIENT_SECRET =
            text("client_secret", "", "Confidential Entra application secret.");
    public static final Option<String> TENANT =
            text("tenant_id", "common", "Microsoft Entra tenant ID or authority alias.");
    public static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional PEM or base64 DER X.509 trust anchor local to this source.");
    public static final Option<String> START =
            text("reports_start_date", "First requested London calendar date, in YYYY-MM-DD.");
    public static final Option<String> RESUME =
            text(
                    "reports_resume_date",
                    "Optional prior reporting cursor date used with lookback_window.");
    public static final Option<String> END =
            text("reports_end_date", "Last requested London calendar date, in YYYY-MM-DD.");
    public static final Option<String> BULK_FROM =
            text(
                    "bulk_sync_from",
                    "Optional timestamp with offset for an initial bulk delta download.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Resource names. An empty list selects every fixed resource and configured custom report.");
    public static final Option<List<JsonNode>> ACCOUNT_NAMES =
            objects("account_names", "Optional account name predicates with operator and name.");
    public static final Option<List<JsonNode>> CUSTOM_REPORTS =
            objects(
                    "custom_reports",
                    "Custom reporting object, columns, aggregation and resource name.");
    public static final Option<Integer> LOOKBACK =
            number(
                    "lookback_window",
                    0,
                    "Days to replay before reports_resume_date without passing reports_start_date.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connection timeout.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            number(
                    "request_timeout_ms",
                    30000,
                    "Deadline for one JSON exchange including read retries.");
    public static final Option<Integer> DOWNLOAD_TIMEOUT =
            number(
                    "download_timeout_ms",
                    300000,
                    "Whole-file download and decoding deadline including retries.");
    public static final Option<Integer> POLL_INTERVAL =
            number("poll_interval_ms", 5000, "Delay between asynchronous job polls.");
    public static final Option<Integer> POLL_TIMEOUT =
            number(
                    "poll_timeout_ms",
                    3600000,
                    "Time allowed for the return of a completed asynchronous job.");
    public static final Option<Integer> RETRIES =
            number(
                    "max_retries",
                    3,
                    "Retries after eligible transport failures and rate-limit rejections.");
    public static final Option<Integer> RETRY_DELAY =
            number("max_retry_delay_ms", 30000, "Maximum permitted Retry-After or backoff.");
    public static final Option<Integer> JSON_BYTES =
            number(
                    "max_response_bytes",
                    8388608,
                    "Maximum JSON response bytes on the wire and after decompression.");
    public static final Option<Integer> COMPRESSED_BYTES =
            number("max_compressed_bytes", 134217728, "Maximum compressed download bytes.");
    public static final Option<Integer> EXPANDED_BYTES =
            number("max_expanded_bytes", 536870912, "Maximum expanded CSV bytes.");
    public static final Option<Integer> CSV_ROW =
            number("max_csv_row_chars", 262144, "Maximum characters in one logical CSV record.");
    public static final Option<Integer> CSV_COLUMNS =
            number("max_csv_columns", 512, "Maximum number of CSV columns.");
    public static final Option<Integer> CSV_ROWS =
            number("max_csv_rows", 10000000, "Maximum records per download including metadata.");
    public static final Option<Integer> ACCOUNT_PAGES =
            number("max_account_pages", 10000, "Maximum SearchAccounts pages across predicates.");
    public static final Option<Integer> ACCOUNTS =
            number("max_accounts", 10000, "Maximum distinct accounts retained for the scan.");

    private MicrosoftAdvertisingSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(DEVELOPER_TOKEN, CLIENT_ID, REFRESH_TOKEN)
                .optional(
                        CLIENT_SECRET,
                        TENANT,
                        CERTIFICATE,
                        START,
                        RESUME,
                        END,
                        BULK_FROM,
                        RESOURCES,
                        ACCOUNT_NAMES,
                        CUSTOM_REPORTS,
                        LOOKBACK,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        DOWNLOAD_TIMEOUT,
                        POLL_INTERVAL,
                        POLL_TIMEOUT,
                        RETRIES,
                        RETRY_DELAY,
                        JSON_BYTES,
                        COMPRESSED_BYTES,
                        EXPANDED_BYTES,
                        CSV_ROW,
                        CSV_COLUMNS,
                        CSV_ROWS,
                        ACCOUNT_PAGES,
                        ACCOUNTS)
                .build();
    }

    private static Option<String> text(String key, String description) {
        return Options.key(key).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<String> text(String key, String value, String description) {
        return Options.key(key).stringType().defaultValue(value).withDescription(description);
    }

    private static Option<List<JsonNode>> objects(String key, String description) {
        return Options.key(key)
                .listType(JsonNode.class)
                .defaultValue(Collections.emptyList())
                .withDescription(description);
    }

    private static Option<Integer> number(String key, int value, String description) {
        return Options.key(key).intType().defaultValue(value).withDescription(description);
    }
}
