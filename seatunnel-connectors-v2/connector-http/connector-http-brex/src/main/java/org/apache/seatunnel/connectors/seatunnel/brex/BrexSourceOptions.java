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
package org.apache.seatunnel.connectors.seatunnel.brex;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Defines the batch scan, credentials and request limits for the Brex source.
 *
 * <p>The token option uses SeaTunnel's built-in sensitive key matching.
 */
public final class BrexSourceOptions {
    public static final Option<String> TOKEN =
            text("token", "Read-scoped Brex dashboard user token.");
    public static final Option<List<String>> RESOURCES =
            list("resources", "Ordered subset by catalog order. Omit to read all six resources.");
    public static final Option<List<String>> USER_STATUSES =
            list("user_statuses", "Optional Team API status filters for the users resource.");
    public static final Option<String> START =
            text("start_date", "Earliest RFC 3339 timestamp for transactions and card expenses.");
    public static final Option<String> END =
            text("end_date", "Inclusive RFC 3339 cutoff. Defaults to the batch start instant.");
    public static final Option<String> TRANSACTION_RESUME =
            text("transactions_resume_date", "Prior UTC posting day. Replays one day before it.");
    public static final Option<String> EXPENSE_RESUME =
            text("expenses_resume_at", "Prior RFC 3339 purchase time. Replays one calendar month.");
    public static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional source-private PEM or base64 DER X.509 anchor.");
    public static final Option<Integer> PAGE_SIZE =
            number("page_size", 100, "Requested items per page, capped at 100 for card expenses.");
    public static final Option<Integer> MAX_PAGES =
            number("max_pages_per_resource", 10000, "Maximum accepted pages per resource.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    public static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    60000,
                    "Deadline across retries of one logical request.");
    public static final Option<Integer> INTERVAL =
            number("request_interval_ms", 65, "Minimum time between request attempts.");
    public static final Option<Integer> RETRIES =
            number("max_retries", 3, "Maximum retries after transient errors.");
    public static final Option<Integer> MAX_RETRY_DELAY =
            number("max_retry_delay_ms", 30000, "Maximum permitted Retry-After or backoff delay.");
    public static final Option<Integer> BODY_LIMIT =
            number(
                    "max_response_bytes",
                    8388608,
                    "Maximum response before and after decompression.");

    private BrexSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        RESOURCES,
                        USER_STATUSES,
                        START,
                        END,
                        TRANSACTION_RESUME,
                        EXPENSE_RESUME,
                        CERTIFICATE,
                        PAGE_SIZE,
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

    private static Option<List<String>> list(String key, String description) {
        return Options.key(key).listType().defaultValue(List.of()).withDescription(description);
    }

    private static Option<Integer> number(String key, int value, String description) {
        return Options.key(key).intType().defaultValue(value).withDescription(description);
    }
}
