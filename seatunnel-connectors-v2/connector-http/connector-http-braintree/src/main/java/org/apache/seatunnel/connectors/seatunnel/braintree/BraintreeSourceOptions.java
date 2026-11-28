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
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Options for a bounded Braintree merchant scan.
 *
 * <p>The access and secret keys use SeaTunnel's default sensitive option names.
 */
public final class BraintreeSourceOptions {
    public static final Option<String> ENVIRONMENT = text("environment", "Production or Sandbox.");
    public static final Option<String> MERCHANT_ID =
            text("merchant_id", "Braintree gateway merchant ID.");
    public static final Option<String> ACCESS_KEY = text("access_key", "Braintree public API key.");
    public static final Option<String> SECRET_KEY =
            text("secret_key", "Braintree private API key.");
    public static final Option<String> START =
            text("start_date", "Inclusive RFC3339 start for searched resources.");
    public static final Option<String> END =
            text(
                    "end_date",
                    "Inclusive RFC3339 cutoff. Omit to freeze the current instant at start.");
    public static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional source-private PEM or base64 DER X.509 trust anchor.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(List.of())
                    .withDescription("Resource names. Omit to select all seven.");
    public static final Option<Integer> CONNECT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    public static final Option<Integer> REQUEST =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    public static final Option<Integer> OPERATION =
            number("operation_timeout_ms", 60000, "Deadline across all attempts of one read.");
    public static final Option<Integer> RETRIES =
            number("max_retries", 3, "Maximum eligible read retries.");
    public static final Option<Integer> RETRY_DELAY =
            number("max_retry_delay_ms", 30000, "Maximum Retry-After or backoff delay.");
    public static final Option<Integer> BYTES =
            number(
                    "max_response_bytes",
                    8388608,
                    "Largest XML response before and after decompression.");
    public static final Option<Integer> XML_DEPTH =
            number("max_xml_depth", 64, "Maximum nested XML element depth.");
    public static final Option<Integer> XML_NODES =
            number("max_xml_nodes", 500000, "Maximum XML element count.");
    public static final Option<Integer> PAGES =
            number("max_pages", 10000, "Maximum list pages or ID batches per resource.");
    public static final Option<Integer> WINDOWS =
            number(
                    "max_search_windows",
                    10000,
                    "Maximum search windows per searched resource, including subdivisions.");
    public static final Option<Integer> WINDOW_DAYS =
            number("search_window_days", 31, "Days in each initial search window.");
    public static final Option<Integer> RESULT_LIMIT =
            number(
                    "search_result_limit",
                    50000,
                    "Subdivide at this result count or the lower provider cap.");
    public static final Option<Integer> BATCH =
            number(
                    "max_ids_per_request",
                    1000,
                    "Maximum requested IDs even when the provider offers a larger page size.");

    private BraintreeSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(ENVIRONMENT, MERCHANT_ID, ACCESS_KEY, SECRET_KEY)
                .optional(
                        START,
                        END,
                        CERTIFICATE,
                        RESOURCES,
                        CONNECT,
                        REQUEST,
                        OPERATION,
                        RETRIES,
                        RETRY_DELAY,
                        BYTES,
                        XML_DEPTH,
                        XML_NODES,
                        PAGES,
                        WINDOWS,
                        WINDOW_DAYS,
                        RESULT_LIMIT,
                        BATCH)
                .build();
    }

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }
}
