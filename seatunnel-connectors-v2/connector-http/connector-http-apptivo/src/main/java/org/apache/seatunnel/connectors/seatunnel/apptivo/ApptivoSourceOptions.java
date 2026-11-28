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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Stable Apptivo source configuration.
 *
 * <p>The two DAO query credentials use option names already masked by SeaTunnel.
 */
public final class ApptivoSourceOptions {
    static final Option<String> TOKEN = text("token", "Apptivo business API key.");
    static final Option<String> ACCESS_KEY = text("access_key", "Apptivo business access key.");
    static final Option<String> FIRM = text("firm_id", "Optional expected Apptivo firm ID.");
    static final Option<String> API =
            Options.key("api_origin")
                    .stringType()
                    .defaultValue("https://api.apptivo.com")
                    .withDescription("Fixed HTTPS origin for DAO reads.");
    static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional X.509 HTTPS trust certificate in PEM or base64 DER form.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Resources in read order. Empty selects all five.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 100, "Records requested per list page, from 1 through 100.");
    static final Option<Integer> MAX_PAGES =
            number("max_pages_per_resource", 1000, "Maximum pages per collection.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Total deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number("operation_timeout_ms", 120000, "Total deadline across a page's attempts.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum time between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Additional HTTPS attempts for transient failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum transfer and decoded JSON body bytes.");

    private ApptivoSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN, ACCESS_KEY)
                .optional(
                        FIRM,
                        API,
                        CERTIFICATE,
                        RESOURCES,
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
