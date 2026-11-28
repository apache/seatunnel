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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Configuration for a bounded Avni integration API read.
 *
 * <p>The account password is used only with the separately configured Cognito HTTPS origin.
 */
public final class AvniSourceOptions {
    static final String DEFAULT_API = "https://app.avniproject.org";
    static final String DEFAULT_IDENTITY = "https://cognito-idp.ap-south-1.amazonaws.com";
    static final Option<String> USERNAME = text("username", "Avni integration account username.");
    static final Option<String> PASSWORD = text("password", "Avni integration account password.");
    static final Option<String> START = text("start_date", "Exclusive lower bound for audits.");
    static final Option<String> END =
            text("end_date", "Optional upper bound before the batch cutoff.");
    static final Option<String> API =
            Options.key("api_origin")
                    .stringType()
                    .defaultValue(DEFAULT_API)
                    .withDescription("Fixed HTTPS origin of the Avni integration API.");
    static final Option<String> IDENTITY =
            Options.key("identity_origin")
                    .stringType()
                    .defaultValue(DEFAULT_IDENTITY)
                    .withDescription("Fixed HTTPS origin of the Cognito identity service.");
    static final Option<Boolean> CUSTOM =
            Options.key("allow_custom_endpoints")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allow explicitly trusted nonstandard origins.");
    static final Option<String> CERTIFICATE =
            text("tls_trust_certificate", "Optional X.509 trust anchor in PEM or base64 DER form.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Resources to read. Empty selects all four.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 100, "Requested page size from 1 to 1000.");
    static final Option<Integer> MAX_PAGES =
            number("max_pages_per_resource", 10000, "Maximum pages for one resource.");
    static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across attempts and authentication for one logical read.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Additional attempts for retryable failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum encoded and decoded response bytes.");

    private AvniSourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(USERNAME, PASSWORD, START)
                .optional(
                        API,
                        IDENTITY,
                        CUSTOM,
                        END,
                        CERTIFICATE,
                        RESOURCES,
                        PAGE_SIZE,
                        MAX_PAGES,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        BODY_LIMIT)
                .build();
    }
}
