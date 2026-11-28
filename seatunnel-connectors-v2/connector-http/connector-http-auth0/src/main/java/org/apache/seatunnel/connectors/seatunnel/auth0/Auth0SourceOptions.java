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
package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Configuration for a bounded Auth0 Management API read.
 *
 * <p>Choose either an existing Management token or the OAuth client credentials grant.
 */
public final class Auth0SourceOptions {
    static final Option<String> TENANT = text("tenant_url", "Fixed HTTPS Auth0 tenant origin.");
    static final Option<Boolean> CUSTOM =
            Options.key("allow_custom_domain")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Allow an explicitly trusted nonstandard tenant host.");
    static final Option<String> CERTIFICATE =
            text("tls_trust_certificate", "Optional X.509 trust anchor in PEM or base64 DER form.");
    static final Option<String> TOKEN = text("token", "Auth0 Management API bearer token.");
    static final Option<String> CLIENT_ID = text("client_id", "Auth0 M2M application identifier.");
    static final Option<String> CLIENT_SECRET =
            text("client_secret", "Auth0 M2M application secret.");
    static final Option<String> AUDIENCE =
            text("audience", "Auth0 Management API audience identifier.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Resources to read. Empty selects all five.");
    static final Option<String> START =
            Options.key("start_date")
                    .stringType()
                    .defaultValue("2013-01-01T00:00:00.000Z")
                    .withDescription("Inclusive lower bound for user updates.");
    static final Option<String> END =
            text("end_date", "Inclusive user update time, defaults to the batch start time.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 50, "Request page size, from 1 to 100.");
    static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_resource",
                    10000,
                    "Maximum logical requests per API resource across the complete batch, including user count probes.");
    static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across request attempts and OAuth renewal for one logical read.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number("max_retries", 3, "Additional attempts for retryable read failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum encoded and decoded response bytes.");

    private Auth0SourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TENANT)
                .exclusive(TOKEN, CLIENT_ID)
                .optional(
                        CLIENT_SECRET,
                        AUDIENCE,
                        CUSTOM,
                        CERTIFICATE,
                        RESOURCES,
                        START,
                        END,
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
