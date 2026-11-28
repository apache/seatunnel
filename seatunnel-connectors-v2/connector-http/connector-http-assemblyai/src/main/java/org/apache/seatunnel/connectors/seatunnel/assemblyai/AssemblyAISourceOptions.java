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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Configuration contract for AssemblyAI pre-recorded transcript reads.
 *
 * <p>The raw API key uses the default-masked token option. Date bounds apply equally to the
 * transcript catalog and every selected child.
 */
public final class AssemblyAISourceOptions {
    static final Option<String> TOKEN = text("token", "AssemblyAI API key.");
    static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("US")
                    .withDescription("Default assembly endpoint region: US or EU.");
    static final Option<String> API = text("api_origin", "Optional fixed HTTPS API origin.");
    static final Option<String> CERTIFICATE =
            text(
                    "tls_trust_certificate",
                    "Optional X.509 trust certificate in PEM or base64 DER form.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType(String.class)
                    .defaultValue(List.of())
                    .withDescription("Resources in per-transcript order. Empty selects all.");
    static final Option<String> START =
            text(
                    "start_date",
                    "Inclusive transcript creation timestamp. Defaults to 30 days before the batch clock.");
    static final Option<String> END =
            text(
                    "end_date",
                    "Inclusive transcript creation timestamp. Defaults to the batch clock.");
    static final Option<Integer> PAGE_SIZE =
            number("page_size", 100, "Transcript page size, from 1 to 200.");
    static final Option<Integer> MAX_PAGES =
            number("max_pages", 1000, "Maximum transcript list pages in the complete batch.");
    static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    static final Option<Integer> OPERATION_TIMEOUT =
            number("operation_timeout_ms", 120000, "Deadline across all attempts for one read.");
    static final Option<Integer> INTERVAL =
            number("min_request_interval_ms", 100, "Minimum interval between HTTPS attempts.");
    static final Option<Integer> RETRIES =
            number(
                    "max_retries",
                    3,
                    "Additional attempts for transient HTTP or connection failures.");
    static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 4194304, "Maximum transfer and decoded JSON bytes.");

    private AssemblyAISourceOptions() {}

    private static Option<String> text(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<Integer> number(String name, int fallback, String description) {
        return Options.key(name).intType().defaultValue(fallback).withDescription(description);
    }

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        REGION,
                        API,
                        CERTIFICATE,
                        RESOURCES,
                        START,
                        END,
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
