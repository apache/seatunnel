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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * User options for the bounded Calendly source.
 *
 * <p>Each block exports one selected resource using a Calendly personal access token.
 */
public final class CalendlySourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Calendly personal access token.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One supported Calendly export resource.");
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Calendly OAuth client identifier.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Calendly OAuth client secret.");
    public static final Option<String> OAUTH_TOKEN_FILE =
            Options.key("oauth_token_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute path to pre-provisioned private OAuth refresh state on each eligible worker.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .defaultValue("1970-01-01T00:00:00Z")
                    .withDescription(
                            "RFC 3339 start date for date-window resources and scheduled event start time.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "RFC 3339 upper cursor bound. Defaults to the first read time.");
    public static final Option<String> INCREMENTAL_START =
            Options.key("incremental_start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Prior cursor high-water mark for the selected incremental resource.");
    public static final Option<Integer> LOOKBACK_DAYS =
            Options.key("lookback_days")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Extra replay days for scheduled_events, in addition to five minutes.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of records requested per cursor page, from 1 to 100.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Maximum collection pages across hidden parents and selected records, including empty pages.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum TCP connect time in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Maximum socket read time in milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(90000)
                    .withDescription(
                            "Total API read, OAuth exchange and retry budget in milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTP attempts, including the initial request.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum wire and decoded response size in bytes.");
    public static final Option<String> TLS_TRUST_CERTIFICATE =
            Options.key("tls_trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Engine-local PEM or DER trust anchor file. Defaults to JVM trust.");

    private CalendlySourceOptions() {}
}
