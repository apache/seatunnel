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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class SentrySourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bearer token with project:read access; not a sink DSN.");
    public static final Option<String> ORGANIZATION =
            Options.key("organization")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sentry organization ID or slug.");
    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sentry project ID or slug.");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Absolute RFC3339 start of the event query.");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Absolute RFC3339 end of the event query.");
    public static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .defaultValue("https://sentry.io")
                    .withDescription(
                            "HTTPS origin of Sentry or a self-hosted deployment; no path.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of events requested per page, 1 to 100.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Fail rather than truncate if the query requires more pages, 1 to 100000.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts for transport failures and HTTP 429/500/502/503/504, 0 to 5.");
    public static final Option<Integer> RETRY_DELAY_MS =
            Options.key("retry_delay_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Minimum retry delay, 1 to 60000 milliseconds.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Per-attempt HTTP connect/socket timeout and abort deadline, 1 to 120000 milliseconds; not a total job deadline.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum uncompressed response size, 1024 to 16777216 bytes.");
    public static final Option<Boolean> MOCK_MODE =
            Options.key("mock_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow HTTP fixtures only with token=mock-token; never use with real credentials.");

    private SentrySourceOptions() {}
}
