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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * User-facing settings for one Datadog resource scan. Credential, time-scope and response limits
 * are validated before the reader starts.
 */
public final class DatadogSourceOptions {
    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Datadog API key.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Datadog application key.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Datadog resource to read. teams selects modern general organization teams.");
    public static final Option<String> SITE =
            Options.key("site")
                    .stringType()
                    .defaultValue("datadoghq.com")
                    .withDescription("Datadog site parameter.");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive ISO-8601 instant for logs or audit logs.");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive ISO-8601 instant for logs or audit logs. Defaults to the initial job time.");
    public static final Option<String> LOGS_QUERY =
            Options.key("logs_query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional Datadog log search expression.");
    public static final Option<Integer> SLICE_DAYS =
            Options.key("slice_days")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Days per event query window, from 1 to 31.");
    public static final Option<Integer> METRICS_WINDOW_SECONDS =
            Options.key("metrics_window_seconds")
                    .intType()
                    .defaultValue(5184000)
                    .withDescription(
                            "Metric catalog reporting lookback in seconds, from 0 to 5184000. Zero selects only metrics reported in the last few minutes.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Requested page size, from 1 to 1000, constrained further by the selected endpoint.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum time without I/O progress or during connection establishment, from 100 to 60000 ms.");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription(
                            "Total page time including retry waits and body reads, from 100 to 300000 ms.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Total attempts for one page, from 1 to 8.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decompressed HTTP response size in bytes, from 1024 to 16777216.");

    private DatadogSourceOptions() {}
}
