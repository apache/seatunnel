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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;
import java.util.List;

/** Options for bounded Acuity resource and civil-date crawls. */
public final class AcuitySchedulingSourceOptions {
    public static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://acuityscheduling.com")
                    .withDescription("Trusted Acuity API HTTPS origin.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Acuity numeric User ID.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Acuity API key for HTTP Basic authentication.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(
                            Arrays.asList(
                                    "appointments",
                                    "calendars",
                                    "clients",
                                    "appointment-types",
                                    "blocks",
                                    "labels",
                                    "forms"))
                    .withDescription("Selected Acuity resources in crawl order.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("First included business calendar date for dated resources.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Last included calendar date. Defaults to the current date in timezone at crawl start.");
    public static final Option<String> RESUME_DATE =
            Options.key("resume_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional previous successful boundary to reread with lookback_days.");
    public static final Option<String> TIMEZONE =
            Options.key("timezone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "IANA business or calendar zone used to resolve a default end date.");
    public static final Option<Integer> LOOKBACK_DAYS =
            Options.key("lookback_days")
                    .intType()
                    .defaultValue(7)
                    .withDescription(
                            "Civil calendar days to reread before resume_date, bounded by start_date.");
    public static final Option<Integer> WINDOW_DAYS =
            Options.key("window_days")
                    .intType()
                    .defaultValue(31)
                    .withDescription(
                            "Maximum initial date window; saturated windows are subdivided.");
    public static final Option<Integer> DATE_LIMIT =
            Options.key("max_date_results")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum records requested per dated window. An equally sized response may be truncated.");
    public static final Option<Integer> CALENDAR_ID =
            Options.key("calendar_id")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional calendar identifier filter for appointments and blocks.");
    public static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Limit on logical window requests and split probes in one crawl.");
    public static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Maximum compressed and decoded response bytes.");
    public static final Option<Integer> MAX_ROWS =
            Options.key("max_records_per_response")
                    .intType()
                    .defaultValue(20000)
                    .withDescription("Maximum records accepted in a full-list response.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection timeout in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "TLS handshake and response read inactivity timeout in milliseconds.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Total request deadline across network, parsing and retries.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum HTTP attempts per logical request including the first.");

    private AcuitySchedulingSourceOptions() {}
}
