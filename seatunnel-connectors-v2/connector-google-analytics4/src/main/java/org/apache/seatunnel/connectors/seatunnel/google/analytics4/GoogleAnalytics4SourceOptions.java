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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

public final class GoogleAnalytics4SourceOptions {
    private GoogleAnalytics4SourceOptions() {}

    public static final Option<String> PROPERTY_ID =
            Options.key("property_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One numeric GA4 property ID.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive absolute start date, yyyy-MM-dd in the property time zone.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive absolute end date, yyyy-MM-dd in the property time zone.");
    public static final Option<List<String>> DIMENSIONS =
            Options.key("dimensions")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Ordered dimension API names, at most 9.");
    public static final Option<List<String>> METRICS =
            Options.key("metrics")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ordered metric API names, 1 to 10.");
    public static final Option<List<String>> METRIC_TYPES =
            Options.key("metric_types")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Exact Google MetricType enum for each metric; checked on every response.");
    public static final Option<String> KEY_FILE =
            Options.key("service_account_key_file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted service account JSON file available on the worker; mutually exclusive with emulator_url.");
    public static final Option<String> EMULATOR_URL =
            Options.key("emulator_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Explicit HTTP mock server origin. Disables authentication; never use for Google Analytics.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Rows requested per page, 1 to 10000.");
    public static final Option<Integer> MAX_ROWS =
            Options.key("max_report_rows")
                    .intType()
                    .defaultValue(1000000)
                    .withDescription(
                            "Fail, never truncate, if rowCount exceeds this bound (1 to 10000000).");
    public static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Maximum response bytes per page, 1024 to 16777216.");
    public static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total HTTP request deadline including response body, 100 to 120000 ms.");
    public static final Option<Integer> REPORT_TIMEOUT =
            Options.key("report_timeout_ms")
                    .intType()
                    .defaultValue(600000)
                    .withDescription(
                            "Whole report deadline checked between rows/requests, 100 to 3600000 ms.");
    public static final Option<Integer> RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts per report page for 429, 500, 502, 503, 504 or transport failure, 0 to 5.");
    public static final Option<Integer> BACKOFF =
            Options.key("max_retry_wait_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Maximum retry wait, 1000 to 120000 ms. Longer Retry-After fails the job instead of retrying early.");
}
