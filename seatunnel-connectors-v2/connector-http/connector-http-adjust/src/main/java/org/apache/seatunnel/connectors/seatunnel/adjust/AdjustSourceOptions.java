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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Declares the daily JSON report selection and request settings. */
public final class AdjustSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Adjust API bearer token.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("First report day, inclusive, in YYYY-MM-DD format.");
    public static final Option<List<String>> DIMENSIONS =
            Options.key("dimensions")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Ordered Adjust dimension slugs. The day dimension is always included.");
    public static final Option<List<String>> METRICS =
            Options.key("metrics")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ordered nonempty list of Adjust metric slugs.");
    public static final Option<List<String>> ADDITIONAL_METRICS =
            Options.key("additional_metrics")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Additional metrics, such as cohort or app-specific metrics.");
    public static final Option<Map<String, String>> METRIC_SCALES =
            Options.key("metric_scales")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Decimal scale overrides for selected metrics. Default scale is 18.");
    public static final Option<Boolean> UNTIL_TODAY =
            Options.key("until_today")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Include today in the fixed report window instead of stopping at yesterday.");
    public static final Option<String> UTC_OFFSET =
            Options.key("utc_offset")
                    .stringType()
                    .defaultValue("+00:00")
                    .withDescription(
                            "Fixed offset used by the report and the job's calendar window.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://automate.adjust.com")
                    .withDescription(
                            "Trusted API origin, without endpoint path or user information.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow cleartext HTTP for an explicitly trusted, isolated origin.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total deadline for one report day, including retries and backoff.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTP attempts for one report day, from 1 to 8.");

    private AdjustSourceOptions() {}
}
