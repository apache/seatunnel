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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Options for one bounded BambooHR resource per source instance.
 *
 * <p>The option rule exposes the same names and defaults to discovery and job validation.
 */
public final class BambooHRSourceOptions {
    public static final Option<String> COMPANY =
            Options.key("company")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Company subdomain for the BambooHR API.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BambooHR API key used as the Basic authentication username.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One supported BambooHR resource.");
    public static final Option<String> CUSTOM_REPORTS_FIELDS =
            Options.key("custom_reports_fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Comma-separated field IDs for the ad-hoc report.");
    public static final Option<String> EMPLOYEE_FIELDS =
            Options.key("employee_fields")
                    .stringType()
                    .defaultValue("firstName,lastName")
                    .withDescription("Comma-separated fields for employee details.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive date or offset date-time for time off and timesheet reads.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive date or offset date-time; defaults to the company's current date.");
    public static final Option<String> COMPANY_TIME_ZONE =
            Options.key("company_time_zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "Company time zone used for calendar windows and date-time conversion.");
    public static final Option<String> TIME_OFF_WATERMARK =
            Options.key("time_off_watermark")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prior time off creation date; up to 365 days of overlap.");
    public static final Option<String> TIMESHEET_WATERMARK =
            Options.key("timesheet_watermark")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prior timesheet date; up to 30 days of overlap.");
    public static final Option<String> EMPLOYEE_CHANGED_SINCE =
            Options.key("employee_changed_since")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive ISO 8601 timestamp for the optional changed-employee read.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum connection timeout in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Maximum active socket read timeout in milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Total request and retry budget in milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per read-only request, including the initial attempt.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum wire and decoded size of one response.");
    public static final Option<Integer> MAX_RECORDS_PER_RESPONSE =
            Options.key("max_records_per_response")
                    .intType()
                    .defaultValue(20000)
                    .withDescription(
                            "Maximum count of records or employee references in one response.");

    private BambooHRSourceOptions() {}
}
