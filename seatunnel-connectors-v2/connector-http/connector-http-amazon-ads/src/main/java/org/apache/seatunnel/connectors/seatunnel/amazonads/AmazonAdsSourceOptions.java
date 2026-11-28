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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

/**
 * Stable options for selecting Ads resources, profiles, reporting dates and bounded downloads.
 *
 * <p>Endpoint overrides, credential settings and resource selection stay local to the product.
 */
public final class AmazonAdsSourceOptions {
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Login with Amazon client identifier.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Login with Amazon client secret.");
    public static final Option<String> REFRESH_TOKEN =
            Options.key("refresh_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Refresh token granted for the Amazon Ads API.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("NA")
                    .withDescription("Regional advertising service, NA, EU, or FE.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional HTTPS override of the selected regional Ads API origin.");
    public static final Option<String> OAUTH_BASE_URL =
            Options.key("oauth_base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional HTTPS origin for the Login with Amazon token service.");
    public static final Option<List<String>> DOWNLOAD_HOSTS =
            Options.key("download_hosts")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Additional exact HTTPS hosts allowed to serve signed downloads.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Unique resource names to read, all 59 when absent.");
    public static final Option<List<String>> PROFILES =
            Options.key("profiles")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Profile IDs selected for child data, combined with marketplace IDs.");
    public static final Option<List<String>> MARKETPLACE_IDS =
            Options.key("marketplace_ids")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Marketplace IDs selected for child data, combined with profile IDs.");
    public static final Option<List<String>> STATE_FILTER =
            Options.key("state_filter")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Optional ENABLED, PAUSED or ARCHIVED states for Sponsored Product and Brand campaigns.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional inclusive lower report date in YYYY-MM-DD.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional inclusive upper report date in YYYY-MM-DD.");
    public static final Option<Integer> LOOK_BACK_WINDOW =
            Options.key("look_back_window")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional overlapping days on a default rolling reporting scan, from 0 to 30.");
    public static final Option<Integer> REPORT_WINDOW_DAYS =
            Options.key("report_window_days")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Maximum days per asynchronous report, from 1 to 30.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Page size for supported read endpoints, from 1 to 100.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum pages per profile and resource, from 1 to 10000.");
    public static final Option<Integer> MAX_PROFILES =
            Options.key("max_profiles")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum profiles loaded from the array endpoint, from 1 to 10000.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per eligible API operation, from 1 to 8.");
    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Minimum request spacing, from 10 to 60000 milliseconds.");
    public static final Option<Integer> IO_TIMEOUT_MS =
            Options.key("io_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription(
                            "Connection and read idle timeout, from 100 to 120000 milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Total operation budget for API calls and retries, from 500 to 900000 milliseconds.");
    public static final Option<Integer> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Interval for checking a pending report, from 100 to 60000 milliseconds.");
    public static final Option<Integer> REPORT_TIMEOUT_MS =
            Options.key("report_timeout_ms")
                    .intType()
                    .defaultValue(10800000)
                    .withDescription(
                            "Report processing timeout, from 1000 to 14400000 milliseconds.");
    public static final Option<Integer> DUPLICATE_WAIT_MS =
            Options.key("duplicate_wait_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Wait before retrying a non-reusable duplicate, from 100 to 120000 milliseconds.");
    public static final Option<Integer> DOWNLOAD_DEADLINE_MS =
            Options.key("download_deadline_ms")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "Maximum duration per report download including decoding, from 500 to 900000 milliseconds.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decoded API response size, from 1024 to 33554432 bytes.");
    public static final Option<Integer> MAX_DOWNLOAD_BYTES =
            Options.key("max_download_bytes")
                    .intType()
                    .defaultValue(67108864)
                    .withDescription("Maximum compressed report bytes, from 1024 to 268435456.");
    public static final Option<Integer> MAX_REPORT_BYTES =
            Options.key("max_report_bytes")
                    .intType()
                    .defaultValue(268435456)
                    .withDescription("Maximum decoded report bytes, from 1024 to 1073741824.");
    public static final Option<Integer> MAX_REPORT_ROWS =
            Options.key("max_report_rows")
                    .intType()
                    .defaultValue(1000000)
                    .withDescription("Maximum rows in a report download, from 1 to 1000000.");

    private AmazonAdsSourceOptions() {}
}
