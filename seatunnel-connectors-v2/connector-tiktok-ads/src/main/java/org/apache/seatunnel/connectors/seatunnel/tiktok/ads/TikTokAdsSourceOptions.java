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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public final class TikTokAdsSourceOptions {
    private TikTokAdsSourceOptions() {}

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Authorized TikTok Access-Token. Use mock-token only with mock_url.");
    public static final Option<String> ADVERTISER_ID =
            Options.key("advertiser_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One numeric advertiser ID.");
    public static final Option<String> DATA_LEVEL =
            Options.key("data_level")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Only AUCTION_AD is supported; report_type is always BASIC.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive yyyy-MM-dd date in the advertiser timezone.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive yyyy-MM-dd date in the advertiser timezone.");
    public static final Option<List<String>> DIMENSIONS =
            Options.key("dimensions")
                    .listType()
                    .noDefaultValue()
                    .withDescription("ad_id, optionally followed by stat_time_day.");
    public static final Option<List<String>> METRICS =
            Options.key("metrics")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Explicit nonempty subset of spend, impressions, clicks.");
    public static final Option<String> MOCK_URL =
            Options.key("mock_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Test-only HTTP origin, requires literal token mock-token; never use real credentials.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Requested rows per page, 1 to 1000.");
    public static final Option<Integer> MAX_ROWS =
            Options.key("max_report_rows")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(
                            "Fail rather than truncate above this total row bound, 1 to 1000000.");
    public static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Response byte limit per request, 1024 to 16777216.");
    public static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "HTTP deadline including body, 100 to 120000 ms; DNS resolution is platform controlled.");
    public static final Option<Integer> REPORT_TIMEOUT =
            Options.key("report_timeout_ms")
                    .intType()
                    .defaultValue(600000)
                    .withDescription(
                            "Report deadline checked between records and requests, 100 to 3600000 ms.");
    public static final Option<Integer> RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts for HTTP 429/500/502/503/504 only, 0 to 5.");
    public static final Option<Integer> RETRY_WAIT =
            Options.key("max_retry_wait_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Maximum retry wait, 1000 to 120000 ms; longer Retry-After fails.");
}
