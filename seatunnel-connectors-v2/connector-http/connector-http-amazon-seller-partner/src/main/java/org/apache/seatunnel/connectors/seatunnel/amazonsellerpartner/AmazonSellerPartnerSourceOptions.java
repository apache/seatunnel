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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Factory options for the Amazon Selling Partner API.
 *
 * <p>Token, client secret, and restricted data use the engine secret names and explicit scope.
 */
public final class AmazonSellerPartnerSourceOptions {
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SP-API stream or report view.");
    public static final Option<String> ACCOUNT_TYPE =
            Options.key("account_type")
                    .stringType()
                    .defaultValue("SELLER")
                    .withDescription("SELLER or VENDOR role family.");
    public static final Option<String> ENVIRONMENT =
            Options.key("environment")
                    .stringType()
                    .defaultValue("PRODUCTION")
                    .withDescription("PRODUCTION or SANDBOX SP-API endpoints.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("US")
                    .withDescription("Marketplace country code.");
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Login with Amazon OAuth client ID.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Login with Amazon OAuth client secret.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Login with Amazon refresh token for the selling partner.");
    public static final Option<Boolean> INCLUDE_PII =
            Options.key("include_pii")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Require explicitly scoped restricted data access where supported.");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Beginning of the requested replication range as an RFC 3339 timestamp.");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fixed end of the replication range as an RFC 3339 timestamp.");
    public static final Option<Integer> PERIOD_IN_DAYS =
            Options.key("period_in_days")
                    .intType()
                    .defaultValue(90)
                    .withDescription(
                            "Upper limit for the duration of a report or order time slice.");
    public static final Option<String> FINANCIAL_EVENTS_STEP =
            Options.key("financial_events_step")
                    .stringType()
                    .defaultValue("180D")
                    .withDescription(
                            "The time step for financial event reads, as an integer followed by H or D.");
    public static final Option<Integer> FINANCIAL_PAGE_SIZE =
            Options.key("financial_page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The financial API page size, from 1 to 100.");
    public static final Option<String> INBOUND_REPLICATION_MODE =
            Options.key("inbound_replication_mode")
                    .stringType()
                    .defaultValue("ROLLING")
                    .withDescription("ROLLING lookback or FIXED window for FBA inbound data.");
    public static final Option<Integer> INBOUND_ROLLING_DAYS =
            Options.key("inbound_rolling_days")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Number of days in the FBA inbound rolling window.");
    public static final Option<String> INBOUND_START_DATETIME =
            Options.key("inbound_start_datetime")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fixed beginning of the FBA inbound interval.");
    public static final Option<String> INBOUND_END_DATETIME =
            Options.key("inbound_end_datetime")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional fixed end of the FBA inbound interval.");
    public static final Option<String> REPORT_OPTIONS_LIST =
            Options.key("report_options_list")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription(
                            "JSON list of named reports and their option name/value entries.");
    public static final Option<String> ASIN_GRANULARITY =
            Options.key("asin_granularity")
                    .stringType()
                    .defaultValue("PARENT")
                    .withDescription("PARENT, CHILD, or SKU for Sales and Traffic by ASIN.");
    public static final Option<Integer> REPORT_LOOKBACK_HOURS =
            Options.key("report_lookback_hours")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Extra hours before the selected report range.");
    public static final Option<Integer> MAX_DONE_REPORT_AGE_HOURS =
            Options.key("max_done_report_age_hours")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Reuse completed reports only up to this age. Zero disables external DONE reuse.");
    public static final Option<Boolean> TREAT_CANCELLED_AS_EMPTY =
            Options.key("treat_cancelled_as_empty")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow a cancelled report from this run to represent an empty result.");
    public static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum provider steps and report output batches per split.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts of a retryable HTTP read.");
    public static final Option<Integer> MAX_RATE_LIMIT_RETRIES =
            Options.key("max_rate_limit_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum HTTP 429 retries within one request.");
    public static final Option<Boolean> STOP_SYNC_ON_RATE_LIMIT =
            Options.key("stop_sync_on_rate_limit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Stop at the first throttled report request.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Total budget for one provider request and its retries.");
    public static final Option<Integer> REPORT_TIMEOUT_MS =
            Options.key("report_timeout_ms")
                    .intType()
                    .defaultValue(3600000)
                    .withDescription("Maximum time spent awaiting one report.");
    public static final Option<Integer> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Interval between report status requests.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decoded response bytes for API and report documents.");
    public static final Option<Integer> MAX_REPORT_ROWS =
            Options.key("max_report_rows")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum records accepted from one decoded report.");
    public static final Option<String> SANDBOX_ORIGIN =
            Options.key("sandbox_origin")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Explicit custom SANDBOX HTTPS origin for API and token requests.");
    public static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM trust anchor for a custom sandbox origin.");

    private AmazonSellerPartnerSourceOptions() {}
}
