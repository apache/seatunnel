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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class PayPalSourceOptions {
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("First-party PayPal REST app client ID.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("PayPal client secret; masked in parsed job configuration.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Absolute RFC3339 start with seconds and offset.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute RFC3339 end, at most 31 days after start; shortened response coverage fails.");
    public static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .defaultValue("https://api-m.paypal.com")
                    .withDescription("PayPal production or sandbox origin.");
    public static final Option<Boolean> MOCK_MODE =
            Options.key("mock_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow an HTTP mock origin with dummy credentials only; never enable for PayPal.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Records per page, 1 to 500.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts for HTTP 429/5xx and transport failures, 0 to 5.");
    public static final Option<Integer> RETRY_DELAY_MS =
            Options.key("retry_delay_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Retry delay, 1 to 60000 ms. Longer Retry-After fails instead of retrying early.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connect, socket and request abort deadline, 1 to 120000 ms.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum uncompressed response bytes, 1024 to 16777216.");

    private PayPalSourceOptions() {}
}
