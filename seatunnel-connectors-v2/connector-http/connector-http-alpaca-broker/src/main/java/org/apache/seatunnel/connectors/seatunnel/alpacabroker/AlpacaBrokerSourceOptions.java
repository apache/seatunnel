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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Configuration options for a read-only US Broker API source.
 *
 * <p>The key ID and secret use the engine's standard masked username and password names.
 */
public final class AlpacaBrokerSourceOptions {
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Broker API resource to read.");
    public static final Option<String> ENVIRONMENT =
            Options.key("environment")
                    .stringType()
                    .defaultValue("SANDBOX")
                    .withDescription("SANDBOX or LIVE US Broker API environment.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Broker API key ID used as the HTTP Basic username.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Broker API secret used as the HTTP Basic password.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional HTTPS origin for the sandbox environment.");
    public static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM trust anchor required for a custom sandbox origin.");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Lower timestamp for accounts or account activities, in RFC 3339.");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Upper timestamp for accounts or account activities, in RFC 3339.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "First inclusive calendar or document date in yyyy-MM-dd format.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Last inclusive calendar or document date in yyyy-MM-dd format.");
    public static final Option<String> CALENDAR_DATE_TYPE =
            Options.key("calendar_date_type")
                    .stringType()
                    .defaultValue("trading")
                    .withDescription(
                            "Calendar query interprets bounds as trading or settlement dates.");
    public static final Option<String> ASSET_CLASS =
            Options.key("asset_class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional Broker API asset class filter.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum records requested per activities page.");
    public static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum logical HTTP requests for one split.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total time budget for each logical request, including retries and response decoding.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum attempts for a retryable read.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum bytes in a decoded successful response.");

    private AlpacaBrokerSourceOptions() {}
}
