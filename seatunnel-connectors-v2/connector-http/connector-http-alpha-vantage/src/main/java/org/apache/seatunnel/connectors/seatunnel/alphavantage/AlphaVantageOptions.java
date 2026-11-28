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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/**
 * Declares job options and defaults for the equity data source. The factory uses these definitions
 * for validation and discovery.
 */
final class AlphaVantageOptions {
    static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://www.alphavantage.co")
                    .withDescription("HTTPS origin of the Alpha Vantage service.");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Alpha Vantage API key.");
    static final Option<String> SYMBOL =
            Options.key("symbol")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Equity symbol including its exchange suffix if required.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(AlphaVantageResource.names())
                    .withDescription("Ordered list of Alpha Vantage resources to read.");
    static final Option<String> INTERVAL =
            Options.key("interval")
                    .stringType()
                    .defaultValue("1min")
                    .withDescription("Time step for intraday data.");
    static final Option<Boolean> ADJUSTED =
            Options.key("adjusted")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Request split and dividend adjustment on intraday observations.");
    static final Option<String> OUTPUT_SIZE =
            Options.key("outputsize")
                    .stringType()
                    .defaultValue("compact")
                    .withDescription("Observation window for intraday and daily series.");
    static final Option<Integer> MAX_RECORDS =
            Options.key("max_records")
                    .intType()
                    .defaultValue(50000)
                    .withDescription("Maximum observations in one response.");
    static final Option<Integer> MAX_RESPONSE =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Separate on-wire and decoded response byte limits.");
    static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection timeout.");
    static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and response inactivity timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total budget per resource request including local pacing and retries.");
    static final Option<Integer> REQUEST_INTERVAL =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("Minimum spacing between attempts by one source reader.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum attempts per resource including the first.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(24)
                    .withDescription(
                            "Maximum HTTPS attempts across all resources per reader execution.");

    private AlphaVantageOptions() {}
}
