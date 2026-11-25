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

package org.apache.seatunnel.connectors.seatunnel.woocommerce.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class WooCommerceSourceOptions {
    private WooCommerceSourceOptions() {}

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTPS store URL, including any WordPress subdirectory.");
    public static final Option<String> CONSUMER_KEY =
            Options.key("consumer_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Read-only WooCommerce REST API consumer key.");
    public static final Option<String> CONSUMER_SECRET =
            Options.key("consumer_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("WooCommerce REST API consumer secret.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive UTC creation-time lower bound, ISO8601 with seconds.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive UTC creation-time upper bound, ISO8601 with seconds.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Orders per page, between 1 and 100.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum pages in a scan, between 1 and 1000000; fail rather than truncate.");
    public static final Option<Integer> DECIMAL_PLACES =
            Options.key("decimal_places")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "WooCommerce dp parameter: monetary decimal places, between 0 and 18.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Retries per page for transport failures, HTTP 429 and transient 5xx.");
    public static final Option<Integer> RETRY_DELAY_MS =
            Options.key("retry_delay_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Minimum delay between retries, between 1 and 60000 ms.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Per-attempt HTTP I/O timeout and abort deadline, between 1 and 120000 ms.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decompressed response bytes per page, between 1024 and 16777216.");
}
