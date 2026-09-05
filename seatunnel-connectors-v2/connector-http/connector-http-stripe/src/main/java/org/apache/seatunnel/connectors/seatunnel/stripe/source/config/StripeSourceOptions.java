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

package org.apache.seatunnel.connectors.seatunnel.stripe.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class StripeSourceOptions {

    public static final String DEFAULT_API_BASE_URL = "https://api.stripe.com";

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stripe secret API key used for Bearer authentication");

    public static final Option<String> API_BASE_URL =
            Options.key("api_base_url")
                    .stringType()
                    .defaultValue(DEFAULT_API_BASE_URL)
                    .withDescription("Stripe API base URL");

    public static final Option<String> API_VERSION =
            Options.key("api_version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional Stripe API version sent in the Stripe-Version header");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of PaymentIntents requested per page, from 1 to 100");

    public static final Option<Long> CREATED_GTE =
            Options.key("created_gte")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Inclusive lower created-time boundary as Unix seconds");

    public static final Option<Long> CREATED_LT =
            Options.key("created_lt")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Exclusive upper created-time boundary as Unix seconds");

    public static final Option<Integer> RATE_LIMIT_MAX_RETRIES =
            Options.key("rate_limit_max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum retries after a Stripe HTTP 429 response");

    public static final Option<Integer> RATE_LIMIT_BACKOFF_MS =
            Options.key("rate_limit_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Initial exponential backoff after a Stripe HTTP 429 response");

    private StripeSourceOptions() {}
}
