/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.harvest;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options for a bounded read of one account's Harvest time entries. */
public final class HarvestSourceOptions {
    private HarvestSourceOptions() {}

    public static final Option<String> ACCESS_TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Harvest personal access token, without the Bearer prefix.");
    public static final Option<String> ACCOUNT_ID =
            Options.key("account_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Harvest account ID accessible to the token.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://api.harvestapp.com")
                    .withDescription(
                            "Harvest API origin. Override only for a trusted proxy or testing; HTTP sends credentials in clear text.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Time entries per request, between 1 and 2000.");
    public static final Option<String> FROM =
            Options.key("from")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive lower spent_date bound, yyyy-MM-dd.");
    public static final Option<String> TO =
            Options.key("to")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive upper spent_date bound, yyyy-MM-dd.");
    public static final Option<String> UPDATED_SINCE =
            Options.key("updated_since")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Only entries updated since this ISO 8601 timestamp with offset. This is a request filter, not a persisted cursor.");
    public static final Option<Integer> RETRY =
            Options.key("retry")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts per page for HTTP 429, 500, 502, 503 and 504; zero disables retries.");
    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Initial exponential retry delay in milliseconds.");
    public static final Option<Integer> MAX_RETRY_DELAY_MS =
            Options.key("max_retry_delay_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Maximum retry wait in milliseconds. A longer Retry-After fails the page rather than retrying early.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Connection and connection-pool timeout in milliseconds, greater than zero.");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Socket read timeout in milliseconds, greater than zero.");
}
