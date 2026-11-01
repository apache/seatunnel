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

package org.apache.seatunnel.connectors.seatunnel.mailchimp;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class MailchimpSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mailchimp API key, with its datacenter suffix.");
    public static final Option<String> LIST_ID =
            Options.key("list_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Audience/list identifier.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Explicitly trusted API root including /3.0; defaults to the API key datacenter HTTPS endpoint.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow cleartext HTTP only for isolated fixtures with dummy credentials. Never use with real API keys.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of members per request, from 1 to 1000.");
    public static final Option<Integer> RETRY =
            Options.key("retry")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Maximum retries per page for HTTP 429 and 5xx, from 0 to 10.");
    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Initial exponential retry delay in milliseconds, from 1 to 60000; capped at 60000.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Connection, pool and socket timeout in milliseconds, from 1 to 300000.");

    private MailchimpSourceOptions() {}
}
