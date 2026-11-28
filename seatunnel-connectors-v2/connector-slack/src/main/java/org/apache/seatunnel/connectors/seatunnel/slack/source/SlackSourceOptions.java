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

package org.apache.seatunnel.connectors.seatunnel.slack.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/**
 * Declares bounded history source options independently of Slack Sink validation. The source uses
 * the default-masked token key without changing the historical Sink OAuth option.
 */
public final class SlackSourceOptions {
    private SlackSourceOptions() {}

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Slack access token sent only in the Authorization header.");
    public static final Option<List<String>> CHANNELS =
            Options.key("channel_ids")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Explicit conversation IDs; membership and history scopes are required.");
    public static final Option<String> API_ROOT =
            Options.key("api_root")
                    .stringType()
                    .defaultValue("https://slack.com/api/")
                    .withDescription(
                            "Trusted Slack API root; HTTP is intended only for API-contract fixtures.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(15)
                    .withDescription(
                            "Requested messages per page, from 1 to 999; Slack may return fewer.");
    public static final Option<String> OLDEST =
            Options.key("oldest")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive lower timestamp boundary, as an exact decimal string.");
    public static final Option<String> LATEST =
            Options.key("latest")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive upper timestamp boundary, frozen at source creation if omitted.");
    public static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Whole HTTP attempt deadline, from 1 to 300000 milliseconds.");
    public static final Option<Integer> RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Additional attempts for transport failures, HTTP 429 and 5xx, from 0 to 10.");
    public static final Option<Integer> BACKOFF =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Fixed retry delay, from 1 to max_retry_delay_ms.");
    public static final Option<Integer> MAX_DELAY =
            Options.key("max_retry_delay_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription(
                            "Maximum accepted Retry-After delay, from 1 to 300000 ms; larger values fail.");
    public static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription(
                            "Maximum uncompressed response bytes per page, from 1 to 67108864.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_channel")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Fail rather than truncate beyond this page limit, from 1 to 100000.");
}
