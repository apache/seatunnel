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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.slack.config.SlackSinkOptions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Validates bounded Slack history settings and freezes the upper window. Existing sink options
 * retain their independent validation and defaults.
 */
public final class SlackSourceConfig implements Serializable {
    public static final Option<String> TOKEN = SlackSinkOptions.OAUTH_TOKEN;
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

    final String token;
    final List<String> channels;
    final String apiRoot;
    final int pageSize;
    final String oldest;
    final String latest;
    final int timeout;
    final int retries;
    final int backoff;
    final int maxDelay;
    final int maxBytes;
    final int maxPages;

    /**
     * Validates source-only settings and freezes an omitted latest boundary before reader creation.
     * Exact decimal strings remain unchanged in API requests and checkpoint state.
     */
    public SlackSourceConfig(ReadonlyConfig config) {
        token = config.get(TOKEN);
        require(
                token != null
                        && !token.trim().isEmpty()
                        && !token.contains("\r")
                        && !token.contains("\n"),
                "oauth_token");
        channels = new ArrayList<>(config.get(CHANNELS));
        require(!channels.isEmpty() && channels.size() <= 10000, "channel_ids");
        for (String channel : channels) {
            require(channel != null && channel.matches("[A-Za-z0-9]+"), "channel_ids");
        }
        require(new HashSet<>(channels).size() == channels.size(), "channel_ids");
        apiRoot = config.get(API_ROOT);
        try {
            URI uri = URI.create(apiRoot);
            require(
                    ("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
                            && uri.getHost() != null
                            && uri.getUserInfo() == null
                            && uri.getQuery() == null
                            && uri.getFragment() == null
                            && apiRoot.endsWith("/"),
                    "api_root");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Slack source option: api_root");
        }
        pageSize = bounded(config, PAGE_SIZE, 1, 999);
        timeout = bounded(config, TIMEOUT, 1, 300000);
        retries = bounded(config, RETRIES, 0, 10);
        maxDelay = bounded(config, MAX_DELAY, 1, 300000);
        backoff = bounded(config, BACKOFF, 1, maxDelay);
        maxBytes = bounded(config, MAX_BYTES, 1, 67108864);
        maxPages = bounded(config, MAX_PAGES, 1, 100000);
        oldest = config.getOptional(OLDEST).orElse(null);
        latest =
                config.getOptional(LATEST)
                        .orElseGet(
                                () ->
                                        BigDecimal.valueOf(System.currentTimeMillis(), 3)
                                                .toPlainString());
        if (oldest != null) {
            validateTimestamp(oldest, "oldest");
        }
        validateTimestamp(latest, "latest");
        require(
                oldest == null || new BigDecimal(oldest).compareTo(new BigDecimal(latest)) < 0,
                "oldest/latest");
    }

    private static int bounded(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        require(value >= min && value <= max, option.key());
        return value;
    }

    static void validateTimestamp(String value, String key) {
        require(
                value != null && value.length() <= 40 && value.matches("[0-9]+(?:\\.[0-9]+)?"),
                key);
    }

    private static void require(boolean condition, String key) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid Slack source option: " + key);
        }
    }
}
