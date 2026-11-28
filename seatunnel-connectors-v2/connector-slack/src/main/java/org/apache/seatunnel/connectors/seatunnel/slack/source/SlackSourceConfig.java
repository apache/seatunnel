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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

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
        token = config.get(SlackSourceOptions.TOKEN);
        require(
                token != null
                        && !token.trim().isEmpty()
                        && !token.contains("\r")
                        && !token.contains("\n"),
                "token");
        channels = new ArrayList<>(config.get(SlackSourceOptions.CHANNELS));
        require(!channels.isEmpty() && channels.size() <= 10000, "channel_ids");
        for (String channel : channels) {
            require(channel != null && channel.matches("[A-Za-z0-9]+"), "channel_ids");
        }
        require(new HashSet<>(channels).size() == channels.size(), "channel_ids");
        apiRoot = config.get(SlackSourceOptions.API_ROOT);
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
        pageSize = bounded(config, SlackSourceOptions.PAGE_SIZE, 1, 999);
        timeout = bounded(config, SlackSourceOptions.TIMEOUT, 1, 300000);
        retries = bounded(config, SlackSourceOptions.RETRIES, 0, 10);
        maxDelay = bounded(config, SlackSourceOptions.MAX_DELAY, 1, 300000);
        backoff = bounded(config, SlackSourceOptions.BACKOFF, 1, maxDelay);
        maxBytes = bounded(config, SlackSourceOptions.MAX_BYTES, 1, 67108864);
        maxPages = bounded(config, SlackSourceOptions.MAX_PAGES, 1, 100000);
        oldest = config.getOptional(SlackSourceOptions.OLDEST).orElse(null);
        latest =
                config.getOptional(SlackSourceOptions.LATEST)
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
