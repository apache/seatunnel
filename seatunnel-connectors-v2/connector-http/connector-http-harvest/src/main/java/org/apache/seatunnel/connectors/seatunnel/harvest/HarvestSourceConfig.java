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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.ACCESS_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.ACCOUNT_ID;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.FROM;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.MAX_RETRY_DELAY_MS;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.RETRY;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.SOCKET_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.TO;
import static org.apache.seatunnel.connectors.seatunnel.harvest.HarvestSourceOptions.UPDATED_SINCE;

/** Validated immutable request settings; never part of reader checkpoint state. */
final class HarvestSourceConfig implements Serializable {
    final String accessToken;
    final String accountId;
    final URI endpoint;
    final Map<String, String> filters;
    final int pageSize;
    final int retries;
    final int backoff;
    final int maxDelay;
    final int connectTimeout;
    final int socketTimeout;

    HarvestSourceConfig(ReadonlyConfig options) {
        accessToken = nonBlank(options, ACCESS_TOKEN);
        accountId = nonBlank(options, ACCOUNT_ID);
        if (!accountId.matches("[0-9]+")) {
            throw invalid("account_id", "must contain only digits");
        }
        if (accessToken.indexOf('\r') >= 0 || accessToken.indexOf('\n') >= 0) {
            throw invalid("token", "must not contain line breaks");
        }
        URI origin;
        try {
            origin = URI.create(nonBlank(options, BASE_URL));
        } catch (IllegalArgumentException e) {
            throw invalid("base_url", "must be an HTTP(S) origin");
        }
        if (!("https".equalsIgnoreCase(origin.getScheme())
                        || "http".equalsIgnoreCase(origin.getScheme()))
                || origin.getHost() == null
                || origin.getUserInfo() != null
                || origin.getQuery() != null
                || origin.getFragment() != null
                || !(origin.getPath().isEmpty() || "/".equals(origin.getPath()))) {
            throw invalid(
                    "base_url",
                    "must be an HTTP(S) origin without user info, path, query or fragment");
        }
        endpoint = origin.resolve("/v2/time_entries");
        pageSize = integer(options, PAGE_SIZE, 1, 2000);
        retries = integer(options, RETRY, 0, Integer.MAX_VALUE - 1);
        backoff = integer(options, RETRY_BACKOFF_MS, 1, Integer.MAX_VALUE);
        maxDelay = integer(options, MAX_RETRY_DELAY_MS, backoff, Integer.MAX_VALUE);
        connectTimeout = integer(options, CONNECT_TIMEOUT_MS, 1, Integer.MAX_VALUE);
        socketTimeout = integer(options, SOCKET_TIMEOUT_MS, 1, Integer.MAX_VALUE);
        Map<String, String> values = new LinkedHashMap<>();
        for (Option<String> option : java.util.Arrays.asList(FROM, TO, UPDATED_SINCE)) {
            if (options.getOptional(option).isPresent()) {
                String value = nonBlank(options, option);
                try {
                    if (option == UPDATED_SINCE) {
                        OffsetDateTime.parse(value);
                    } else {
                        LocalDate.parse(value);
                    }
                } catch (DateTimeParseException e) {
                    throw invalid(option.key(), "has an invalid date or timestamp");
                }
                values.put(option.key(), value);
            }
        }
        if (values.containsKey(FROM.key())
                && values.containsKey(TO.key())
                && LocalDate.parse(values.get(FROM.key()))
                        .isAfter(LocalDate.parse(values.get(TO.key())))) {
            throw invalid("from", "must not be after to");
        }
        filters = Collections.unmodifiableMap(values);
    }

    private static String nonBlank(ReadonlyConfig config, Option<String> option) {
        String value = config.get(option);
        if (value == null || value.trim().isEmpty()) {
            throw invalid(option.key(), "must not be blank");
        }
        return value;
    }

    private static int integer(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "must be between " + min + " and " + max);
        }
        return value;
    }

    private static IllegalArgumentException invalid(String key, String reason) {
        return new IllegalArgumentException("Harvest option '" + key + "' " + reason + ".");
    }
}
