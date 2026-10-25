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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.options.EnvCommonOptions;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

final class PayPalConfig implements Serializable {
    final String clientId;
    final String clientSecret;
    final String origin;
    final Instant start;
    final Instant end;
    final int pageSize;
    final int retries;
    final int retryDelay;
    final int timeout;
    final int maxBytes;

    PayPalConfig(ReadonlyConfig config) {
        ConfigValidator.validateUnknownKeys(
                config, new PayPalSourceFactory().optionRule(), "PayPal");
        if (config.getSourceMap().containsKey("schema")) {
            throw new IllegalArgumentException(
                    "PayPal uses a fixed schema and does not support a schema option");
        }
        if (get(config, EnvCommonOptions.PARALLELISM) != 1) {
            throw new IllegalArgumentException("PayPal source requires parallelism=1");
        }
        clientId = credential(get(config, PayPalSourceOptions.CLIENT_ID), "client_id");
        clientSecret = credential(get(config, PayPalSourceOptions.CLIENT_SECRET), "client_secret");
        start = date(get(config, PayPalSourceOptions.START_DATE));
        end = date(get(config, PayPalSourceOptions.END_DATE));
        if (!start.isBefore(end)
                || Duration.between(start, end).compareTo(Duration.ofDays(31)) > 0) {
            throw new IllegalArgumentException(
                    "PayPal requires start_date < end_date and a window <=31 days");
        }
        if (get(config, PayPalSourceOptions.MOCK_MODE)
                && !(clientId.equals("mock-client") && clientSecret.equals("mock-secret"))) {
            throw new IllegalArgumentException(
                    "PayPal mock_mode requires client_id=mock-client and client_secret=mock-secret");
        }
        origin =
                origin(
                        get(config, PayPalSourceOptions.API_BASE_URL),
                        get(config, PayPalSourceOptions.MOCK_MODE));
        pageSize = range(get(config, PayPalSourceOptions.PAGE_SIZE), 1, 500, "page_size");
        retries = range(get(config, PayPalSourceOptions.MAX_RETRIES), 0, 5, "max_retries");
        retryDelay =
                range(get(config, PayPalSourceOptions.RETRY_DELAY_MS), 1, 60000, "retry_delay_ms");
        timeout =
                range(
                        get(config, PayPalSourceOptions.REQUEST_TIMEOUT_MS),
                        1,
                        120000,
                        "request_timeout_ms");
        maxBytes =
                range(
                        get(config, PayPalSourceOptions.MAX_RESPONSE_BYTES),
                        1024,
                        16777216,
                        "max_response_bytes");
    }

    private static <T> T get(ReadonlyConfig config, Option<T> option) {
        try {
            return config.get(option);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid PayPal option " + option.key() + " (value withheld)");
        }
    }

    static Instant date(String value) {
        try {
            if (value == null
                    || !value.matches(
                            "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})")) {
                throw new IllegalArgumentException();
            }
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        } catch (RuntimeException e) {
            // Parser causes can contain configuration values or response data.
            throw new IllegalArgumentException(
                    "PayPal requires valid absolute RFC3339 dates with seconds and offset");
        }
    }

    private static String credential(String value, String key) {
        if (value == null
                || value.trim().isEmpty()
                || value.length() > 4096
                || value.indexOf(':') >= 0
                || value.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw new IllegalArgumentException("Invalid PayPal " + key);
        }
        return value;
    }

    private static String origin(String value, boolean mock) {
        try {
            URI uri = URI.create(value);
            boolean official =
                    value.equals("https://api-m.paypal.com")
                            || value.equals("https://api-m.sandbox.paypal.com");
            if ((!official && !mock)
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || !uri.getRawPath().isEmpty()
                    || !(uri.getScheme().equals("https")
                            || (mock && uri.getScheme().equals("http")))) {
                throw new IllegalArgumentException();
            }
            return value;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "PayPal api_base_url must be an official origin; custom origins require mock_mode and dummy credentials");
        }
    }

    private static int range(int value, int min, int max, String key) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "PayPal " + key + " must be between " + min + " and " + max);
        }
        return value;
    }
}
