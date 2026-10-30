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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;

import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

final class WooCommerceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    final String url;
    final String key;
    final String secret;
    final String start;
    final String end;
    final int pageSize;
    final int maxPages;
    final int decimalPlaces;
    final int retries;
    final int retryDelay;
    final int timeout;
    final int maxBytes;

    WooCommerceConfig(ReadonlyConfig config) {
        // The engine injects this policy into all sources, including single-table sources.
        // It is not a WooCommerce option or a multi-table capability.
        ConfigValidator.validateUnknownKeys(
                ReadonlyConfig.fromConfig(
                        config.toConfig()
                                .withoutPath(
                                        MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key())),
                new WooCommerceSourceFactory().optionRule(),
                "WooCommerce");
        if (get(config, MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY)
                != MultiTableFailurePolicy.FAIL_FAST) {
            throw new IllegalArgumentException(
                    "WooCommerce single-table source requires the FAIL_FAST failure policy");
        }
        if (get(config, EnvCommonOptions.PARALLELISM) != 1) {
            throw new IllegalArgumentException("WooCommerce source requires parallelism=1");
        }
        url = endpoint(get(config, WooCommerceSourceOptions.URL));
        key = credential(get(config, WooCommerceSourceOptions.CONSUMER_KEY), "consumer_key", "ck_");
        secret =
                credential(
                        get(config, WooCommerceSourceOptions.CONSUMER_SECRET),
                        "consumer_secret",
                        "cs_");
        Instant first = date(get(config, WooCommerceSourceOptions.START_DATE));
        Instant last = date(get(config, WooCommerceSourceOptions.END_DATE));
        if (!first.isBefore(last)) {
            throw new IllegalArgumentException("WooCommerce requires start_date < end_date");
        }
        // WordPress applies the site timezone to offset-bearing strings even for a GMT
        // column. Send UTC wall-clock values with dates_are_gmt=true to avoid that shift.
        start = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(first.atOffset(ZoneOffset.UTC));
        end = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(last.atOffset(ZoneOffset.UTC));
        pageSize = range(get(config, WooCommerceSourceOptions.PAGE_SIZE), 1, 100, "page_size");
        maxPages = range(get(config, WooCommerceSourceOptions.MAX_PAGES), 1, 1000000, "max_pages");
        decimalPlaces =
                range(
                        get(config, WooCommerceSourceOptions.DECIMAL_PLACES),
                        0,
                        18,
                        "decimal_places");
        retries = range(get(config, WooCommerceSourceOptions.MAX_RETRIES), 0, 5, "max_retries");
        retryDelay =
                range(
                        get(config, WooCommerceSourceOptions.RETRY_DELAY_MS),
                        1,
                        60000,
                        "retry_delay_ms");
        timeout =
                range(
                        get(config, WooCommerceSourceOptions.REQUEST_TIMEOUT_MS),
                        1,
                        120000,
                        "request_timeout_ms");
        maxBytes =
                range(
                        get(config, WooCommerceSourceOptions.MAX_RESPONSE_BYTES),
                        1024,
                        16777216,
                        "max_response_bytes");
    }

    private static <T> T get(ReadonlyConfig config, Option<T> option) {
        try {
            return config.get(option);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid WooCommerce option " + option.key() + " (value withheld)");
        }
    }

    private static String endpoint(String value) {
        try {
            URI uri = URI.create(value);
            if (!"https".equals(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || !uri.normalize().equals(uri)) {
                throw new IllegalArgumentException();
            }
            return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "WooCommerce url must be an HTTPS store URL without credentials, query or fragment");
        }
    }

    private static String credential(String value, String name, String prefix) {
        if (value == null || !value.matches(prefix + "[a-fA-F0-9]{40}")) {
            throw new IllegalArgumentException("Invalid WooCommerce " + name + " (value withheld)");
        }
        return value;
    }

    private static Instant date(String value) {
        try {
            if (!value.matches(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})")) {
                throw new IllegalArgumentException();
            }
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "WooCommerce dates must be ISO8601 with seconds and UTC offset");
        }
    }

    private static int range(int value, int min, int max, String name) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "WooCommerce " + name + " must be between " + min + " and " + max);
        }
        return value;
    }
}
