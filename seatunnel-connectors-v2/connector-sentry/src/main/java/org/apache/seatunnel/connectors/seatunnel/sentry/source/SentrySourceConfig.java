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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.sentry.exception.SentryConnectorException;

import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

final class SentrySourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    final String token;
    final String endpoint;
    final Instant start;
    final Instant end;
    final int pageSize;
    final int maxPages;
    final int retries;
    final int retryDelay;
    final int timeout;
    final int maxBytes;

    SentrySourceConfig(ReadonlyConfig options) {
        // Zeta injects this engine-owned policy even for single-table sources.
        if (get(options, MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY)
                != MultiTableFailurePolicy.FAIL_FAST) {
            throw failure("Sentry source supports only multi_table.failure_policy=FAIL_FAST");
        }
        Map<String, Object> validation = new HashMap<>(options.getSourceMap());
        validation.remove(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key());
        if (validation.get("multi_table") instanceof Map) {
            Map<String, Object> nested =
                    new HashMap<>((Map<String, Object>) validation.get("multi_table"));
            nested.remove("failure_policy");
            if (nested.isEmpty()) {
                validation.remove("multi_table");
            } else {
                validation.put("multi_table", nested);
            }
        }
        ConfigValidator.validateUnknownKeys(
                ReadonlyConfig.fromMap(validation),
                new SentrySourceFactory().optionRule(),
                "Sentry");
        if (options.getSourceMap().containsKey("schema")) {
            throw failure("Sentry source uses a fixed schema");
        }
        if (get(options, EnvCommonOptions.PARALLELISM) != 1) {
            throw failure("Sentry source requires parallelism=1");
        }
        token = get(options, SentrySourceOptions.TOKEN);
        if (token == null || token.length() > 8192 || !token.matches("[A-Za-z0-9._~+/-]+=*")) {
            throw failure("Invalid token (value withheld)");
        }
        boolean mock = get(options, SentrySourceOptions.MOCK_MODE);
        if (mock && !token.equals("mock-token")) {
            throw failure("mock_mode requires token=mock-token");
        }
        String origin = origin(get(options, SentrySourceOptions.API_BASE_URL), mock);
        endpoint =
                origin
                        + "/api/0/projects/"
                        + slug(get(options, SentrySourceOptions.ORGANIZATION), "organization")
                        + "/"
                        + slug(get(options, SentrySourceOptions.PROJECT), "project")
                        + "/events/";
        start = date(get(options, SentrySourceOptions.START_TIME));
        end = date(get(options, SentrySourceOptions.END_TIME));
        if (!start.isBefore(end)) {
            throw failure("start_time must be earlier than end_time");
        }
        pageSize = range(get(options, SentrySourceOptions.PAGE_SIZE), 1, 100, "page_size");
        maxPages = range(get(options, SentrySourceOptions.MAX_PAGES), 1, 100000, "max_pages");
        retries = range(get(options, SentrySourceOptions.MAX_RETRIES), 0, 5, "max_retries");
        retryDelay =
                range(get(options, SentrySourceOptions.RETRY_DELAY_MS), 1, 60000, "retry_delay_ms");
        timeout =
                range(
                        get(options, SentrySourceOptions.REQUEST_TIMEOUT_MS),
                        1,
                        120000,
                        "request_timeout_ms");
        maxBytes =
                range(
                        get(options, SentrySourceOptions.MAX_RESPONSE_BYTES),
                        1024,
                        16777216,
                        "max_response_bytes");
    }

    private static <T> T get(ReadonlyConfig config, Option<T> option) {
        try {
            return config.get(option);
        } catch (RuntimeException e) {
            throw failure("Invalid option " + option.key() + " (value withheld)");
        }
    }

    private static String origin(String value, boolean mock) {
        try {
            URI uri = URI.create(value);
            if (uri.getHost() == null
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || !uri.getRawPath().isEmpty()
                    || !("https".equals(uri.getScheme())
                            || (mock && "http".equals(uri.getScheme())))) {
                throw new IllegalArgumentException();
            }
            return value;
        } catch (RuntimeException e) {
            throw failure(
                    "api_base_url must be an HTTPS origin without user info, path, query or fragment");
        }
    }

    private static String slug(String value, String name) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{1,200}")) {
            throw failure("Invalid " + name + " ID or slug");
        }
        return value;
    }

    private static Instant date(String value) {
        try {
            if (value == null
                    || !value.matches(
                            "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})")) {
                throw new IllegalArgumentException();
            }
            return OffsetDateTime.parse(value).toInstant();
        } catch (RuntimeException e) {
            throw failure(
                    "start_time and end_time must be absolute RFC3339 timestamps with offsets");
        }
    }

    private static int range(int value, int min, int max, String name) {
        if (value < min || value > max) {
            throw failure(name + " must be between " + min + " and " + max);
        }
        return value;
    }

    static SentryConnectorException failure(String message) {
        return new SentryConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, message);
    }
}
