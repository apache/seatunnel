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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.GROUP_BY;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REPORT_END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REPORT_START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.SEARCH_STORE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.USER_END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresOptions.USER_START_TIME;

/**
 * Validated options and the immutable identity of one Appfigures read.
 *
 * <p>The split retains a settings digest and never serializes the personal access token.
 */
final class AppfiguresSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Map<String, String> PLURALS =
            Map.of(
                    "products",
                    "product",
                    "countries",
                    "country",
                    "dates",
                    "date",
                    "stores",
                    "store",
                    "networks",
                    "network",
                    "devices",
                    "device");

    final AppfiguresResource resource;
    final String token;
    final String origin;
    final String trustCertificate;
    final String store;
    final List<String> groups;
    final String wireGroups;
    final LocalDate reportStart;
    final LocalDate reportEnd;
    final LocalDateTime userStart;
    final LocalDateTime userEnd;
    final int pageSize;
    final int maxPages;
    final int maxRows;
    final int maxBytes;
    final int maxAttempts;
    final int timeout;
    final String fingerprint;

    AppfiguresSettings(ReadonlyConfig config) {
        resource = AppfiguresResource.select(config.get(RESOURCE));
        token = config.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 8192
                || !token.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid("token", "expected a non-empty personal access token");
        }
        String endpoint = config.getOptional(ENDPOINT).orElse(null);
        trustCertificate = config.getOptional(TRUST_CERTIFICATE).orElse(null);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires a worker trust certificate");
        }
        origin = endpoint == null ? "https://api.appfigures.com" : httpsOrigin(endpoint);
        String search = config.getOptional(SEARCH_STORE).orElse(null);
        if (search != null
                && (!search.matches("[a-z0-9_]{1,64}")
                        || (resource != AppfiguresResource.PRODUCTS && !resource.report()))) {
            throw invalid("search_store", "expected a store identifier for products or reports");
        }
        store = search == null && resource == AppfiguresResource.PRODUCTS ? "apple" : search;
        groups = groups(config.get(GROUP_BY), resource.pivots());
        wireGroups = groups.stream().map(resource::wirePivot).collect(Collectors.joining(","));
        reportStart =
                reportDate(config.getOptional(REPORT_START_DATE).orElse(null), "report_start_date");
        reportEnd = reportDate(config.getOptional(REPORT_END_DATE).orElse(null), "report_end_date");
        if ((reportStart != null || reportEnd != null)
                && (!resource.report()
                        || reportStart == null
                        || reportEnd == null
                        || reportEnd.isBefore(reportStart))) {
            throw invalid(
                    "report_end_date",
                    "a report date range requires both inclusive dates in order");
        }
        if (resource == AppfiguresResource.SALES
                && reportStart != null
                && ChronoUnit.DAYS.between(reportStart, reportEnd) >= 30) {
            throw invalid("report_end_date", "sales date ranges may contain at most 30 days");
        }

        userStart = userTime(config.getOptional(USER_START_TIME).orElse(null), "user_start_time");
        userEnd = userTime(config.getOptional(USER_END_TIME).orElse(null), "user_end_time");
        if ((userStart != null || userEnd != null) && resource != AppfiguresResource.USERS) {
            throw invalid("user_start_time", "login bounds apply only to users");
        }
        if (userStart != null && userEnd != null && !userStart.isBefore(userEnd)) {
            throw invalid("user_end_time", "must be after the login start time");
        }

        pageSize = limited(config.get(PAGE_SIZE), 1, 100, "page_size");
        maxPages = limited(config.get(MAX_PAGES), 1, 10000, "max_pages");
        maxRows = limited(config.get(MAX_ROWS), 1, 500000, "max_rows");
        maxBytes =
                limited(
                        config.get(MAX_RESPONSE_BYTES),
                        1024,
                        64 * 1024 * 1024,
                        "max_response_bytes");
        maxAttempts = limited(config.get(MAX_ATTEMPTS), 1, 10, "max_attempts");
        timeout = limited(config.get(REQUEST_TIMEOUT_MS), 100, 300000, "request_timeout_ms");
        fingerprint =
                digest(
                        String.join(
                                "\0",
                                resource.name,
                                origin,
                                text(trustCertificate),
                                digest(token),
                                text(store),
                                wireGroups,
                                text(reportStart),
                                text(reportEnd),
                                text(userStart),
                                text(userEnd),
                                Integer.toString(pageSize)));
    }

    private static List<String> groups(String text, Set<String> supported) {
        if (text.isBlank()) {
            return List.of();
        }
        if (text.length() > 128) {
            throw invalid("group_by", "group list exceeds its size limit");
        }
        List<String> groups = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String raw : text.split(",", -1)) {
            String value = raw.trim().toLowerCase(Locale.ROOT);
            String pivot = PLURALS.getOrDefault(value, value);
            if (!supported.contains(pivot) || !seen.add(pivot)) {
                throw invalid("group_by", "unsupported or repeated report pivot");
            }
            groups.add(pivot);
        }
        return List.copyOf(groups);
    }

    private static LocalDate reportDate(String raw, String key) {
        try {
            return raw == null ? null : LocalDate.parse(raw);
        } catch (DateTimeException e) {
            throw invalid(key, "expected YYYY-MM-DD");
        }
    }

    private static LocalDateTime userTime(String raw, String key) {
        try {
            return raw == null ? null : LocalDateTime.parse(raw);
        } catch (DateTimeException e) {
            throw invalid(key, "expected local ISO date and time without an offset");
        }
    }

    private static int limited(int value, int min, int max, String key) {
        if (value < min || value > max) {
            throw invalid(key, "must be between " + min + " and " + max);
        }
        return value;
    }

    private static String httpsOrigin(String raw) {
        try {
            URI value = URI.create(raw);
            if ("https".equals(value.getScheme())
                    && value.getHost() != null
                    && value.getRawUserInfo() == null
                    && value.getRawQuery() == null
                    && value.getRawFragment() == null
                    && (value.getRawPath().isEmpty() || "/".equals(value.getRawPath()))
                    && (value.getPort() == -1
                            || (value.getPort() > 0 && value.getPort() <= 65535))) {
                return "https://"
                        + value.getHost().toLowerCase(Locale.ROOT)
                        + (value.getPort() == -1 ? "" : ":" + value.getPort());
            }
        } catch (IllegalArgumentException ignored) {
            // Configuration errors never reproduce a supplied endpoint.
        }
        throw invalid("endpoint", "expected an HTTPS origin without path or credentials");
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    static String digest(byte[] value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String reason) {
        return new IllegalArgumentException("Appfigures " + key + ": " + reason);
    }
}
