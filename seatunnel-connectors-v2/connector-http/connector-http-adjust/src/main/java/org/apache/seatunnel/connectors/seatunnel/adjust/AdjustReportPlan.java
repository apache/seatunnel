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

package org.apache.seatunnel.connectors.seatunnel.adjust;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.ADDITIONAL_METRICS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.DIMENSIONS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.METRICS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.METRIC_SCALES;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.UNTIL_TODAY;
import static org.apache.seatunnel.connectors.seatunnel.adjust.AdjustSourceOptions.UTC_OFFSET;

/** Freezes report schema and query semantics independently of credentials and the job clock. */
final class AdjustReportPlan implements Serializable {
    private static final long serialVersionUID = 1L;
    static final int PRECISION = 38;
    private static final int MAX_COLUMNS = 96;
    private static final int MAX_DAYS = 3660;
    private static final int MAX_TARGET_LENGTH = 8192;

    final URI origin;
    final String token;
    final LocalDate start;
    final ZoneOffset offset;
    final boolean untilToday;
    final List<String> dimensions;
    final List<Metric> metrics;
    final int timeoutMillis;
    final int maxAttempts;
    private final String query;

    AdjustReportPlan(ReadonlyConfig options) {
        origin = checkedOrigin(options.get(BASE_URL), options.get(ALLOW_INSECURE_HTTP));
        token = options.get(TOKEN);
        if (token.isEmpty()
                || token.length() > 4096
                || !token.chars().allMatch(code -> code >= 33 && code <= 126)) {
            throw invalid("token must be nonempty and contain no whitespace");
        }
        start = isoDay(options.get(START_DATE));
        offset = fixedOffset(options.get(UTC_OFFSET));
        untilToday = options.get(UNTIL_TODAY);
        timeoutMillis = range(options.get(REQUEST_TIMEOUT_MS), 300000, "request_timeout_ms");
        maxAttempts = range(options.get(MAX_ATTEMPTS), 8, "max_attempts");

        dimensions = new ArrayList<>();
        metrics = new ArrayList<>();
        Set<String> fieldNames = new HashSet<>();
        Set<String> dimensionInput = new HashSet<>();
        for (String name : options.get(DIMENSIONS)) {
            requireSlug(dimensionInput, name);
            if (!"day".equals(name)) {
                dimensions.add(name);
                fieldNames.add(name.toLowerCase(Locale.ROOT));
            }
        }
        if (!fieldNames.add("day")) {
            throw invalid("day is reserved for the required report date");
        }
        List<String> metricNames = new ArrayList<>(options.get(METRICS));
        if (metricNames.isEmpty()) {
            throw invalid("metrics must select at least one metric");
        }
        metricNames.addAll(options.get(ADDITIONAL_METRICS));
        Map<String, String> scales = options.get(METRIC_SCALES);
        if (!new HashSet<>(metricNames).containsAll(scales.keySet())) {
            throw invalid("metric_scales must reference selected metrics");
        }
        for (String name : metricNames) {
            requireSlug(fieldNames, name);
            metrics.add(new Metric(name, decimalScale(scales.getOrDefault(name, "18"))));
        }
        if (1 + dimensions.size() + metrics.size() > MAX_COLUMNS) {
            throw invalid("a report can contain at most 96 output columns");
        }
        List<String> reportDimensions = new ArrayList<>(dimensions);
        reportDimensions.add("day");
        query =
                "/reports-service/report?dimensions="
                        + encode(String.join(",", reportDimensions))
                        + "&metrics="
                        + encode(String.join(",", metricNames))
                        + "&format_dates=false&utc_offset="
                        + encode(offset.equals(ZoneOffset.UTC) ? "+00:00" : offset.getId())
                        + "&date_period=";
        if (query.length() + 23 > MAX_TARGET_LENGTH) {
            throw invalid("report query exceeds the request length limit");
        }
    }

    LocalDate windowEnd() {
        LocalDate today = LocalDate.now(offset);
        if (start.isAfter(today)) {
            throw invalid("start_date cannot be later than today in utc_offset");
        }
        LocalDate end = untilToday ? today : today.minusDays(1);
        if (ChronoUnit.DAYS.between(start, end) >= MAX_DAYS) {
            throw invalid("a job can scan at most 3660 report days");
        }
        return end;
    }

    TableSchema schema() {
        TableSchema.Builder columns =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "day",
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        0,
                                        false,
                                        null,
                                        null));
        for (String dimension : dimensions) {
            columns.column(
                    PhysicalColumn.of(dimension, BasicType.STRING_TYPE, 0, true, null, null));
        }
        for (Metric metric : metrics) {
            columns.column(
                    PhysicalColumn.of(
                            metric.name,
                            new DecimalType(PRECISION, metric.scale),
                            0,
                            true,
                            null,
                            null));
        }
        return columns.build();
    }

    String target(LocalDate day) {
        return query + encode(day + ":" + day);
    }

    String signature() {
        return origin
                + "|"
                + query
                + "|"
                + start
                + "|"
                + untilToday
                + "|"
                + metrics.stream()
                        .map(metric -> Integer.toString(metric.scale))
                        .collect(Collectors.joining(","));
    }

    private static void requireSlug(Set<String> names, String name) {
        if (name == null || !name.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,95}")) {
            throw invalid("dimension and metric names must be Adjust slugs");
        }
        if (!names.add(name.toLowerCase(Locale.ROOT))) {
            throw invalid("dimension and metric names must be unique");
        }
    }

    private static int decimalScale(String string) {
        try {
            int scale = Integer.parseInt(string);
            if (scale >= 0 && scale <= PRECISION) {
                return scale;
            }
        } catch (NumberFormatException ignored) {
            // The option-specific error below avoids logging arbitrary input.
        }
        throw invalid("metric_scales must be integers from 0 to 38");
    }

    private static LocalDate isoDay(String value) {
        try {
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(value);
            }
        } catch (DateTimeException ignored) {
            // The diagnostic never includes the supplied value.
        }
        throw invalid("start_date must be a valid YYYY-MM-DD");
    }

    private static ZoneOffset fixedOffset(String value) {
        try {
            if (value.matches("[+-][0-9]{2}:[0-9]{2}")) {
                ZoneOffset offset = ZoneOffset.of(value);
                if (Math.abs(offset.getTotalSeconds()) <= 14 * 3600) {
                    return offset;
                }
            }
        } catch (DateTimeException ignored) {
            // Reject named zones and unsupported offsets with the same diagnostic.
        }
        throw invalid("utc_offset must be a fixed +HH:MM or -HH:MM offset within 14 hours of UTC");
    }

    private static URI checkedOrigin(String value, boolean allowHttp) {
        try {
            URI url = URI.create(value);
            String scheme = url.getScheme();
            boolean permitted =
                    "https".equalsIgnoreCase(scheme)
                            || (allowHttp && "http".equalsIgnoreCase(scheme));
            if (permitted
                    && url.getHost() != null
                    && url.getRawUserInfo() == null
                    && url.getRawQuery() == null
                    && url.getRawFragment() == null
                    && url.getPort() != 0
                    && url.getPort() <= 65535
                    && (url.getRawPath().isEmpty() || "/".equals(url.getRawPath()))) {
                return URI.create(scheme.toLowerCase(Locale.ROOT) + "://" + url.getRawAuthority());
            }
        } catch (IllegalArgumentException ignored) {
            // Origins may contain private information, so do not forward parser exceptions.
        }
        throw invalid("base_url must be an HTTPS origin unless allow_insecure_http is enabled");
    }

    private static int range(int value, int max, String name) {
        if (value < 1 || value > max) {
            throw invalid(name + " must be between 1 and " + max);
        }
        return value;
    }

    private static String encode(String text) {
        try {
            return URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Adjust Source " + message);
    }

    static final class Metric implements Serializable {
        private static final long serialVersionUID = 1L;
        final String name;
        final int scale;

        private Metric(String name, int scale) {
            this.name = name;
            this.scale = scale;
        }
    }
}
