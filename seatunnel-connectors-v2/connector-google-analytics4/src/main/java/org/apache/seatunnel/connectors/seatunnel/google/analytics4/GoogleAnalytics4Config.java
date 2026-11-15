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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Getter;

import java.io.Serializable;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.BACKOFF;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.DIMENSIONS;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.EMULATOR_URL;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.KEY_FILE;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.METRICS;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.METRIC_TYPES;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.PROPERTY_ID;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.REPORT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4SourceOptions.TIMEOUT;

@Getter
final class GoogleAnalytics4Config implements Serializable {
    private static final long serialVersionUID = 1L;
    static final Set<String> FLOAT_TYPES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "TYPE_FLOAT",
                                    "TYPE_SECONDS",
                                    "TYPE_MILLISECONDS",
                                    "TYPE_MINUTES",
                                    "TYPE_HOURS",
                                    "TYPE_STANDARD",
                                    "TYPE_CURRENCY",
                                    "TYPE_FEET",
                                    "TYPE_MILES",
                                    "TYPE_METERS",
                                    "TYPE_KILOMETERS")));

    private final String propertyId;
    private final String startDate;
    private final String endDate;
    private final List<String> dimensions;
    private final List<String> metrics;
    private final List<String> metricTypes;
    private final String keyFile;
    private final String endpoint;
    private final int pageSize;
    private final int maxRows;
    private final int maxBytes;
    private final int timeout;
    private final int reportTimeout;
    private final int retries;
    private final int backoff;
    private final CatalogTable table;

    GoogleAnalytics4Config(ReadonlyConfig config) {
        propertyId = config.get(PROPERTY_ID);
        require(
                propertyId != null && propertyId.matches("[1-9][0-9]{0,19}"),
                "property_id must be numeric");
        startDate = config.get(START_DATE);
        endDate = config.get(END_DATE);
        require(!date(startDate).isAfter(date(endDate)), "start_date must not follow end_date");
        dimensions = names(config.get(DIMENSIONS), 0, 9, "dimensions");
        metrics = names(config.get(METRICS), 1, 10, "metrics");
        Set<String> all = new HashSet<>(dimensions);
        require(metrics.stream().allMatch(all::add), "dimensions and metrics must not overlap");
        metricTypes = Collections.unmodifiableList(new ArrayList<>(config.get(METRIC_TYPES)));
        require(metricTypes.size() == metrics.size(), "metric_types must match metrics length");
        for (String type : metricTypes) {
            require(
                    "TYPE_INTEGER".equals(type) || FLOAT_TYPES.contains(type),
                    "unsupported metric_types entry");
        }
        keyFile = config.getOptional(KEY_FILE).orElse(null);
        String emulator = config.getOptional(EMULATOR_URL).orElse(null);
        require(
                (keyFile == null) != (emulator == null),
                "exactly one of service_account_key_file and emulator_url is required");
        if (keyFile != null) {
            require(!keyFile.trim().isEmpty(), "service_account_key_file must not be blank");
        }
        endpoint = emulator == null ? "https://analyticsdata.googleapis.com" : emulator(emulator);
        pageSize = range(config.get(PAGE_SIZE), 1, 10000, "page_size");
        maxRows = range(config.get(MAX_ROWS), 1, 10000000, "max_report_rows");
        maxBytes = range(config.get(MAX_BYTES), 1024, 16777216, "max_response_bytes");
        timeout = range(config.get(TIMEOUT), 100, 120000, "request_timeout_ms");
        reportTimeout = range(config.get(REPORT_TIMEOUT), 100, 3600000, "report_timeout_ms");
        retries = range(config.get(RETRIES), 0, 5, "max_retries");
        backoff = range(config.get(BACKOFF), 1000, 120000, "max_retry_wait_ms");
        table = CatalogTableUtil.buildWithConfig(config);
        SeaTunnelRowType schema = table.getSeaTunnelRowType();
        List<String> names = new ArrayList<>(dimensions);
        names.addAll(metrics);
        require(
                Arrays.equals(schema.getFieldNames(), names.toArray(new String[0])),
                "schema fields must exactly match dimensions followed by metrics, in order");
        for (int i = 0; i < names.size(); i++) {
            Object expected =
                    i < dimensions.size()
                            ? BasicType.STRING_TYPE
                            : ("TYPE_INTEGER".equals(metricTypes.get(i - dimensions.size()))
                                    ? BasicType.LONG_TYPE
                                    : BasicType.DOUBLE_TYPE);
            require(
                    expected.equals(schema.getFieldType(i)),
                    "schema types must be STRING dimensions, BIGINT integer metrics and DOUBLE other metrics");
        }
    }

    private static List<String> names(List<String> values, int min, int max, String option) {
        require(
                values != null && values.size() >= min && values.size() <= max,
                option + " has invalid length");
        Set<String> unique = new HashSet<>();
        for (String name : values) {
            require(
                    name != null
                            && name.length() <= 256
                            && name.matches("[a-zA-Z][a-zA-Z0-9_:]*")
                            && unique.add(name),
                    option + " must contain unique API names");
        }
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static LocalDate date(String value) {
        require(
                value != null && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"),
                "dates must be absolute yyyy-MM-dd");
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("GA4 dates must be valid calendar dates");
        }
    }

    private static String emulator(String value) {
        try {
            URI uri = URI.create(value);
            require(
                    "http".equals(uri.getScheme())
                            && uri.getHost() != null
                            && !uri.getHost().endsWith("googleapis.com")
                            && uri.getUserInfo() == null
                            && uri.getQuery() == null
                            && uri.getFragment() == null
                            && (uri.getPath().isEmpty() || "/".equals(uri.getPath())),
                    "emulator_url must be a non-Google HTTP origin without credentials, path, query or fragment");
            return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("GA4 emulator_url must be a non-Google HTTP origin");
        }
    }

    private static int range(int value, int min, int max, String option) {
        require(value >= min && value <= max, option + " is outside its supported bounds");
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("GA4 " + message);
        }
    }
}
