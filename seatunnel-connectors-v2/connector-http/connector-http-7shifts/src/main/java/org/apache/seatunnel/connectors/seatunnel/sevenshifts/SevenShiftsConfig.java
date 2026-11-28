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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.API_VERSION;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.TOKEN;

/**
 * Validates one bounded export without retaining credentials in its checkpoint identity. Resource,
 * origin, API version, inclusive cutoff and page size determine which saved scan can be resumed.
 */
final class SevenShiftsConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter COMPACT_OFFSET =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .appendOffset("+HHMM", "Z")
                    .toFormatter();

    final SevenShiftsResource resource;
    final URI origin;
    final String token;
    final String apiVersion;
    final Instant startDate;
    final int pageSize;
    final int timeoutMs;
    final int maxAttempts;

    SevenShiftsConfig(ReadonlyConfig config) {
        resource = SevenShiftsResource.parse(config.get(RESOURCE));
        origin = origin(config.get(BASE_URL), config.get(ALLOW_INSECURE_HTTP));
        token = config.get(TOKEN);
        if (token.isEmpty()
                || token.length() > 4096
                || !token.chars().allMatch(ch -> ch >= 0x21 && ch <= 0x7e)) {
            throw invalid("token must be a nonempty Bearer token without whitespace");
        }
        apiVersion = config.get(API_VERSION);
        try {
            if (!apiVersion.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                throw invalid("api_version must use YYYY-MM-DD");
            }
            LocalDate.parse(apiVersion);
        } catch (DateTimeParseException e) {
            throw invalid("api_version must use a valid YYYY-MM-DD");
        }
        startDate = config.getOptional(START_DATE).map(SevenShiftsConfig::startDate).orElse(null);
        if (startDate != null && resource.modification == SevenShiftsResource.Modification.NONE) {
            throw invalid("start_date is unsupported for this resource");
        }
        pageSize = range(config.get(PAGE_SIZE), 100, "page_size");
        timeoutMs = range(config.get(REQUEST_TIMEOUT_MS), 300000, "request_timeout_ms");
        maxAttempts = range(config.get(MAX_ATTEMPTS), 8, "max_attempts");
    }

    String checkpointKey() {
        return "accepted-prefix-v2|"
                + resource.apiName
                + "|"
                + origin
                + "|"
                + apiVersion
                + "|"
                + startDate
                + "|"
                + pageSize;
    }

    /**
     * Overlap the API's coarse or exclusive lower bound, then let the reader enforce the exact
     * inclusive UTC cutoff against returned modification timestamps.
     */
    String providerCutoff() {
        if (startDate == null) {
            return null;
        }
        if (resource.modification == SevenShiftsResource.Modification.DAY) {
            return startDate.atOffset(ZoneOffset.UTC).toLocalDate().minusDays(1).toString();
        }
        return startDate.truncatedTo(ChronoUnit.SECONDS).minusSeconds(1).toString();
    }

    static Instant parseTimestamp(String value) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(value, COMPACT_OFFSET).toInstant();
        }
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("7shifts Source " + message);
    }

    private static Instant startDate(String value) {
        try {
            Instant date = OffsetDateTime.parse(value).toInstant();
            int year = date.atOffset(ZoneOffset.UTC).getYear();
            if (year < 2 || year > 9999) {
                throw invalid("start_date UTC year must be between 0002 and 9999");
            }
            return date;
        } catch (DateTimeParseException e) {
            throw invalid("start_date must be an ISO-8601 timestamp with an offset");
        }
    }

    private static URI origin(String value, boolean allowHttp) {
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw invalid("base_url must be a trusted API origin");
        }
        boolean https = "https".equalsIgnoreCase(uri.getScheme());
        if ((!https && !(allowHttp && "http".equalsIgnoreCase(uri.getScheme())))
                || uri.getHost() == null
                || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || !(uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                || uri.getPort() == 0
                || uri.getPort() > 65535) {
            throw invalid("base_url must be an HTTPS origin unless allow_insecure_http is enabled");
        }
        return URI.create(uri.getScheme().toLowerCase(Locale.ROOT) + "://" + uri.getRawAuthority());
    }

    private static int range(int value, int max, String name) {
        if (value < 1 || value > max) {
            throw invalid(name + " must be between 1 and " + max);
        }
        return value;
    }
}
