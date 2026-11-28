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

package org.apache.seatunnel.connectors.seatunnel.brevo;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.brevo.BrevoResource.TimeFilter;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.TLS_TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.brevo.BrevoSourceOptions.TOKEN;

/**
 * Validates a bounded Brevo export and generates fixed-origin request targets.
 *
 * <p>Restore is bound to one resource, key, selected page size and date interval through a digest.
 */
final class BrevoConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String HOST = "api.brevo.com";
    static final int MAX_UNPAGED_ROWS = 50_000;
    private static final int WINDOW_DAYS = 30;
    private static final Pattern KEY = Pattern.compile("[\\x21-\\x7e]{1,1024}");
    private static final DateTimeFormatter MILLIS =
            new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

    final BrevoResource resource;
    final String token;
    final String trustCertificate;
    final Instant start;
    final Instant end;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BrevoConfig(ReadonlyConfig config) {
        token = config.get(TOKEN);
        if (token == null || !KEY.matcher(token).matches()) {
            throw invalid("token must be a Brevo API key without whitespace");
        }
        resource = BrevoResource.parse(config.get(RESOURCE));
        trustCertificate = config.getOptional(TLS_TRUST_CERTIFICATE).orElse(null);
        if (trustCertificate != null
                && (trustCertificate.isBlank()
                        || trustCertificate.length() > 4096
                        || trustCertificate.indexOf('\0') >= 0)) {
            throw invalid("tls_trust_certificate must be a valid engine-local certificate path");
        }
        start = instant(config.getOptional(START_DATE).orElse(null), "start_date");
        end = instant(config.getOptional(END_DATE).orElse(null), "end_date");
        if (resource.filter == TimeFilter.NONE && (start != null || end != null)) {
            throw invalid("date options require a resource with a modification or event time");
        }
        if (resource.filter.windowed() && start == null) {
            throw invalid("start_date is required for event reports");
        }
        if (start != null && end != null && start.isAfter(end)) {
            throw invalid("start_date must be before or equal to end_date");
        }

        Integer preferred = config.getOptional(PAGE_SIZE).orElse(null);
        if (preferred != null
                && (resource.maxPageSize == 0
                        || preferred < 1
                        || preferred > resource.maxPageSize)) {
            throw invalid("page_size exceeds the selected resource's supported range");
        }
        pageSize = preferred == null ? resource.maxPageSize : preferred;
        maxPages = range(config, MAX_PAGES, 1, 20000);
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                BrevoJson.fingerprint(
                        List.of(
                                token,
                                resource.id,
                                String.valueOf(start),
                                String.valueOf(end),
                                Integer.toString(pageSize)));
    }

    /**
     * Pins the upper bound of a timestamped extraction at initial enumeration.
     *
     * @param now the initial wall-clock instant
     * @return cutoff for a selected date interval, or null for a full scan
     */
    Instant freeze(Instant now) {
        Instant current = now.truncatedTo(ChronoUnit.MILLIS);
        if ((start != null && start.isAfter(current)) || (end != null && end.isAfter(current))) {
            throw invalid("start_date and end_date must not be in the future");
        }
        return end == null && start != null ? current : end;
    }

    /**
     * Gives the inclusive end of one report request and the corresponding local window.
     *
     * <p>Inbound uses 30 exact UTC days by milliseconds. Transactional reports take inclusive
     * calendar dates, so their next window starts at UTC midnight even after a partial first day.
     *
     * @param from the active report window's precise lower bound
     * @param cutoff the frozen upper bound of the scan
     * @return the lesser of the endpoint's window edge and the extraction cutoff
     */
    Instant last(Instant from, Instant cutoff) {
        Instant boundary =
                resource.filter == TimeFilter.INBOUND
                        ? from.plus(WINDOW_DAYS, ChronoUnit.DAYS).minusMillis(1)
                        : utcDate(from)
                                .plusDays(WINDOW_DAYS)
                                .atStartOfDay()
                                .toInstant(ZoneOffset.UTC)
                                .minusMillis(1);
        return boundary.isBefore(cutoff) ? boundary : cutoff;
    }

    /**
     * Creates one relative GET target using documented pagination and filter semantics.
     *
     * @param selected the resource being requested, including parent discovery when applicable
     * @param parent the positive numeric ID for a child endpoint, absent for discovery
     * @param position provider offset or company page number
     * @param from optional lower report window boundary
     * @param until optional upper report window boundary
     * @return the URI relative to the fixed modern Brevo endpoint
     */
    String target(BrevoResource selected, Long parent, int position, Instant from, Instant until) {
        String path =
                parent == null
                        ? selected.path
                        : selected.path.replace("{parent}", parent.toString());
        if (selected.maxPageSize == 0) {
            return "/v3" + path;
        }
        String base =
                "/v3"
                        + path
                        + "?limit="
                        + pageSize(selected)
                        + (selected == BrevoResource.COMPANIES ? "&page=" : "&offset=")
                        + position
                        + "&sort=asc";
        if (selected != resource) {
            return base;
        }
        if (resource.filter == TimeFilter.MODIFIED_SINCE && start != null) {
            // The service lower bound is exclusive. Read one extra millisecond and filter locally.
            return base + "&modifiedSince=" + encode(MILLIS.format(start.minusMillis(1)));
        }
        if (resource.filter == TimeFilter.INBOUND) {
            return base
                    + "&startDate="
                    + encode(MILLIS.format(from))
                    + "&endDate="
                    + encode(MILLIS.format(until));
        }
        if (resource.filter == TimeFilter.TRANSACTIONAL) {
            return base + "&startDate=" + utcDate(from) + "&endDate=" + utcDate(until);
        }
        return base;
    }

    int pageSize(BrevoResource selected) {
        return selected == resource ? pageSize : selected.maxPageSize;
    }

    static LocalDate utcDate(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static Instant instant(String value, String option) {
        if (value == null) {
            return null;
        }
        try {
            OffsetDateTime date = OffsetDateTime.parse(value);
            if (date.getYear() >= 1970
                    && date.getYear() <= 9999
                    && date.getNano() % 1_000_000 == 0) {
                return date.toInstant();
            }
        } catch (DateTimeException ignored) {
            // Retain only the option key because a bad value may be sensitive.
        }
        throw invalid(
                option + " must be an ISO-8601 timestamp with an offset and at most milliseconds");
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Brevo configuration: " + reason);
    }
}
