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

package org.apache.seatunnel.connectors.seatunnel.acuityscheduling;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.CALENDAR_ID;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.DATE_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.LOOKBACK_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.MAX_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.RESUME_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.TIMEZONE;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.acuityscheduling.AcuitySchedulingSourceOptions.WINDOW_DAYS;

/** Holds the query scope and credentials. Only a credential-free query fingerprint enters state. */
final class AcuityConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final URI origin;
    final String authorization;
    final List<AcuityResource> resources;
    final LocalDate start;
    final LocalDate end;
    final ZoneId timezone;
    final int windowDays;
    final int dateLimit;
    final Integer calendarId;
    final int maxRequests;
    final int maxBytes;
    final int maxRows;
    final int connectTimeout;
    final int readTimeout;
    final int totalTimeout;
    final int maxAttempts;
    final String fingerprint;

    AcuityConfig(ReadonlyConfig input) {
        origin = origin(input.get(API_URL));
        String username = input.get(USERNAME);
        String password = input.get(PASSWORD);
        if (username == null || !username.matches("[0-9]{1,32}")) {
            throw invalid("username must be the numeric Acuity User ID");
        }
        if (password == null
                || password.isEmpty()
                || password.length() > 4096
                || password.chars().anyMatch(ch -> ch < 33 || ch > 126)) {
            throw invalid("password must be a nonempty printable Acuity API key");
        }
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.US_ASCII));
        Set<AcuityResource> selected = new LinkedHashSet<>();
        for (String name : input.get(RESOURCES)) {
            AcuityResource resource = AcuityResource.parse(name);
            if (!selected.add(resource)) {
                throw invalid("resources must not contain duplicates");
            }
        }
        if (selected.isEmpty()) {
            throw invalid("resources must not be empty");
        }
        resources = Collections.unmodifiableList(new ArrayList<>(selected));
        String zone = input.get(TIMEZONE);
        if (!"UTC".equals(zone) && !ZoneId.getAvailableZoneIds().contains(zone)) {
            throw invalid("timezone must be an IANA timezone ID");
        }
        timezone = ZoneId.of(zone);
        LocalDate first = date(input.get(START_DATE), START_DATE);
        end = date(input.get(END_DATE), END_DATE);
        LocalDate resume = date(input.get(RESUME_DATE), RESUME_DATE);
        int lookback = range(input, LOOKBACK_DAYS, 0, 90);
        if (resources.stream().anyMatch(resource -> resource.dated) && first == null) {
            throw invalid("start_date is required for appointments or blocks");
        }
        if ((resume != null || end != null) && first == null) {
            throw invalid("start_date is required when resume_date or end_date is supplied");
        }
        if (resume != null && (resume.isBefore(first) || (end != null && resume.isAfter(end)))) {
            throw invalid("resume_date must lie within the configured date bounds");
        }
        LocalDate rewind = resume == null ? first : resume.minusDays(lookback);
        start = rewind != null && rewind.isBefore(first) ? first : rewind;
        if (end != null && end.isBefore(start)) {
            throw invalid("end_date must not precede the effective start date");
        }
        windowDays = range(input, WINDOW_DAYS, 1, 365);
        dateLimit = range(input, DATE_LIMIT, 1, 100);
        calendarId = input.get(CALENDAR_ID);
        if (calendarId != null && calendarId < 1) {
            throw invalid("calendar_id must be positive");
        }
        maxRequests = range(input, MAX_REQUESTS, 1, 50000);
        maxBytes = range(input, MAX_BYTES, 1024, 16777216);
        maxRows = range(input, MAX_ROWS, 1, 50000);
        connectTimeout = range(input, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = range(input, READ_TIMEOUT, 1, 300000);
        totalTimeout = range(input, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = range(input, MAX_ATTEMPTS, 1, 10);
        fingerprint =
                digest(
                        origin
                                + "|"
                                + digest(username)
                                + "|"
                                + resources
                                + "|"
                                + first
                                + "|"
                                + start
                                + "|"
                                + end
                                + "|"
                                + timezone
                                + "|"
                                + windowDays
                                + "|"
                                + dateLimit
                                + "|"
                                + calendarId);
    }

    LocalDate frozenEnd() {
        LocalDate result = end == null ? LocalDate.now(timezone) : end;
        if (start != null && result.isBefore(start)) {
            throw invalid("effective start date is later than the crawl end date");
        }
        return result;
    }

    private static LocalDate date(String value, Option<String> key) {
        if (value == null) {
            return null;
        }
        try {
            LocalDate parsed = LocalDate.parse(value);
            if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}") || parsed.getYear() < 1) {
                throw invalid(key.key() + " must be a valid YYYY-MM-DD date");
            }
            return parsed;
        } catch (DateTimeException e) {
            throw invalid(key.key() + " must be a valid YYYY-MM-DD date");
        }
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getHost().length() > 253
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
                throw invalid("api_url must be a trusted HTTPS origin");
            }
            return new URI(
                    "https",
                    null,
                    uri.getHost().toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("api_url must be a trusted HTTPS origin");
        }
    }

    private static int range(ReadonlyConfig input, Option<Integer> key, int low, int high) {
        int value = input.get(key);
        if (value < low || value > high) {
            throw invalid(key.key() + " must be between " + low + " and " + high);
        }
        return value;
    }

    static String digest(String value) {
        try {
            byte[] sha =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(sha);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Acuity Scheduling source: " + reason);
    }
}
