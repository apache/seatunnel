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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.COMPANY;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.COMPANY_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.CUSTOM_REPORTS_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.EMPLOYEE_CHANGED_SINCE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.EMPLOYEE_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_RECORDS_PER_RESPONSE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TIMESHEET_WATERMARK;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TIME_OFF_WATERMARK;
import static org.apache.seatunnel.connectors.seatunnel.bamboohr.BambooHRSourceOptions.TOKEN;

/**
 * Validates provider inputs and freezes date bounds once per source run. The connector derives its
 * only HTTPS destination from a single DNS label.
 */
final class BambooHRConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern COMPANY_LABEL =
            Pattern.compile("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?");
    private static final Pattern FIELD = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_]{0,127}");
    private static final Pattern DATE = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final Pattern API_KEY = Pattern.compile("[\\x21-\\x39\\x3b-\\x7e]{1,512}");

    private static final LocalDate FIRST_DATE = LocalDate.of(1, 1, 1);
    private static final Instant FIRST_CHANGE = Instant.parse("0001-01-01T00:00:01Z");

    final String host;
    final String token;
    final Resource resource;
    final List<String> fields;
    final ZoneId zone;
    final LocalDate start;
    final LocalDate end;
    final LocalDate watermark;
    final Instant employeeChangedSince;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final int maxRecords;
    final String scope;

    BambooHRConfig(ReadonlyConfig config) {
        String company = config.get(COMPANY);
        if (company == null || !COMPANY_LABEL.matcher(company.toLowerCase(Locale.ROOT)).matches()) {
            throw invalid("company must be one BambooHR subdomain DNS label");
        }
        host = company.toLowerCase(Locale.ROOT) + ".bamboohr.com";
        token = config.get(TOKEN);
        if (token == null || !API_KEY.matcher(token).matches()) {
            throw invalid(
                    "token must contain a nonempty Basic API key without a colon or whitespace");
        }
        resource = Resource.parse(config.get(RESOURCE));
        try {
            zone = ZoneId.of(config.get(COMPANY_TIME_ZONE));
        } catch (DateTimeException e) {
            throw invalid("company_time_zone must be a valid IANA zone ID or UTC");
        }
        start = date(config.getOptional(START_DATE).orElse(null), zone, "start_date");
        end = date(config.getOptional(END_DATE).orElse(null), zone, "end_date");
        if (start != null && end != null && start.isAfter(end)) {
            throw invalid("start_date is after end_date");
        }
        rejectUnless(config, START_DATE, resource.isDated());
        rejectUnless(config, END_DATE, resource.isDated());
        rejectUnless(config, TIME_OFF_WATERMARK, resource == Resource.TIME_OFF);
        rejectUnless(config, TIMESHEET_WATERMARK, resource == Resource.TIMESHEETS);
        rejectUnless(config, CUSTOM_REPORTS_FIELDS, resource == Resource.REPORTS);
        rejectUnless(config, EMPLOYEE_CHANGED_SINCE, resource == Resource.EMPLOYEES);
        if (resource == Resource.REPORTS) {
            fields =
                    fields(
                            config.getOptional(CUSTOM_REPORTS_FIELDS).orElse(null),
                            "custom_reports_fields");
        } else if (resource == Resource.EMPLOYEES) {
            fields = fields(config.get(EMPLOYEE_FIELDS), "employee_fields");
        } else {
            fields = List.of();
        }
        String externalWatermark =
                resource == Resource.TIME_OFF
                        ? config.getOptional(TIME_OFF_WATERMARK).orElse(null)
                        : config.getOptional(TIMESHEET_WATERMARK).orElse(null);
        watermark =
                date(
                        externalWatermark,
                        zone,
                        resource == Resource.TIME_OFF
                                ? "time_off_watermark"
                                : "timesheet_watermark");
        employeeChangedSince = instant(config.getOptional(EMPLOYEE_CHANGED_SINCE).orElse(null));
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        maxRecords = range(config, MAX_RECORDS_PER_RESPONSE, 1, 100000);
        scope =
                BambooHRJson.digestText(
                        host
                                + "|"
                                + resource.id
                                + "|"
                                + zone
                                + "|"
                                + start
                                + "|"
                                + end
                                + "|"
                                + watermark
                                + "|"
                                + employeeChangedSince
                                + "|"
                                + String.join(",", fields));
    }

    /**
     * Computes the effective inclusive calendar range and change fence. Savepoint restore uses the
     * resulting bounds and does not apply the lookback a second time.
     */
    Bounds freeze(Instant now) {
        LocalDate today = now.atZone(zone).toLocalDate();
        if (employeeChangedSince != null && employeeChangedSince.isAfter(now)) {
            throw invalid("employee_changed_since is after the source initialization time");
        }
        if (!resource.isDated()) {
            return new Bounds(null, null, now);
        }
        LocalDate until = end == null ? today : laterDateWithin(end, today);
        LocalDate from =
                start == null
                        ? now.minus(Duration.ofHours(30 * 24 + 23)).atZone(zone).toLocalDate()
                        : start;
        if (watermark != null) {
            if (watermark.isAfter(until)) {
                throw invalid("the watermark is after end_date");
            }
            LocalDate lookback =
                    max(FIRST_DATE, watermark.minusDays(resource == Resource.TIME_OFF ? 365 : 30));
            from = start == null ? lookback : max(start, lookback);
        }
        if (resource == Resource.TIMESHEETS) {
            from = max(from, today.minusDays(364));
        }
        if (from.isAfter(until)) {
            throw invalid(
                    "the effective date window is empty or outside the timesheet retention period");
        }
        return new Bounds(from, until, now);
    }

    private static LocalDate laterDateWithin(LocalDate requested, LocalDate today) {
        if (requested.isAfter(today)) {
            throw invalid("end_date is after the company's current date");
        }
        return requested;
    }

    private static LocalDate max(LocalDate first, LocalDate second) {
        return first.isAfter(second) ? first : second;
    }

    private static LocalDate date(String text, ZoneId zone, String key) {
        if (text == null) {
            return null;
        }
        try {
            LocalDate value =
                    DATE.matcher(text).matches()
                            ? LocalDate.parse(text)
                            : OffsetDateTime.parse(text).atZoneSameInstant(zone).toLocalDate();
            if (value.getYear() >= 1 && value.getYear() <= 9999) {
                return value;
            }
        } catch (DateTimeException ignored) {
            // Report the config key without repeating potentially sensitive config values.
        }
        throw invalid(key + " must be a date or ISO 8601 timestamp with an explicit offset");
    }

    private static Instant instant(String value) {
        if (value == null) {
            return null;
        }
        try {
            Instant changed = OffsetDateTime.parse(value).toInstant();
            if (!changed.isBefore(FIRST_CHANGE)) {
                return changed;
            }
        } catch (DateTimeException ignored) {
            // Report only the parameter name and format.
        }
        throw invalid(
                "employee_changed_since must be an ISO 8601 timestamp with an explicit offset");
    }

    private static List<String> fields(String text, String key) {
        if (text == null || text.length() > 52000) {
            throw invalid(key + " must contain at least one field and at most 400 fields");
        }
        String[] raw = text.split(",", -1);
        if (raw.length > 400) {
            throw invalid(key + " must contain at most 400 fields");
        }
        List<String> parsed = new ArrayList<>();
        for (String field : raw) {
            String value = field.trim();
            if (!FIELD.matcher(value).matches()) {
                throw invalid(key + " contains an invalid field identifier");
            }
            parsed.add(value);
        }
        return List.copyOf(parsed);
    }

    private static void rejectUnless(ReadonlyConfig config, Option<String> key, boolean accepted) {
        if (!accepted && config.getOptional(key).isPresent()) {
            throw invalid(key.key() + " does not apply to the selected resource");
        }
    }

    private static int range(ReadonlyConfig config, Option<Integer> key, int min, int max) {
        int value = config.get(key);
        if (value < min || value > max) {
            throw invalid(key.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("BambooHR configuration: " + message);
    }

    /**
     * Each resource has its own wire contract and cursor semantics.
     *
     * <p>A resource's bounds and response format determine how its split is resumed.
     */
    enum Resource {
        REPORTS("custom_reports_stream"),
        DIRECTORY("employees_directory_stream"),
        META("meta_fields_stream"),
        TIME_OFF("time_off_requests_stream"),
        TIMESHEETS("timesheet_entries"),
        EMPLOYEES("employees");

        final String id;

        Resource(String id) {
            this.id = id;
        }

        boolean isDated() {
            return this == TIME_OFF || this == TIMESHEETS;
        }

        static Resource parse(String id) {
            for (Resource resource : values()) {
                if (resource.id.equals(id)) {
                    return resource;
                }
            }
            throw invalid("resource must name a supported BambooHR resource");
        }
    }

    /**
     * Immutable source-run bounds serialized in enumerator and reader state.
     *
     * <p>The same interval is retained before assignment and after a reader begins emitting
     * records.
     */
    static final class Bounds implements Serializable {
        private static final long serialVersionUID = 1L;
        final LocalDate from;
        final LocalDate until;
        final Instant initialized;

        Bounds(LocalDate from, LocalDate until, Instant initialized) {
            this.from = from;
            this.until = until;
            this.initialized = initialized;
        }
    }
}
