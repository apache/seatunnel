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

package org.apache.seatunnel.connectors.seatunnel.braze;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.INSTANCE;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.braze.BrazeSourceOptions.TOKEN;

/**
 * Validates one workspace and one export resource.
 *
 * <p>All targets use fixed paths and the REST host registered for the selected Braze instance.
 */
final class BrazeConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern KEY = Pattern.compile("[a-zA-Z0-9._~+/=-]{1,512}");
    private static final LocalDate EARLIEST = LocalDate.of(2000, 1, 1);
    private static final Map<String, String> ENDPOINTS =
            Map.ofEntries(
                    Map.entry("us-01", "rest.iad-01.braze.com"),
                    Map.entry("us-02", "rest.iad-02.braze.com"),
                    Map.entry("us-03", "rest.iad-03.braze.com"),
                    Map.entry("us-04", "rest.iad-04.braze.com"),
                    Map.entry("us-05", "rest.iad-05.braze.com"),
                    Map.entry("us-06", "rest.iad-06.braze.com"),
                    Map.entry("us-07", "rest.iad-07.braze.com"),
                    Map.entry("us-08", "rest.iad-08.braze.com"),
                    Map.entry("us-10", "rest.us-10.braze.com"),
                    Map.entry("eu-01", "rest.fra-01.braze.eu"),
                    Map.entry("eu-02", "rest.fra-02.braze.eu"),
                    Map.entry("au-01", "rest.au-01.braze.com"),
                    Map.entry("id-01", "rest.id-01.braze.com"),
                    Map.entry("jp-01", "rest.jp-01.braze.com"),
                    Map.entry("kr-01", "rest.kr-01.braze.com"));

    final String host;
    final String token;
    final Resource resource;
    final LocalDate start;
    final LocalDate end;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BrazeConfig(ReadonlyConfig config) {
        String instance = config.get(INSTANCE);
        host = instance == null ? null : ENDPOINTS.get(instance.toLowerCase(Locale.ROOT));
        if (host == null) {
            throw invalid("instance must match a documented Braze REST instance");
        }
        token = config.get(TOKEN);
        if (token == null || !KEY.matcher(token).matches()) {
            throw invalid("token must be a valid Braze REST API key without whitespace");
        }
        resource = Resource.parse(config.get(RESOURCE));
        start = day(config.getOptional(START_DATE).orElse(null), "start_date");
        end = day(config.getOptional(END_DATE).orElse(null), "end_date");
        if (resource.days > 0 && start == null) {
            throw invalid("start_date is required for analytics and KPI resources");
        }
        if (resource.days == 0 && end != null) {
            throw invalid("end_date applies only to analytics and KPI resources");
        }
        if (start != null && resource.days == 0 && !resource.editFilter) {
            throw invalid("start_date on a list requires campaigns, canvases or cards");
        }
        if (end != null && start.isAfter(end)) {
            throw invalid("start_date must be before or equal to end_date");
        }
        maxPages = range(config, MAX_PAGES, 1, 20000);
        connectTimeout = range(config, CONNECT_TIMEOUT_MS, 1, 120000);
        readTimeout = range(config, READ_TIMEOUT_MS, 1, 120000);
        deadlineMs = range(config, REQUEST_DEADLINE_MS, 1, 300000);
        maxAttempts = range(config, MAX_ATTEMPTS, 1, 10);
        maxBytes = range(config, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        scope =
                BrazeJson.fingerprint(
                        List.of(
                                host,
                                token,
                                resource.id,
                                String.valueOf(start),
                                String.valueOf(end)));
    }

    /**
     * Freezes today's partial UTC bucket at the initial instant, or the configured last UTC day.
     *
     * @param now the coordinator's initial instant
     * @return the inclusive analytics cutoff, absent for non-series resources
     */
    Instant freeze(Instant now) {
        LocalDate today = utcDay(now);
        if ((start != null && start.isAfter(today)) || (end != null && end.isAfter(today))) {
            throw invalid("start_date and end_date must be on or before the current UTC date");
        }
        if (resource.days == 0) {
            return null;
        }
        return end == null || end.equals(today) ? now.truncatedTo(ChronoUnit.SECONDS) : dayEnd(end);
    }

    /**
     * Builds a numbered list target. An independent unfiltered inventory drives child exports.
     *
     * @param collection the selected or parent list resource
     * @param page the zero-based numbered page
     * @param discovery whether this list is used to discover parents
     * @return the request target on the instance host
     */
    String listTarget(Resource collection, int page, boolean discovery) {
        return collection.path
                + "?page="
                + page
                + (discovery || start == null || !collection.editFilter
                        ? ""
                        : "&last_edit.time%5Bgt%5D=" + encode(start + "T00:00:00Z"));
    }

    /**
     * Builds the details or series target from a validated parent ID and date window.
     *
     * @param id the provider parent ID when required
     * @param from the first day in this series window
     * @param cutoff the initial scan's inclusive upper instant
     * @return the request target with explicitly encoded query values
     */
    String recordTarget(String id, LocalDate from, Instant cutoff) {
        String parent = resource.parent == null ? "" : resource.param + "=" + encode(id);
        if (resource.days == 0) {
            return resource.path + "?" + parent;
        }
        LocalDate until = lastDay(from, cutoff);
        Instant ending = until.equals(utcDay(cutoff)) ? cutoff : dayEnd(until);
        return resource.path
                + "?"
                + (parent.isEmpty() ? "" : parent + "&")
                + "length="
                + (ChronoUnit.DAYS.between(from, until) + 1)
                + "&ending_at="
                + encode(ending.toString())
                + (resource == Resource.CANVASES_ANALYTICS
                        ? "&include_variant_breakdown=true&include_step_breakdown=true"
                        : "")
                + (resource == Resource.EVENTS_ANALYTICS || resource == Resource.CARDS_ANALYTICS
                        ? "&unit=day"
                        : "");
    }

    LocalDate lastDay(LocalDate from, Instant cutoff) {
        LocalDate endOfWindow = from.plusDays(resource.days - 1L);
        LocalDate last = utcDay(cutoff);
        return endOfWindow.isBefore(last) ? endOfWindow : last;
    }

    static LocalDate utcDay(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDate();
    }

    static Instant dayEnd(LocalDate day) {
        return day.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).minusSeconds(1);
    }

    static boolean validId(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 512
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static LocalDate day(String text, String option) {
        if (text == null) {
            return null;
        }
        try {
            LocalDate value = LocalDate.parse(text);
            if (text.length() == 10 && !value.isBefore(EARLIEST) && value.getYear() <= 9999) {
                return value;
            }
        } catch (DateTimeException ignored) {
            // Configuration errors must not carry option values.
        }
        throw invalid(option + " must be a YYYY-MM-DD date on or after 2000-01-01");
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Braze configuration: " + reason);
    }

    /**
     * Declares the selected route, wire result shape and parent traversal.
     *
     * <p>Legacy News Feed exports retain their original routes and have an explicit availability
     * boundary. Their list is read until an empty page because its current page size is
     * unpublished.
     */
    enum Resource {
        CAMPAIGNS("campaigns", "/campaigns/list", "campaigns", 100, true),
        CAMPAIGNS_ANALYTICS(
                "campaigns_analytics",
                "/campaigns/data_series",
                CAMPAIGNS,
                "campaign_id",
                "data",
                100),
        CAMPAIGNS_DETAILS(
                "campaigns_details", "/campaigns/details", CAMPAIGNS, "campaign_id", null, 0),
        CANVASES("canvases", "/canvas/list", "canvases", 100, true),
        CANVASES_ANALYTICS(
                "canvases_analytics", "/canvas/data_series", CANVASES, "canvas_id", "stats", 14),
        CANVASES_DETAILS("canvases_details", "/canvas/details", CANVASES, "canvas_id", null, 0),
        EVENTS("events", "/events/list", "events", 250, false),
        EVENTS_ANALYTICS("events_analytics", "/events/data_series", EVENTS, "event", "data", 100),
        KPI_NEW_USERS("kpi_daily_new_users", "/kpi/new_users/data_series", null, null, "data", 100),
        KPI_ACTIVE_USERS("kpi_daily_active_users", "/kpi/dau/data_series", null, null, "data", 100),
        KPI_UNINSTALLS(
                "kpi_daily_app_uninstalls", "/kpi/uninstalls/data_series", null, null, "data", 100),
        CARDS("cards", "/feed/list", "cards", 1000, true),
        CARDS_ANALYTICS("cards_analytics", "/feed/data_series", CARDS, "card_id", "data", 100),
        SEGMENTS("segments", "/segments/list", "segments", 100, false),
        SEGMENTS_ANALYTICS(
                "segments_analytics", "/segments/data_series", SEGMENTS, "segment_id", "data", 100),
        SEGMENTS_DETAILS("segments_details", "/segments/details", SEGMENTS, "segment_id", null, 0);

        final String id;
        final String path;
        final String member;
        final int listSize;
        final boolean editFilter;
        final Resource parent;
        final String param;
        final String lineage;
        final int days;

        Resource(String id, String path, String member, int listSize, boolean editFilter) {
            this.id = id;
            this.path = path;
            this.member = member;
            this.listSize = listSize;
            this.editFilter = editFilter;
            this.parent = null;
            this.param = null;
            this.lineage = "events".equals(id) ? "event_name" : null;
            this.days = 0;
        }

        Resource(String id, String path, Resource parent, String param, String member, int days) {
            this.id = id;
            this.path = path;
            this.member = member;
            this.listSize = 0;
            this.editFilter = false;
            this.parent = parent;
            this.param = param;
            this.lineage = "event".equals(param) ? "event_name" : param;
            this.days = days;
        }

        static Resource parse(String value) {
            for (Resource resource : values()) {
                if (resource.id.equals(value)) {
                    return resource;
                }
            }
            throw invalid("resource must name a supported Braze export");
        }
    }
}
