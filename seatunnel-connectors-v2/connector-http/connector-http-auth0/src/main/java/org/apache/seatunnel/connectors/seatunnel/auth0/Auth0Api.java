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
package org.apache.seatunnel.connectors.seatunnel.auth0;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Builds same-tenant Management API requests and validates the provider's distinct list responses.
 *
 * <p>Only the network worker owns the cached OAuth credential and its monotonic renewal clock.
 */
final class Auth0Api {
    // The users endpoint returns at most 1000 results for one search query.
    static final int USER_CAP = 1000;
    private static final String API = "/api/v2/";
    private static final DateTimeFormatter MILLIS =
            new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

    private final Auth0Config config;
    private final Auth0Http http;
    // Renewal uses elapsed time so a wall-clock correction cannot extend a token lifetime.
    private String cachedToken;
    private long renewAt;

    Auth0Api(Auth0Config config, Auth0Http http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Probes window density before allocating any offset against the capped users search.
     *
     * <p>A saturated result is split by the reader before any rows in that range are committed.
     */
    int count(Auth0Split.Window window, long deadline) throws InterruptedException {
        Auth0Resource resource = Auth0Resource.USERS;
        JsonNode response = get(resource, userTarget(window, 0, 0, true), deadline);
        JsonNode total = response.get("total");
        JsonNode data = response.get("users");
        if (!response.isObject()
                || total == null
                || !total.isIntegralNumber()
                || !total.canConvertToInt()
                || total.intValue() < 0
                || data == null
                || !data.isArray()
                || !data.isEmpty()) {
            throw resource.failure("invalid user count response");
        }
        return total.intValue();
    }

    /**
     * Fetches one complete page and normalizes its validated identities for checkpoint replay.
     *
     * <p>Parents and raw cursors are encoded as URI data. They cannot change the HTTPS authority,
     * route structure, or existing query keys.
     */
    Page page(
            Auth0Resource resource,
            Auth0Split.Position position,
            Auth0Split.Window window,
            String organization,
            String user,
            long deadline)
            throws InterruptedException {
        String route;
        switch (resource) {
            case CLIENTS:
                route = API + "clients" + offset(position.number, config.pageSize, false);
                break;
            case USERS:
                route = userTarget(window, position.number, config.pageSize, false);
                break;
            case ORGANIZATIONS:
                route = API + "organizations" + checkpoint(position.cursor);
                break;
            case ORGANIZATION_MEMBERS:
                route =
                        API
                                + "organizations/"
                                + encoded(organization)
                                + "/members"
                                + checkpoint(position.cursor);
                break;
            case ORGANIZATION_MEMBER_ROLES:
                route =
                        API
                                + "organizations/"
                                + encoded(organization)
                                + "/members/"
                                + encoded(user)
                                + "/roles"
                                + offset(position.number, config.pageSize, false);
                break;
            default:
                throw new IllegalStateException("Unknown Auth0 API resource");
        }
        JsonNode response = get(resource, route, deadline);
        JsonNode records = response;
        String next = null;
        if (resource.envelope != null) {
            records = response.get(resource.envelope);
            if (!response.isObject()) {
                throw resource.failure("expected a checkpoint response object");
            }
            JsonNode cursor = response.get("next");
            if (cursor != null && !cursor.isNull()) {
                next = Auth0Resource.text(cursor, 4096);
                if (next == null) {
                    throw resource.failure("invalid pagination cursor");
                }
            }
        }
        if (records == null || !records.isArray() || records.size() > config.pageSize) {
            throw resource.failure("invalid or oversized page");
        }
        List<Row> rows = new ArrayList<>(records.size());
        Set<String> identities = new HashSet<>();
        long previous = Long.MIN_VALUE;
        for (JsonNode record : records) {
            if (!record.isObject()) {
                throw resource.failure("expected each record to be a JSON object");
            }
            String id = resource.id(record);
            if (!identities.add(id)) {
                throw resource.failure("page repeats a record identity");
            }
            long updated =
                    resource == Auth0Resource.USERS ? updateTime(resource, record, window) : 0;
            if (updated < previous) {
                throw resource.failure("users page is not ordered by updated_at");
            }
            previous = updated;
            rows.add(new Row(id, record, updated));
        }
        return new Page(
                rows,
                next,
                Auth0Config.hash(records.toString()),
                resource.envelope == null && rows.size() == config.pageSize);
    }

    private long updateTime(Auth0Resource resource, JsonNode record, Auth0Split.Window window) {
        String value = Auth0Resource.text(record.get("updated_at"), 80);
        if (value != null) {
            try {
                Instant time = OffsetDateTime.parse(value).toInstant();
                long millisecond = time.toEpochMilli();
                if (time.getNano() % 1000000 == 0
                        && millisecond >= window.first
                        && millisecond <= window.last) {
                    return millisecond;
                }
            } catch (ArithmeticException | DateTimeException ex) {
                // Do not include raw record fields in the exception.
            }
        }
        throw resource.failure(
                "record updated_at falls outside the user search window or is invalid");
    }

    private String checkpoint(String cursor) {
        return "?take=" + config.pageSize + (cursor == null ? "" : "&from=" + encoded(cursor));
    }

    private static String userTarget(Auth0Split.Window window, int page, int size, boolean count) {
        String query =
                "updated_at:["
                        + MILLIS.format(Instant.ofEpochMilli(window.first))
                        + " TO "
                        + MILLIS.format(Instant.ofEpochMilli(window.last))
                        + "]";
        return API
                + "users"
                + offset(page, size, count)
                + "&search_engine=v3&sort=updated_at%3A1&q="
                + encoded(query);
    }

    private static String offset(int page, int size, boolean total) {
        return "?page=" + page + "&per_page=" + size + "&include_totals=" + total;
    }

    private static String encoded(String input) {
        return URLEncoder.encode(input, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private JsonNode get(Auth0Resource resource, String target, long deadline)
            throws InterruptedException {
        try {
            return http.request(resource, target, token(deadline), null, deadline);
        } catch (Auth0Http.Unauthorized challenge) {
            if (config.token == null) {
                cachedToken = null;
                try {
                    return http.request(resource, target, token(deadline), null, deadline);
                } catch (Auth0Http.Unauthorized rejected) {
                    cachedToken = null;
                    throw resource.failure("HTTP 401 after OAuth renewal");
                }
            }
            throw resource.failure("HTTP 401");
        }
    }

    private String token(long deadline) throws InterruptedException {
        if (config.token != null) {
            return config.token;
        }
        if (cachedToken == null || System.nanoTime() - renewAt >= 0) {
            ObjectNode grant =
                    http.json
                            .createObjectNode()
                            .put("grant_type", "client_credentials")
                            .put("client_id", config.clientId)
                            .put("client_secret", config.clientSecret)
                            .put("audience", config.audience);
            byte[] payload;
            try {
                payload = http.json.writeValueAsBytes(grant);
            } catch (JsonProcessingException ex) {
                throw Auth0Http.failure(null, "cannot encode client credentials grant");
            }
            long issued = System.nanoTime();
            JsonNode response = http.request(null, "/oauth/token", null, payload, deadline);
            String accessToken = Auth0Resource.text(response.get("access_token"), 8192);
            String type = Auth0Resource.text(response.get("token_type"), 32);
            JsonNode expires = response.get("expires_in");
            if (accessToken == null
                    || !accessToken.chars().allMatch(c -> c >= 33 && c <= 126)
                    || !"Bearer".equalsIgnoreCase(type)
                    || expires == null
                    || !expires.isIntegralNumber()
                    || !expires.canConvertToInt()
                    || expires.intValue() <= 0) {
                throw Auth0Http.failure(null, "invalid client credentials response");
            }
            long ttl = TimeUnit.SECONDS.toNanos(Math.min(expires.intValue(), 86400));
            renewAt = issued + ttl - Math.min(TimeUnit.SECONDS.toNanos(30), ttl / 10);
            cachedToken = accessToken;
        }
        return cachedToken;
    }

    /**
     * One validated provider record, its identity and its optional user search watermark.
     *
     * <p>The document stays in reader memory until its page is consumed.
     */
    static final class Row {
        final String id;
        final JsonNode record;
        final long updated;

        Row(String id, JsonNode record, long updated) {
            this.id = id;
            this.record = record;
            this.updated = updated;
        }
    }

    /**
     * Validated rows and enough pagination metadata to commit or reconstruct progress.
     *
     * <p>Only the digest, cursors and row offsets are eligible for split state.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final String digest;
        final boolean full;

        Page(List<Row> rows, String next, String digest, boolean full) {
            this.rows = rows;
            this.next = next;
            this.digest = Auth0Config.hash(digest + "\n" + (next == null ? "0" : "1" + next));
            this.full = full;
        }
    }
}
