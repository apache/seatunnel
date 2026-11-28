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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reads the five Basecamp resources and controls the reader's Launchpad credential cache.
 *
 * <p>All data routes use fixed scope, and authentication and upstream payloads stay out of
 * recoverable scan state.
 */
final class BasecampApi {
    private final BasecampConfig config;
    private final BasecampHttp http;
    private final String accountPath;

    // Launchpad may return a replacement refresh value. This cache belongs only to one reader.
    private String currentRefresh;
    private String access;
    private long expires;

    BasecampApi(BasecampConfig config, BasecampHttp http) {
        this.config = config;
        this.http = http;
        accountPath = "/" + config.account;
        currentRefresh = config.refreshToken;
    }

    /**
     * Reads a validated entity page, including the descending order contract for feeds.
     *
     * <p>Equality of adjacent timestamps is valid. The opaque Link query and the row fingerprint
     * are independently maintained for the same route.
     */
    Page page(
            BasecampResource resource,
            BasecampSplit.Position position,
            String project,
            String schedule,
            long deadline)
            throws InterruptedException {
        Map<String, String> selectors = new LinkedHashMap<>();
        String path;
        switch (resource) {
            case PROJECTS:
                path = accountPath + "/projects.json";
                break;
            case SCHEDULE_ENTRIES:
                path =
                        accountPath
                                + "/buckets/"
                                + project
                                + "/schedules/"
                                + schedule
                                + "/entries.json";
                break;
            case TODOS:
            case MESSAGES:
                path = accountPath + "/projects/recordings.json";
                selectors.put("type", resource.recordingType);
                selectors.put("sort", "updated_at");
                selectors.put("direction", "desc");
                break;
            default:
                throw resource.failure("unsupported page resource");
        }
        String initialQuery =
                resource.recordingType == null
                        ? ""
                        : "?type=" + resource.recordingType + "&sort=updated_at&direction=desc";
        String query =
                position.query == null
                        ? initialQuery
                        : BasecampLink.resume(resource, path, selectors, position.query);
        BasecampHttp.Reply reply = get(resource, path + query, deadline);
        if (!reply.json.isArray() || reply.json.size() > config.maxPageItems) {
            throw resource.failure("invalid or oversized record array");
        }
        String next = BasecampLink.next(resource, path, selectors, reply.links);
        if (next != null && (next.equals(query) || next.equals(initialQuery))) {
            throw resource.failure("repeated continuation link");
        }

        List<Row> rows = new ArrayList<>(reply.json.size());
        Set<String> identifiers = new HashSet<>();
        Instant previous = position.previousUpdated;
        for (JsonNode raw : reply.json) {
            String id = resource.id(raw);
            if (!identifiers.add(id) || position.previousIds.contains(BasecampConfig.hash(id))) {
                throw resource.failure("record ID repeats within consecutive pages");
            }
            String updated = timestamp(resource, raw);
            Instant when = updated == null ? null : parseTime(resource, updated);
            String owner = project;
            if (resource.recordingType != null) {
                if (when == null || previous != null && when.isAfter(previous)) {
                    throw resource.failure("missing or out-of-order recording updated_at");
                }
                previous = when;
                JsonNode bucket = raw.get("bucket");
                if (bucket != null && bucket.isObject() && bucket.hasNonNull("id")) {
                    owner = resource.id(bucket);
                }
            }
            rows.add(new Row(id, owner, schedule, updated, when, raw));
        }
        String digest =
                BasecampConfig.hash(
                        reply.json.toString()
                                + "\n"
                                + (next == null ? "terminal" : "next:" + next));
        return new Page(rows, next, previous, digest);
    }

    /**
     * Discovers every schedule in the actual parent project's dock.
     *
     * <p>Schedule rows include the parent fields expected by downstream consumers. A disabled
     * schedule is retained as a dock row while its children are left unqueried.
     */
    Dock dock(String project, long deadline) throws InterruptedException {
        BasecampResource resource = BasecampResource.SCHEDULES;
        String path = accountPath + "/projects/" + project + ".json";
        BasecampHttp.Reply reply = get(resource, path, deadline);
        if (!project.equals(resource.id(reply.json))) {
            throw resource.failure("project detail returned a different parent ID");
        }
        if (BasecampLink.next(resource, path, Map.of(), reply.links) != null) {
            throw resource.failure("unexpected continuation on a single project detail");
        }
        JsonNode dock = reply.json.get("dock");
        if (dock == null || dock.isNull()) {
            return new Dock(List.of(), BasecampConfig.hash("empty dock"));
        }
        if (!dock.isArray() || dock.size() > config.maxPageItems) {
            throw resource.failure("invalid or oversized project dock");
        }
        List<Schedule> found = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (JsonNode raw : dock) {
            if (!raw.isObject() || !raw.path("name").isTextual()) {
                throw resource.failure("invalid project dock tool");
            }
            if (!"schedule".equals(raw.path("name").asText())) {
                continue;
            }
            String id = resource.id(raw);
            JsonNode flag = raw.get("enabled");
            if (!ids.add(id) || flag != null && !flag.isBoolean()) {
                throw resource.failure("invalid or repeated schedule tool");
            }
            ObjectNode record = raw.deepCopy();
            record.put("project_id", new BigInteger(project));
            record.putObject("substream_data")
                    .put("project_id", new BigInteger(project))
                    .put("schedule_id", new BigInteger(id));
            String updated = timestamp(resource, raw);
            found.add(
                    new Schedule(
                            new Row(id, project, id, updated, null, record),
                            flag == null || flag.booleanValue()));
        }
        return new Dock(found, BasecampConfig.hash(dock.toString()));
    }

    private BasecampHttp.Reply get(BasecampResource resource, String target, long deadline)
            throws InterruptedException {
        try {
            return http.request(
                    BasecampHttp.Route.DATA, resource, target, token(deadline), null, deadline);
        } catch (BasecampHttp.Unauthorized first) {
            if (config.token == null) {
                access = null;
                try {
                    return http.request(
                            BasecampHttp.Route.DATA,
                            resource,
                            target,
                            token(deadline),
                            null,
                            deadline);
                } catch (BasecampHttp.Unauthorized second) {
                    access = null;
                    throw resource.failure("HTTP 401 after Launchpad token refresh");
                }
            }
            throw resource.failure("HTTP 401");
        }
    }

    private String token(long deadline) throws InterruptedException {
        if (config.token != null) {
            return config.token;
        }
        if (access != null && System.nanoTime() - expires < 0) {
            return access;
        }
        String form =
                "type=refresh&client_id="
                        + encoded(config.clientId)
                        + "&client_secret="
                        + encoded(config.clientSecret)
                        + "&refresh_token="
                        + encoded(currentRefresh);
        long issued = System.nanoTime();
        JsonNode reply =
                http.request(
                                BasecampHttp.Route.REFRESH,
                                null,
                                "/authorization/token",
                                null,
                                form.getBytes(StandardCharsets.UTF_8),
                                deadline)
                        .json;
        String token = text(reply.get("access_token"));
        String type = text(reply.get("token_type"));
        JsonNode ttl = reply.get("expires_in");
        if (!BasecampConfig.bearer(token)
                || !"Bearer".equalsIgnoreCase(type)
                || ttl == null
                || !ttl.isIntegralNumber()
                || !ttl.canConvertToInt()
                || ttl.intValue() <= 0) {
            throw BasecampHttp.failure(null, "invalid OAuth token response");
        }
        JsonNode refresh = reply.get("refresh_token");
        if (refresh != null && !refresh.isNull()) {
            String replacement = text(refresh);
            if (!BasecampConfig.opaqueCredential(replacement)) {
                throw BasecampHttp.failure(null, "invalid replacement refresh token");
            }
            currentRefresh = replacement;
        }
        long duration = TimeUnit.SECONDS.toNanos(Math.min(ttl.intValue(), 1209600));
        expires = issued + duration - Math.min(TimeUnit.SECONDS.toNanos(60), duration / 10);
        access = token;
        return access;
    }

    private static String encoded(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String timestamp(BasecampResource resource, JsonNode row) {
        JsonNode value = row.get("updated_at");
        if (value == null || value.isNull()) {
            return null;
        }
        String raw = text(value);
        if (raw == null || raw.length() > 80) {
            throw resource.failure("invalid updated_at");
        }
        return raw;
    }

    private static Instant parseTime(BasecampResource resource, String raw) {
        try {
            return OffsetDateTime.parse(raw).toInstant();
        } catch (DateTimeException ex) {
            throw resource.failure("invalid updated_at");
        }
    }

    private static String text(JsonNode node) {
        return node != null && node.isTextual() ? node.textValue() : null;
    }

    /**
     * One provider row and the source columns needed to project or time-filter it.
     *
     * <p>When the row belongs to a child, project and schedule identify its provider ancestry.
     */
    static final class Row {
        final String id;
        final String project;
        final String schedule;
        final String updated;
        final Instant when;
        final JsonNode raw;

        Row(
                String id,
                String project,
                String schedule,
                String updated,
                Instant when,
                JsonNode raw) {
            this.id = id;
            this.project = project;
            this.schedule = schedule;
            this.updated = updated;
            this.when = when;
            this.raw = raw;
        }
    }

    /**
     * A validated provider collection page and its bounded resume position.
     *
     * <p>The previous update time tracks global descending feed order across pages.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final Instant tail;
        final String digest;

        Page(List<Row> rows, String next, Instant tail, String digest) {
            this.rows = rows;
            this.next = next;
            this.tail = tail;
            this.digest = digest;
        }
    }

    /**
     * One schedule dock and its enabled state for optional nested reads.
     *
     * <p>Disabled schedules still have a valid identity and may be emitted when selected.
     */
    static final class Schedule {
        final Row row;
        final boolean enabled;

        Schedule(Row row, boolean enabled) {
            this.row = row;
            this.enabled = enabled;
        }
    }

    /**
     * The schedule tools discovered from the anchored project detail.
     *
     * <p>The digest binds the original dock across checkpoint recovery.
     */
    static final class Dock {
        final List<Schedule> schedules;
        final String digest;

        Dock(List<Schedule> schedules, String digest) {
            this.schedules = schedules;
            this.digest = digest;
        }
    }
}
