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
package org.apache.seatunnel.connectors.seatunnel.avni;

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
 * Reads bounded data pages and renews their Cognito authentication.
 *
 * <p>Discovery and login have independent cache lifetimes and the token exists only in the network
 * worker that owns this client.
 */
final class AvniApi {
    private static final DateTimeFormatter MILLIS =
            new DateTimeFormatterBuilder().appendInstant(3).toFormatter();
    private static final DateTimeFormatter COMPACT_OFFSET =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .appendOffset("+HHMM", "Z")
                    .toFormatter();
    // Expiry is optional in Cognito's response. Four minutes stay below its five-minute minimum.
    private static final int FALLBACK_TOKEN_SECONDS = 240;

    private final AvniConfig config;
    private final AvniHttp http;
    private String clientId;
    private long clientExpiry;
    private String cachedToken;
    private long tokenExpiry;

    AvniApi(AvniConfig config, AvniHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Validates Avni's page metadata and audit ordering before a row reaches the collector.
     *
     * <p>Avni labels the number of rows in this response totalElements. Its collection-wide
     * continuation count is totalPages and the effective page size is pageSize.
     */
    Page page(AvniResource resource, AvniSplit split, long deadline) throws InterruptedException {
        String target =
                resource.path
                        + "?page="
                        + split.page
                        + "&size="
                        + config.pageSize
                        + "&lastModifiedDateTime="
                        + encodeDate(split.start)
                        + "&now="
                        + encodeDate(split.end);
        JsonNode response = authorizedRead(resource, target, deadline);
        JsonNode records = response.get("content");
        int size = integer(resource, response, "pageSize");
        int pages = integer(resource, response, "totalPages");
        int count = integer(resource, response, "totalElements");
        if (size < 1
                || size > config.pageSize
                || records == null
                || !records.isArray()
                || count != records.size()
                || count > size
                || (pages == 0
                        ? split.page != 0 || count != 0
                        : split.page >= pages
                                || count == 0
                                || split.page + 1 < pages && count != size)) {
            throw resource.failure("inconsistent page metadata");
        }
        if (pages > config.maxPages) {
            throw resource.failure("exceeded max_pages_per_resource");
        }
        List<Row> rows = new ArrayList<>(count);
        Set<String> seen = new HashSet<>();
        Instant previous = split.previousTime;
        for (JsonNode record : records) {
            String id = record.isObject() ? AvniResource.text(record.get("ID"), 1024) : null;
            if (id == null || !seen.add(id) || split.previousIds.contains(AvniConfig.hash(id))) {
                throw resource.failure("invalid or repeated record ID");
            }
            Instant modified = auditTime(resource, record);
            if (!modified.isAfter(split.start) || !modified.isBefore(split.end)) {
                throw resource.failure("audit timestamp outside the requested exclusive interval");
            }
            if (previous != null && modified.isBefore(previous)) {
                throw resource.failure("audit timestamps are not ordered");
            }
            previous = modified;
            rows.add(new Row(id, MILLIS.format(modified), record));
        }
        return new Page(
                rows,
                pages,
                size,
                previous,
                AvniConfig.hash(pages + ":" + size + ":" + records.toString()));
    }

    private JsonNode authorizedRead(AvniResource resource, String target, long deadline)
            throws InterruptedException {
        try {
            return http.request(
                    AvniHttp.Route.DATA, resource, target, token(deadline), null, deadline);
        } catch (AvniHttp.Unauthorized first) {
            cachedToken = null;
            try {
                return http.request(
                        AvniHttp.Route.DATA, resource, target, token(deadline), null, deadline);
            } catch (AvniHttp.Unauthorized second) {
                cachedToken = null;
                throw resource.failure("HTTP 401 after Cognito token renewal");
            }
        }
    }

    /**
     * Reuses a login while its safe lifetime is open and requests a new ID token otherwise.
     *
     * <p>A Cognito challenge is an explicit job failure. It must be resolved on the account before
     * opening an unattended data session.
     */
    private String token(long deadline) throws InterruptedException {
        if (cachedToken != null && System.nanoTime() - tokenExpiry < 0) {
            return cachedToken;
        }
        ObjectNode login =
                http.json
                        .createObjectNode()
                        .put("AuthFlow", "USER_PASSWORD_AUTH")
                        .put("ClientId", clientId(deadline));
        login.putObject("AuthParameters")
                .put("USERNAME", config.username)
                .put("PASSWORD", config.password);
        byte[] payload;
        try {
            payload = http.json.writeValueAsBytes(login);
        } catch (JsonProcessingException ex) {
            throw AvniHttp.failure(null, "cannot encode Cognito sign-in");
        }
        long issued = System.nanoTime();
        JsonNode reply = http.request(AvniHttp.Route.SIGN_IN, null, "/", null, payload, deadline);
        JsonNode challenge = reply.get("ChallengeName");
        if (challenge != null && !challenge.isNull()) {
            throw AvniHttp.failure(null, "Cognito requires an additional sign-in challenge");
        }
        JsonNode authentication = reply.path("AuthenticationResult");
        String id = AvniResource.text(authentication.get("IdToken"), 8192);
        if (id == null || !id.matches("[A-Za-z0-9_=.\\-]+")) {
            throw AvniHttp.failure(null, "Cognito sign-in did not return a valid ID token");
        }
        int ttl = FALLBACK_TOKEN_SECONDS;
        JsonNode expires = authentication.get("ExpiresIn");
        if (expires != null && !expires.isNull()) {
            if (!expires.isIntegralNumber()
                    || !expires.canConvertToInt()
                    || expires.intValue() <= 0) {
                throw AvniHttp.failure(null, "invalid Cognito token expiry");
            }
            ttl = Math.min(expires.intValue(), 86400);
        }
        long duration = TimeUnit.SECONDS.toNanos(ttl);
        tokenExpiry = issued + duration - Math.min(TimeUnit.SECONDS.toNanos(30), duration / 10);
        cachedToken = id;
        return cachedToken;
    }

    private String clientId(long deadline) throws InterruptedException {
        if (clientId == null || System.nanoTime() - clientExpiry >= 0) {
            JsonNode discovery =
                    http.request(
                            AvniHttp.Route.DISCOVERY, null, "/idp-details", null, null, deadline);
            String id = AvniResource.text(discovery.path("cognito").get("clientId"), 128);
            if (id == null || !id.matches("[A-Za-z0-9_+]+")) {
                throw AvniHttp.failure(
                        null, "identity discovery did not return a valid Cognito client ID");
            }
            clientId = id;
            clientExpiry = System.nanoTime() + TimeUnit.DAYS.toNanos(1);
        }
        return clientId;
    }

    private static Instant auditTime(AvniResource resource, JsonNode record) {
        String value = AvniResource.text(record.path("audit").get("Last modified at"), 80);
        if (value != null) {
            try {
                Instant parsed;
                try {
                    parsed = OffsetDateTime.parse(value).toInstant();
                } catch (DateTimeException first) {
                    parsed = OffsetDateTime.parse(value, COMPACT_OFFSET).toInstant();
                }
                if (parsed.getNano() % 1000000 == 0) {
                    return parsed;
                }
            } catch (DateTimeException ex) {
                // Raw audit data is not safe to include in a failure message.
            }
        }
        throw resource.failure("invalid Last modified at audit timestamp");
    }

    private static String encodeDate(Instant instant) {
        return URLEncoder.encode(MILLIS.format(instant), StandardCharsets.UTF_8);
    }

    private static int integer(AvniResource resource, JsonNode object, String field) {
        JsonNode value = object.get(field);
        if (value == null
                || !value.isIntegralNumber()
                || !value.canConvertToInt()
                || value.intValue() < 0) {
            throw resource.failure("invalid page " + field);
        }
        return value.intValue();
    }

    /**
     * An independent provider record and its projected identity and timestamp.
     *
     * <p>Raw records remain in memory only until their page is consumed.
     */
    static final class Row {
        final String id;
        final String timestamp;
        final JsonNode record;

        Row(String id, String timestamp, JsonNode record) {
            this.id = id;
            this.timestamp = timestamp;
            this.record = record;
        }
    }

    /**
     * A response page with checked provider continuation metadata.
     *
     * <p>The digest includes the collection's page count and effective page size for restore.
     */
    static final class Page {
        final List<Row> rows;
        final int totalPages;
        final int pageSize;
        final Instant tail;
        final String digest;

        Page(List<Row> rows, int totalPages, int pageSize, Instant tail, String digest) {
            this.rows = rows;
            this.totalPages = totalPages;
            this.pageSize = pageSize;
            this.tail = tail;
            this.digest = digest;
        }
    }
}
