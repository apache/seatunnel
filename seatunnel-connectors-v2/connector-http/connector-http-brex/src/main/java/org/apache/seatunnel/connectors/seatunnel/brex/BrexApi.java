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
package org.apache.seatunnel.connectors.seatunnel.brex;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps the six Brex JSON read endpoints to a common page envelope.
 *
 * <p>Provider cursors are opaque query values. The client constructs every request from a fixed
 * resource path and never interprets a cursor as a URI.
 */
final class BrexApi {
    private final BrexConfig config;
    private final BrexHttp http;

    BrexApi(BrexConfig config, BrexHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(BrexResource resource, String cursor, Instant end, long deadline)
            throws InterruptedException {
        int limit = config.limit(resource);
        JsonNode response = http.request(resource, target(resource, cursor, end, limit), deadline);
        JsonNode items = response.get("items");
        if (!response.isObject() || items == null || !items.isArray()) {
            throw resource.failure("expected an items array");
        }
        if (items.size() > limit) {
            throw resource.failure("item count exceeds the requested page size");
        }
        JsonNode continuation = response.get("next_cursor");
        String next = null;
        if (continuation != null && !continuation.isNull()) {
            if (!continuation.isTextual() || !validCursor(continuation.textValue())) {
                throw resource.failure("invalid next_cursor");
            }
            next = continuation.textValue();
        }
        List<Row> rows = new ArrayList<>(items.size());
        for (JsonNode item : items) {
            http.remaining(resource, deadline);
            Row row = row(resource, item, end);
            if (row != null) {
                rows.add(row);
            }
        }
        String digest = BrexConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, next, digest);
    }

    private String target(BrexResource resource, String cursor, Instant end, int limit) {
        StringBuilder query = new StringBuilder(resource.path).append("?limit=").append(limit);
        if (cursor != null) {
            parameter(query, "cursor", cursor);
        }
        if (resource == BrexResource.TRANSACTIONS) {
            parameter(query, "posted_at_start", config.transactionStart.toString());
        } else if (resource == BrexResource.EXPENSES) {
            parameter(query, "expense_type[]", "CARD");
            parameter(query, "purchased_at_start", config.expenseStart.toString());
            parameter(query, "purchased_at_end", end.toString());
        } else if (resource == BrexResource.USERS) {
            config.userStatuses.forEach(status -> parameter(query, "status[]", status));
        }
        return query.toString();
    }

    private static void parameter(StringBuilder query, String name, String value) {
        query.append('&')
                .append(URLEncoder.encode(name, StandardCharsets.UTF_8))
                .append('=')
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20"));
    }

    private Row row(BrexResource resource, JsonNode item, Instant end) {
        if (!item.isObject()) {
            throw resource.failure("expected an object in items");
        }
        String identity = text(resource, item, resource.identityField);
        if (!validCursor(identity)) {
            throw resource.failure("invalid " + resource.identityField);
        }
        if (resource == BrexResource.TRANSACTIONS) {
            LocalDate day;
            try {
                day = BrexConfig.day(text(resource, item, "posted_at_date"));
            } catch (DateTimeException ex) {
                throw resource.failure("invalid posted_at_date");
            }
            if (day.isBefore(config.transactionStart.atOffset(ZoneOffset.UTC).toLocalDate())
                    || day.isAfter(end.atOffset(ZoneOffset.UTC).toLocalDate())) {
                return null;
            }
            return new Row(identity, day.toString(), item.toString());
        }
        if (resource == BrexResource.EXPENSES) {
            JsonNode expenseType = item.get("expense_type");
            if (expenseType != null
                    && !expenseType.isNull()
                    && (!expenseType.isTextual() || !"CARD".equals(expenseType.textValue()))) {
                throw resource.failure("returned expense does not match the card filter");
            }
            Instant purchased;
            try {
                purchased = BrexConfig.instant(text(resource, item, "purchased_at"));
            } catch (DateTimeException ex) {
                throw resource.failure("invalid purchased_at");
            }
            if (purchased.isBefore(config.expenseStart) || purchased.isAfter(end)) {
                return null;
            }
            return new Row(identity, purchased.toString(), item.toString());
        }
        return new Row(identity, null, item.toString());
    }

    private static String text(BrexResource resource, JsonNode item, String field) {
        JsonNode value = item.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    static boolean validCursor(String cursor) {
        return !cursor.isBlank()
                && cursor.length() <= 4096
                && cursor.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    /**
     * Holds one complete response with stable row and continuation boundaries.
     *
     * <p>A short or empty response can still have a continuation cursor.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final String digest;

        Page(List<Row> rows, String next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Retains one record and its identity without remapping monetary fields.
     *
     * <p>The cursor is distinct from the original posted or purchased value in the payload.
     */
    static final class Row {
        final String id;
        final String cursor;
        final String json;

        Row(String id, String cursor, String json) {
            this.id = id;
            this.cursor = cursor;
            this.json = json;
        }
    }
}
