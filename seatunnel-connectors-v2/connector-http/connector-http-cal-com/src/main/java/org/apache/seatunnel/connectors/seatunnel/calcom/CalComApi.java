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
package org.apache.seatunnel.connectors.seatunnel.calcom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses the six Cal.com v2 response shapes and their native continuation.
 *
 * <p>The selected API versions fix both the JSON envelope and pagination protocol for a job.
 */
final class CalComApi {
    private final CalComConfig config;
    private final CalComHttp http;

    CalComApi(CalComConfig config, CalComHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(CalComResource resource, int offset, String cursor, long deadline)
            throws InterruptedException {
        JsonNode reply =
                http.request(
                        resource,
                        target(resource, offset, cursor),
                        config.version(resource),
                        deadline);
        JsonNode status = reply.get("status");
        if (!reply.isObject()
                || status == null
                || !status.isTextual()
                || !"success".equals(status.textValue())
                || reply.hasNonNull("error")
                || reply.hasNonNull("errors")) {
            throw resource.failure("API did not return a successful response");
        }
        JsonNode data = reply.path("data");
        List<Row> rows = new ArrayList<>();
        switch (resource) {
            case EVENT_TYPES:
                if (config.groupedEvents()) {
                    appendNested(resource, data, "eventTypeGroups", "eventTypes", rows, deadline);
                } else {
                    appendArray(resource, data, null, rows, deadline);
                }
                break;
            case CALENDARS:
                appendNested(resource, data, "connectedCalendars", "calendars", rows, deadline);
                break;
            case MY_PROFILE:
                rows.add(row(resource, data, null));
                break;
            default:
                appendArray(resource, data, null, rows, deadline);
        }
        boolean hasNext = false;
        String next = null;
        if (resource == CalComResource.BOOKINGS) {
            JsonNode pagination = reply.path("pagination");
            if (!pagination.isObject()) {
                throw resource.failure("missing bookings pagination");
            }
            if (config.cursorBookings()) {
                hasNext = flag(resource, pagination, "hasMore");
                next = readCursor(resource, pagination.get("nextCursor"), hasNext);
            } else {
                hasNext = validateOffset(resource, pagination, offset, rows.size());
            }
        }
        String digest = CalComConfig.hash(reply.toString());
        http.remaining(resource, deadline);
        return new Page(rows, hasNext, next, digest);
    }

    private String target(CalComResource resource, int offset, String cursor) {
        if (resource != CalComResource.BOOKINGS) {
            return resource.path;
        }
        if (config.cursorBookings()) {
            String path = resource.path + "?limit=" + config.pageSize;
            return cursor == null
                    ? path
                    : path
                            + "&cursor="
                            + URLEncoder.encode(cursor, StandardCharsets.UTF_8).replace("+", "%20");
        }
        return resource.path + "?skip=" + offset + "&take=" + config.pageSize;
    }

    private void appendNested(
            CalComResource resource,
            JsonNode data,
            String groupsField,
            String recordsField,
            List<Row> rows,
            long deadline)
            throws InterruptedException {
        JsonNode groups = data.path(groupsField);
        if (!data.isObject() || !groups.isArray() || groups.size() > config.maxItems) {
            throw resource.failure("invalid " + groupsField + " collection");
        }
        for (JsonNode group : groups) {
            http.remaining(resource, deadline);
            if (!group.isObject()) {
                throw resource.failure("expected an object in " + groupsField);
            }
            ObjectNode parent = JsonNodeFactory.instance.objectNode();
            group.fields()
                    .forEachRemaining(
                            field -> {
                                if (!recordsField.equals(field.getKey())) {
                                    parent.set(field.getKey(), field.getValue());
                                }
                            });
            appendArray(resource, group.get(recordsField), parent.toString(), rows, deadline);
        }
    }

    private void appendArray(
            CalComResource resource, JsonNode data, String parent, List<Row> rows, long deadline)
            throws InterruptedException {
        if (data == null || !data.isArray()) {
            throw resource.failure("expected an array of records");
        }
        if (data.size() > config.expectedMax(resource) - rows.size()) {
            throw resource.failure("response exceeds the configured record count");
        }
        for (JsonNode record : data) {
            http.remaining(resource, deadline);
            rows.add(row(resource, record, parent));
        }
    }

    private static Row row(CalComResource resource, JsonNode record, String parent) {
        if (!record.isObject()) {
            throw resource.failure("expected a record object");
        }
        JsonNode identity = record.path(resource.identity);
        String id;
        if (resource == CalComResource.CALENDARS) {
            if (!identity.isTextual() || !validOpaque(identity.textValue())) {
                throw resource.failure("missing or invalid " + resource.identity);
            }
            id = identity.textValue();
        } else {
            if (!identity.isIntegralNumber() || identity.bigIntegerValue().signum() < 1) {
                throw resource.failure("missing or invalid " + resource.identity);
            }
            id = identity.bigIntegerValue().toString();
        }
        if (resource == CalComResource.CONFERENCING) {
            // Credential key material is excluded from the conferencing list contract.
            ((ObjectNode) record)
                    .remove(List.of("key", "access_token", "refresh_token", "client_secret"));
        }
        return new Row(id, parent, record.toString());
    }

    private static String readCursor(CalComResource resource, JsonNode cursor, boolean hasNext) {
        if (!hasNext && (cursor == null || cursor.isNull())) {
            return null;
        }
        if (!hasNext || cursor == null || !cursor.isTextual() || !validOpaque(cursor.textValue())) {
            throw resource.failure("invalid bookings pagination nextCursor");
        }
        return cursor.textValue();
    }

    /**
     * Ensures a legacy offset request and its reply agree on the page boundaries.
     *
     * <p>The provider counts IDs before it hydrates rows. IDs that disappear between those
     * operations may leave a short or empty data array even when the next offset exists.
     */
    private boolean validateOffset(CalComResource resource, JsonNode meta, int offset, int rows) {
        long size = count(resource, meta, "itemsPerPage");
        long total = count(resource, meta, "totalItems");
        long returned = count(resource, meta, "returnedItems");
        long remaining = count(resource, meta, "remainingItems");
        long pages = count(resource, meta, "totalPages");
        long current = count(resource, meta, "currentPage");
        boolean hasNext = flag(resource, meta, "hasNextPage");
        boolean previous = flag(resource, meta, "hasPreviousPage");
        long acceptedOffset = Math.min(total, offset);
        long expectedPages = total == 0 ? 0 : 1 + (total - 1) / config.pageSize;
        if (size != config.pageSize
                || returned != Math.min(total - acceptedOffset, config.pageSize)
                || rows > returned
                || remaining != Math.max(0, total - acceptedOffset - config.pageSize)
                || pages != expectedPages
                || current != Math.min(expectedPages, 1 + acceptedOffset / config.pageSize)
                || hasNext != (remaining > 0)
                || previous != (acceptedOffset > 0)) {
            throw resource.failure("inconsistent offset pagination metadata");
        }
        return hasNext;
    }

    private static long count(CalComResource resource, JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw resource.failure("invalid bookings pagination " + field);
        }
        return value.longValue();
    }

    private static boolean flag(CalComResource resource, JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isBoolean()) {
            throw resource.failure("invalid bookings pagination " + field);
        }
        return value.booleanValue();
    }

    static boolean validOpaque(String value) {
        return !value.isBlank()
                && value.length() <= 4096
                && value.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    /**
     * Holds a complete decoded response and its validated continuation.
     *
     * <p>The digest covers both records and pagination fields or nested group context.
     */
    static final class Page {
        final List<Row> rows;
        final boolean hasNext;
        final String cursor;
        final String digest;

        Page(List<Row> rows, boolean hasNext, String cursor, String digest) {
            this.rows = rows;
            this.hasNext = hasNext;
            this.cursor = cursor;
            this.digest = digest;
        }
    }

    /**
     * Retains the original resource data and optional flattened parent context.
     *
     * <p>Both JSON values keep exact numeric precision and native field names.
     */
    static final class Row {
        final String id;
        final String parent;
        final String record;

        Row(String id, String parent, String record) {
            this.id = id;
            this.parent = parent;
            this.record = record;
        }
    }
}
