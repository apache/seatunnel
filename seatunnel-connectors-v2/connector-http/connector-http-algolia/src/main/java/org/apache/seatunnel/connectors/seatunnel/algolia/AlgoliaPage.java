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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Validates a response before any rows advance their index, query, or log traversal. */
final class AlgoliaPage {
    static final int QUERY_LIMIT = 20000;
    final List<Row> rows;
    final boolean more;
    final int pages;
    final int perPage;
    final long hits;
    final String digest;
    final String keysDigest;

    private AlgoliaPage(List<Row> rows, boolean more, int pages, int perPage, long hits) {
        this.rows = rows;
        this.more = more;
        this.pages = pages;
        this.perPage = perPage;
        this.hits = hits;
        String[] data = new String[rows.size() + 4];
        String[] keys = new String[rows.size()];
        data[0] = Boolean.toString(more);
        data[1] = Integer.toString(pages);
        data[2] = Integer.toString(perPage);
        data[3] = Long.toString(hits);
        for (int i = 0; i < rows.size(); i++) {
            data[i + 4] = rows.get(i).record;
            keys[i] = rows.get(i).id;
        }
        Arrays.sort(keys);
        keysDigest = keys.length == 0 ? null : hash(keys);
        digest = hash(data);
    }

    static AlgoliaPage read(
            AlgoliaConfig config,
            AlgoliaResource resource,
            String parent,
            AlgoliaSplit.Cursor cursor,
            JsonNode payload,
            AlgoliaHttpTransport transport,
            long deadline)
            throws InterruptedException {
        if (resource.array == null) {
            if (resource == AlgoliaResource.AVAILABLE_LANGUAGES) {
                for (JsonNode language : payload) {
                    transport.remaining(resource, deadline);
                    if (!language.isObject()) {
                        throw resource.failure("invalid language map");
                    }
                }
            }
            if (resource == AlgoliaResource.INDEXES_SETTINGS
                    && payload.hasNonNull("message")
                    && payload.size() == 1) {
                throw resource.failure("invalid settings object");
            }
            Row singleton =
                    new Row(parent == null ? "languages" : parent, null, payload.toString());
            return new AlgoliaPage(Collections.singletonList(singleton), false, -1, -1, -1);
        }

        JsonNode array = payload.path(resource.array);
        if (!array.isArray() || array.size() > 1000) {
            throw resource.failure("invalid collection response");
        }
        List<Row> rows = new ArrayList<>(array.size());
        Set<String> identities = new HashSet<>();
        Instant previous = cursor.lastTimestamp;
        for (JsonNode object : array) {
            transport.remaining(resource, deadline);
            if (!object.isObject()) {
                throw resource.failure("collection contains a non-object record");
            }
            JsonNode key = object.path(resource.key);
            String id = key.asText("");
            if (!key.isTextual()
                    || id.isBlank()
                    || id.length() > 512
                    || id.codePoints().anyMatch(Character::isISOControl)) {
                throw resource.failure("record has an invalid identity");
            }
            if (!identities.add(id)
                    || cursor.seenKeys != null && cursor.seenKeys.contains(hash(id))) {
                throw resource.failure("pagination repeated a record identity");
            }
            Instant time = null;
            if (resource == AlgoliaResource.INDICES
                    && config.selection.contains(AlgoliaResource.INDICES)) {
                time = time(resource, object.path("updatedAt"));
            } else if (resource == AlgoliaResource.LOGS) {
                time = time(resource, object.path("timestamp"));
                if (previous != null && time.isAfter(previous)) {
                    throw resource.failure("log order changed during pagination");
                }
                previous = time;
            }
            rows.add(new Row(id, time, object.toString()));
        }
        if (resource == AlgoliaResource.LOGS) {
            int requested = Math.min(config.logsSize, AlgoliaResource.LOG_HORIZON - cursor.offset);
            if (rows.size() > requested) {
                throw resource.failure("log response exceeds its requested length");
            }
            boolean more =
                    rows.size() == requested
                            && previous != null
                            && !previous.isBefore(config.start)
                            && cursor.offset + rows.size() < AlgoliaResource.LOG_HORIZON;
            if (more && cursor.page + 1 >= config.maxPages) {
                throw resource.failure("max_pages reached with more log entries in the window");
            }
            return new AlgoliaPage(rows, more, -1, requested, -1);
        }
        if (resource == AlgoliaResource.INDEXES_QUERY) {
            return query(config, resource, cursor, payload, rows);
        }

        int pages = integer(resource, payload.path("nbPages"), "nbPages");
        JsonNode echo = payload.get("page");
        if (rows.size() > config.indicesSize
                || pages == 0 && (cursor.page != 0 || !rows.isEmpty())
                || pages > 0 && cursor.page >= pages
                || echo != null && integer(resource, echo, "page") != cursor.page
                || cursor.expectedPages >= 0 && cursor.expectedPages != pages) {
            throw resource.failure("index pagination metadata changed or contradicts the page");
        }
        if (pages > config.maxPages) {
            throw resource.failure("index enumeration exceeds max_pages");
        }
        AlgoliaPage page =
                new AlgoliaPage(rows, cursor.page + 1 < pages, pages, config.indicesSize, -1);
        if (page.keysDigest != null && page.keysDigest.equals(cursor.previousKeys)) {
            throw resource.failure("repeated index page");
        }
        return page;
    }

    private static AlgoliaPage query(
            AlgoliaConfig config,
            AlgoliaResource resource,
            AlgoliaSplit.Cursor cursor,
            JsonNode payload,
            List<Row> rows) {
        AlgoliaQuery request = config.query;
        if (request.offset != null) {
            Integer length = request.length == null ? request.hitsPerPage : request.length;
            if (integer(resource, payload.path("offset"), "offset") != request.offset
                    || length != null && rows.size() > length
                    || length != null
                            && payload.has("length")
                            && integer(resource, payload.path("length"), "length") != length) {
                throw resource.failure("search offset response exceeds or differs from its window");
            }
            return new AlgoliaPage(rows, false, -1, -1, -1);
        }
        int pages = integer(resource, payload.path("nbPages"), "nbPages");
        int size = integer(resource, payload.path("hitsPerPage"), "hitsPerPage");
        int echo = integer(resource, payload.path("page"), "page");
        long count = nonnegative(resource, payload.path("nbHits"), "nbHits");
        int expected = request.allPages() ? cursor.page : request.page;
        if (echo != expected
                || size < 1
                || size > 1000
                || rows.size() > size
                || request.hitsPerPage != null && request.hitsPerPage != size) {
            throw resource.failure("search pagination metadata differs from the request");
        }
        if (!request.allPages()) {
            return new AlgoliaPage(rows, false, pages, size, count);
        }

        JsonNode approximate = payload.path("exhaustive").path("nbHits");
        JsonNode legacyApproximate = payload.path("exhaustiveNbHits");
        if (approximate.isBoolean() && !approximate.booleanValue()
                || approximate.isMissingNode()
                        && legacyApproximate.isBoolean()
                        && !legacyApproximate.booleanValue()) {
            throw resource.failure("search hit count is approximate for a complete scan");
        }
        long expectedOnPage = Math.min(size, Math.max(0, count - (long) echo * size));
        long requiredPages = count == 0 ? 0 : (count - 1) / size + 1;
        if (count > QUERY_LIMIT || requiredPages > pages || expectedOnPage > rows.size()) {
            throw resource.failure("search results exceed accessible pagination or were truncated");
        }
        if (count > 0 && pages != requiredPages
                || count == 0 && pages > 1
                || rows.size() != expectedOnPage
                || count > 0 && echo >= pages
                || cursor.expectedPages >= 0
                        && (cursor.expectedPages != pages
                                || cursor.hitsPerPage != size
                                || cursor.expectedHits != count)) {
            throw resource.failure("search pagination metadata changed or contradicts the page");
        }
        if (pages > config.maxPages) {
            throw resource.failure("search exceeds max_pages");
        }
        return new AlgoliaPage(rows, echo + 1 < pages, pages, size, count);
    }

    private static int integer(AlgoliaResource resource, JsonNode value, String field) {
        long number = nonnegative(resource, value, field);
        if (number > Integer.MAX_VALUE) {
            throw resource.failure("invalid " + field + " metadata");
        }
        return (int) number;
    }

    private static long nonnegative(AlgoliaResource resource, JsonNode value, String field) {
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw resource.failure("invalid " + field + " metadata");
        }
        return value.longValue();
    }

    private static Instant time(AlgoliaResource resource, JsonNode value) {
        try {
            if (value.isTextual()) {
                return Instant.parse(value.textValue());
            }
        } catch (DateTimeParseException ignored) {
            // Leave timestamp values out of diagnostics.
        }
        throw resource.failure("missing or invalid provider timestamp");
    }

    static boolean isHash(String value) {
        return value != null && value.matches("[A-Za-z0-9_-]{43}");
    }

    static String hash(String... fields) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Retains the provider JSON together with the stable identity and optional filter timestamp.
     */
    static final class Row {
        final String id;
        final Instant timestamp;
        final String record;

        private Row(String id, Instant timestamp, String record) {
            this.id = id;
            this.timestamp = timestamp;
            this.record = record;
        }
    }
}
