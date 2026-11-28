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
package org.apache.seatunnel.connectors.seatunnel.canny;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.canny.CannyResource.Paging;
import org.apache.seatunnel.connectors.seatunnel.canny.CannySplit.Frame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Encodes read-only Canny requests and validates each resource's response shape.
 *
 * <p>The API key exists only in a POST JSON body. Paths and methods are fixed by the resource.
 */
final class CannyApi {
    private final CannyConfig config;
    private final CannyHttp http;

    CannyApi(CannyConfig config, CannyHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(Frame frame, long deadline) throws InterruptedException {
        CannyResource resource = frame.resource;
        int limit = resource.pageLimit(config, frame.page);
        JsonNode root = http.request(resource, payload(frame, limit), deadline);
        if (!root.isObject() || root.hasNonNull("error") || root.hasNonNull("errors")) {
            throw resource.failure("API did not return a list response");
        }
        JsonNode raw = root.path(resource.items);
        if (!raw.isArray()) {
            throw resource.failure("expected a " + resource.items + " array");
        }
        if (raw.size() > limit) {
            throw resource.failure("response exceeds the configured record count");
        }
        int nextPage = 0;
        String cursor = null;
        String history = null;
        if (resource.paging == Paging.CURSOR) {
            if (flag(resource, root, "hasNextPage")) {
                nextPage = frame.page + 1;
                cursor = text(resource, root.path("cursor"), "cursor");
                history = CannyConfig.hash(cursor);
            }
            // Canny may include a cursor on a terminal page. The flag takes precedence.
        } else if (resource.paging == Paging.OFFSET) {
            nextPage = flag(resource, root, "hasMore") ? frame.page + 1 : 0;
            if (resource == CannyResource.CHANGELOG_ENTRIES && frame.page == 2 && nextPage > 0) {
                throw resource.failure("changelog offset exceeds the Canny limit of 1000");
            }
            if (!raw.isEmpty()) {
                history = CannyConfig.hash(raw.toString());
            }
        } else if (root.hasNonNull("hasMore")
                || root.hasNonNull("hasNextPage")
                || root.hasNonNull("cursor")) {
            throw resource.failure("unexpected pagination on the complete board list");
        }
        if (history != null && frame.seen.contains(history)) {
            throw resource.failure(
                    resource.paging == Paging.CURSOR ? "repeated cursor" : "repeated offset page");
        }
        List<Row> rows = new ArrayList<>(raw.size());
        Set<String> boardIds = new HashSet<>();
        for (JsonNode item : raw) {
            http.remaining(resource, deadline);
            Row row = row(frame, item);
            if (resource == CannyResource.BOARDS && !boardIds.add(row.id)) {
                throw resource.failure("duplicate board ID");
            }
            rows.add(row);
        }
        String digest;
        if (resource == CannyResource.BOARDS) {
            // The board endpoint promises no order. Keep checkpoint positions deterministic.
            rows.sort(Comparator.comparing(row -> row.id));
            StringBuilder snapshot = new StringBuilder();
            rows.forEach(row -> snapshot.append(row.record).append('\n'));
            digest = CannyConfig.hash(snapshot.toString());
        } else {
            digest = CannyConfig.hash(root.toString());
        }
        http.remaining(resource, deadline);
        return new Page(rows, nextPage, cursor, history, digest);
    }

    private byte[] payload(Frame frame, int limit) {
        CannyResource resource = frame.resource;
        ObjectNode body = JsonNodeFactory.instance.objectNode().put("apiKey", config.token);
        if (resource.paging != Paging.COMPLETE) {
            body.put("limit", limit);
        }
        if (resource.paging == Paging.OFFSET) {
            int step =
                    resource == CannyResource.CHANGELOG_ENTRIES
                            ? CannyResource.CHANGELOG_MAX_SKIP
                            : config.pageSize;
            body.put("skip", (frame.page - 1) * step);
        } else if (frame.cursor != null) {
            body.put("cursor", frame.cursor);
        }
        if (frame.boardId != null) {
            body.put("boardID", frame.boardId);
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static Row row(Frame frame, JsonNode item) {
        CannyResource resource = frame.resource;
        if (!item.isObject()) {
            throw resource.failure("expected an object for a record");
        }
        String id = text(resource, item.path("id"), "id");
        String board = optionalId(resource, item.path("board"), "board");
        if (frame.boardId != null) {
            if (board != null && !frame.boardId.equals(board)) {
                throw resource.failure("category belongs to a different board");
            }
            board = frame.boardId;
        } else if (resource == CannyResource.BOARDS) {
            board = id;
        } else if (board == null) {
            board = optionalId(resource, item.path("post").path("board"), "post board");
        }
        String post =
                resource == CannyResource.POSTS
                        ? id
                        : optionalId(resource, item.path("post"), "post");
        return new Row(id, board, post, item.toString());
    }

    private static boolean flag(CannyResource resource, JsonNode root, String field) {
        JsonNode value = root.path(field);
        if (!value.isBoolean()) {
            throw resource.failure("invalid pagination " + field);
        }
        return value.booleanValue();
    }

    private static String optionalId(CannyResource resource, JsonNode item, String parent) {
        JsonNode value = item.path("id");
        return value.isMissingNode() || value.isNull()
                ? null
                : text(resource, value, parent + " id");
    }

    private static String text(CannyResource resource, JsonNode value, String field) {
        if (!value.isTextual() || !validOpaque(value.textValue())) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
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
     * Retains the rows and confirmed pagination of one API response.
     *
     * <p>The digest is separate from continuation history used to stop repetitions.
     */
    static final class Page {
        final List<Row> rows;
        final int next;
        final String cursor;
        final String history;
        final String digest;

        Page(List<Row> rows, int next, String cursor, String history, String digest) {
            this.rows = rows;
            this.next = next;
            this.cursor = cursor;
            this.history = history;
            this.digest = digest;
        }
    }

    /**
     * Projects stable identifiers while retaining the native JSON record.
     *
     * <p>Optional post and board associations are kept separately from the record key.
     */
    static final class Row {
        final String id;
        final String board;
        final String post;
        final String record;

        Row(String id, String board, String post, String record) {
            this.id = id;
            this.board = board;
            this.post = post;
            this.record = record;
        }
    }
}
