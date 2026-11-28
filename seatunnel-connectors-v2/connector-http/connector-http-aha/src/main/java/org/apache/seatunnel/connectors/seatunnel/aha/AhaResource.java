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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/** Aha v1 paths, bounded page envelope validation, and explicit child-to-parent scope. */
enum AhaResource {
    FEATURES("/features", null),
    PRODUCTS("/products", null),
    IDEA_CATEGORIES("/products/%s/idea_categories", PRODUCTS),
    IDEAS("/ideas", null),
    IDEA_ENDORSEMENTS("/ideas/%s/endorsements", IDEAS),
    IDEA_COMMENTS("/ideas/%s/idea_comments", IDEAS),
    USERS("/users", null),
    GOALS("/goals", null);

    final AhaResource parent;
    private final String path;

    AhaResource(String path, AhaResource parent) {
        this.path = path;
        this.parent = parent;
    }

    String key() {
        return name().toLowerCase(Locale.ROOT);
    }

    String target(int page, int limit, String parentId) {
        String route = parent == null ? path : String.format(Locale.ROOT, path, encode(parentId));
        return "/api/v1" + route + "?page=" + page + "&per_page=" + limit;
    }

    /** Uses only numeric pagination facts. URI-shaped response fields never influence a request. */
    Page parse(JsonNode root, int pageSize) throws IOException {
        requireComplete(root.get("truncated"));
        JsonNode pagination = root.path("pagination");
        if (pagination.isArray() && pagination.size() == 1) {
            pagination = pagination.get(0);
        }
        if (!pagination.isObject()) {
            throw problem("expected pagination object or singleton array");
        }
        requireComplete(pagination.get("truncated"));
        JsonNode array = root.path(key());
        if (!array.isArray() || array.size() > pageSize) {
            throw problem("expected a bounded " + key() + " records array");
        }
        List<Record> records = new ArrayList<>(array.size());
        Set<String> ids = new TreeSet<>();
        StringBuilder fingerprint = new StringBuilder();
        for (JsonNode value : array) {
            if (!value.isObject()) {
                throw problem("list contains a non-object record");
            }
            JsonNode key = value.get("id");
            if (key == null || (!key.isIntegralNumber() && !key.isTextual())) {
                throw problem("record has no valid provider ID");
            }
            String id = key.asText();
            if (id.trim().isEmpty()
                    || id.length() > 512
                    || id.chars().anyMatch(ch -> ch < 32 || ch == 127)) {
                throw problem("record has no valid provider ID");
            }
            if (!ids.add(id)) {
                throw problem("page contains repeated record IDs");
            }
            records.add(new Record(id, value));
        }
        for (String id : ids) {
            fingerprint.append(id.length()).append(':').append(id);
        }
        return new Page(
                number(pagination.get("current_page")),
                number(pagination.get("total_pages")),
                number(pagination.get("total_records")),
                AhaConfig.digest(fingerprint.toString()),
                records);
    }

    private void requireComplete(JsonNode flag) throws IOException {
        if (flag != null && !flag.isNull() && (!flag.isBoolean() || flag.booleanValue())) {
            throw problem("provider reported a truncated or incomplete response");
        }
    }

    private int number(JsonNode value) throws IOException {
        if (value == null
                || !value.isIntegralNumber()
                || !value.canConvertToInt()
                || value.intValue() < 0) {
            throw problem("invalid numeric pagination metadata");
        }
        return value.intValue();
    }

    IOException problem(String detail) {
        return new IOException("Aha source " + key() + ": " + detail);
    }

    private static String encode(String input) {
        StringBuilder escaped = new StringBuilder();
        String hex = "0123456789ABCDEF";
        for (byte b : input.getBytes(StandardCharsets.UTF_8)) {
            int ch = b & 255;
            if ((ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '-'
                    || ch == '_'
                    || ch == '~') {
                escaped.append((char) ch);
            } else {
                escaped.append('%').append(hex.charAt(ch >>> 4)).append(hex.charAt(ch & 15));
            }
        }
        return escaped.toString();
    }

    /** Provider metadata and records from one fully validated HTTP response. */
    static final class Page {
        final int index;
        final int totalPages;
        final int totalRows;
        final String fingerprint;
        final List<Record> records;

        Page(int index, int totalPages, int totalRows, String fingerprint, List<Record> records) {
            this.index = index;
            this.totalPages = totalPages;
            this.totalRows = totalRows;
            this.fingerprint = fingerprint;
            this.records = records;
        }

        boolean last() {
            return index >= totalPages;
        }
    }

    /** Stable provider identity and unmodified JSON object that can be emitted after validation. */
    static final class Record {
        final String id;
        final JsonNode data;

        Record(String id, JsonNode data) {
            this.id = id;
            this.data = data;
        }
    }
}
