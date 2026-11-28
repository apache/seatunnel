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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsConfig.digest;

/** Wire routes, envelope shapes and incremental fields for the eight 100ms resources. */
enum OneHundredMsResource {
    SESSIONS("/v2/sessions", "active", null, null, null),
    ROOMS("/v2/rooms", "enabled", "updated_at", "created_at", null),
    ACTIVE_ROOM_PEERS("/v2/active-rooms/%s/peers", null, null, null, ROOMS),
    TEMPLATES("/v2/templates", null, "updatedAt", "createdAt", null),
    TEMPLATE_SETTINGS("/v2/templates/%s/settings", null, null, null, TEMPLATES),
    TEMPLATES_DESTINATIONS("/v2/templates/%s/destinations", null, null, null, TEMPLATES),
    ANALYTICS_EVENTS("/v2/analytics/events", null, null, null, ROOMS),
    RECORDINGS("/v2/recordings", null, "updated_at", "created_at", null);

    private static final Instant UNSET_TIMESTAMP = Instant.parse("0001-01-01T00:00:00Z");
    private final String path;
    private final String statusFilter;
    private final String updatedField;
    private final String createdField;
    final OneHundredMsResource parent;

    OneHundredMsResource(
            String path,
            String statusFilter,
            String updatedField,
            String createdField,
            OneHundredMsResource parent) {
        this.path = path;
        this.statusFilter = statusFilter;
        this.updatedField = updatedField;
        this.createdField = createdField;
        this.parent = parent;
    }

    int phases() {
        return statusFilter == null ? 1 : 2;
    }

    String tableName() {
        return name().toLowerCase(Locale.ROOT);
    }

    String target(int phase, int pageSize, String parentId, String cursor) {
        if (this == TEMPLATE_SETTINGS
                || this == TEMPLATES_DESTINATIONS
                || this == ACTIVE_ROOM_PEERS) {
            return String.format(Locale.ROOT, path, encode(parentId));
        }
        StringBuilder uri = new StringBuilder(path).append("?limit=").append(pageSize);
        if (statusFilter != null) {
            uri.append('&').append(statusFilter).append('=').append(phase == 0);
        }
        if (this == ANALYTICS_EVENTS) {
            uri.append("&room_id=").append(encode(parentId));
        }
        if (cursor != null) {
            uri.append("&start=").append(encode(cursor));
        }
        return uri.toString();
    }

    Page page(JsonNode body, String parentId, int maxRecords, boolean discovery)
            throws IOException {
        if (body == null && this == ACTIVE_ROOM_PEERS) {
            return new Page(null, new ArrayList<>());
        }
        List<Value> values = new ArrayList<>();
        if (this == TEMPLATE_SETTINGS || this == TEMPLATES_DESTINATIONS) {
            // The documented destination response is the entire object of destination types.
            values.add(new Value(parentId, object(body), null));
            return new Page(null, values);
        }
        if (this == ACTIVE_ROOM_PEERS) {
            JsonNode peers = body.path("peers");
            if (peers.isArray() && peers.isEmpty()) {
                return new Page(null, values);
            }
            if (!peers.isObject() || peers.size() > maxRecords) {
                throw invalid("expected a bounded peer ID map");
            }
            Iterator<Map.Entry<String, JsonNode>> members = peers.fields();
            while (members.hasNext()) {
                Map.Entry<String, JsonNode> entry = members.next();
                String peerId = bounded(entry.getKey(), 512, "peer identifier");
                JsonNode row = object(entry.getValue());
                String id = optional(row.get("id"), 512, "peer identifier");
                if (id != null && !peerId.equals(id)) {
                    throw invalid("peer map key disagrees with the peer identifier");
                }
                values.add(new Value(peerId, row, null));
            }
            return new Page(null, values);
        }
        JsonNode rows = body.path(this == ANALYTICS_EVENTS ? "events" : "data");
        if (!rows.isArray() || rows.size() > maxRecords) {
            throw invalid("expected a bounded records array");
        }
        for (JsonNode node : rows) {
            JsonNode row = object(node);
            String id = optional(row.get("id"), 512, "record identifier");
            if (id == null && this == ANALYTICS_EVENTS) {
                id = optional(row.get("event_id"), 512, "event identifier");
                if (id == null) {
                    id = "sha256:" + digest(row.toString());
                }
            }
            if (id == null) {
                throw invalid("missing provider record identifier");
            }
            values.add(
                    new Value(id, row, discovery || updatedField == null ? null : updateTime(row)));
        }
        return new Page(
                optional(
                        body.get(this == ANALYTICS_EVENTS ? "next" : "last"),
                        2048,
                        "continuation cursor"),
                values);
    }

    private Instant updateTime(JsonNode row) throws IOException {
        Instant changed = timestamp(row.get(updatedField));
        if (changed == null || changed.equals(UNSET_TIMESTAMP)) {
            changed = timestamp(row.get(createdField));
        }
        return changed != null && changed.equals(UNSET_TIMESTAMP) ? null : changed;
    }

    private Instant timestamp(JsonNode node) throws IOException {
        String value = optional(node, 64, "record timestamp");
        try {
            return value == null ? null : Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw invalid("invalid provider record timestamp");
        }
    }

    private JsonNode object(JsonNode node) throws IOException {
        if (node == null || !node.isObject()) {
            throw invalid("expected a provider record object");
        }
        return node;
    }

    private String optional(JsonNode node, int max, String description) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isTextual()) {
            throw invalid("invalid " + description);
        }
        String value = node.textValue();
        return value.isEmpty() ? null : bounded(value, max, description);
    }

    private String bounded(String text, int max, String description) throws IOException {
        if (text.trim().isEmpty()
                || text.length() > max
                || text.chars().anyMatch(character -> character < 32 || character == 127)) {
            throw invalid("invalid " + description);
        }
        return text;
    }

    IOException invalid(String reason) {
        return new IOException("100ms source " + tableName() + ": " + reason);
    }

    private static String encode(String value) {
        StringBuilder encoded = new StringBuilder();
        String hex = "0123456789ABCDEF";
        for (byte item : value.getBytes(StandardCharsets.UTF_8)) {
            int ch = item & 255;
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '-'
                    || ch == '_'
                    || ch == '~') {
                encoded.append((char) ch);
            } else {
                // Escaping dots blocks dot-segment interpretation of provider IDs.
                encoded.append('%').append(hex.charAt(ch >>> 4)).append(hex.charAt(ch & 15));
            }
        }
        return encoded.toString();
    }

    /** A validated response, including a continuation independently of page length. */
    static final class Page {
        final String next;
        final List<Value> rows;

        Page(String next, List<Value> rows) {
            this.next = next;
            this.rows = rows;
        }

        String fingerprint() {
            StringBuilder identities = new StringBuilder();
            for (Value row : rows) {
                identities.append(row.id.length()).append(':').append(row.id);
            }
            return digest(identities.toString());
        }
    }

    /** Public identity and provider payload before the source applies its update window. */
    static final class Value {
        final String id;
        final JsonNode data;
        final Instant updated;

        Value(String id, JsonNode data, Instant updated) {
            this.id = id;
            this.data = data;
            this.updated = updated;
        }
    }
}
