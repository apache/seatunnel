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

package org.apache.seatunnel.connectors.seatunnel.appcues;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Defines the date field and wire timestamp formats for each list resource. All supported endpoints
 * expose an unpaginated JSON array and use a local date filter.
 */
enum AppcuesResource {
    LAUNCHPADS("updated_at", TimestampEncoding.EPOCH_MILLIS),
    FLOWS("created_at", TimestampEncoding.ISO_OR_MILLIS),
    BANNERS("updated_at", TimestampEncoding.EPOCH_MILLIS),
    CHECKLISTS("created_at", TimestampEncoding.ISO_OR_MILLIS),
    PINS("updated_at", TimestampEncoding.EPOCH_MILLIS),
    TAGS("updated_at", TimestampEncoding.ISO),
    SEGMENTS("updated_at", TimestampEncoding.ISO);

    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);
    private static final Pattern MILLIS = Pattern.compile("-?[0-9]{1,19}");
    private final String field;
    private final TimestampEncoding encoding;

    AppcuesResource(String field, TimestampEncoding encoding) {
        this.field = field;
        this.encoding = encoding;
    }

    String key() {
        return name().toLowerCase(Locale.ROOT);
    }

    static List<String> names() {
        List<String> resources = new ArrayList<>();
        for (AppcuesResource resource : values()) {
            resources.add(resource.key());
        }
        return Collections.unmodifiableList(resources);
    }

    static AppcuesResource named(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw AppcuesConfig.invalid("resources contains an unsupported name");
        }
    }

    /**
     * Validates the complete API array and selects an exact, deterministic replay sequence.
     * Ordering by normalized time and ID makes equivalent object or list orderings safe to resume.
     */
    Snapshot snapshot(JsonNode root, Instant start, Instant end, int maxRecords)
            throws IOException {
        if (root == null || !root.isArray()) {
            throw new IOException("expected an unpaginated JSON array");
        }
        if (root.size() > maxRecords) {
            throw new IOException("response exceeds max_records");
        }
        List<Row> rows = new ArrayList<>();
        for (JsonNode record : root) {
            if (!record.isObject()) {
                throw new IOException("expected a JSON object for each record");
            }
            JsonNode timeNode = record.get(field);
            if (timeNode == null || timeNode.isNull()) {
                continue;
            }
            Instant time = timestamp(timeNode);
            if (time.isBefore(start) || !time.isBefore(end)) {
                continue;
            }
            JsonNode id = record.path("id");
            if (!id.isTextual() || id.textValue().isEmpty()) {
                throw new IOException("expected a record id");
            }
            rows.add(new Row(time, id.textValue(), canonical(record).toString()));
        }
        rows.sort(
                Comparator.comparing((Row row) -> row.time)
                        .thenComparing(row -> row.id)
                        .thenComparing(row -> row.payload));
        return new Snapshot(rows);
    }

    private Instant timestamp(JsonNode value) throws IOException {
        try {
            if (encoding != TimestampEncoding.ISO) {
                if (value.isIntegralNumber() && value.canConvertToLong()) {
                    return Instant.ofEpochMilli(value.longValue());
                }
                if (value.isTextual() && MILLIS.matcher(value.textValue()).matches()) {
                    return Instant.ofEpochMilli(Long.parseLong(value.textValue()));
                }
            }
            if (encoding != TimestampEncoding.EPOCH_MILLIS && value.isTextual()) {
                return Instant.parse(value.textValue());
            }
        } catch (DateTimeParseException | NumberFormatException ignored) {
            // Keep timestamp values out of failures because they are provider payload.
        }
        throw new IOException("invalid " + field + " timestamp");
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, JsonNode> sorted = new TreeMap<>();
            node.fields().forEachRemaining(entry -> sorted.put(entry.getKey(), entry.getValue()));
            ObjectNode copy = NODES.objectNode();
            sorted.forEach((key, value) -> copy.set(key, canonical(value)));
            return copy;
        }
        if (node.isArray()) {
            ArrayNode copy = NODES.arrayNode();
            node.forEach(value -> copy.add(canonical(value)));
            return copy;
        }
        return node;
    }

    /**
     * Separates true millisecond cursors from ISO-only and mixed timestamps. Each endpoint retains
     * the cursor field declared for that resource.
     */
    private enum TimestampEncoding {
        EPOCH_MILLIS,
        ISO_OR_MILLIS,
        ISO
    }

    /**
     * Contains the validated rows within the date bounds and their ordered replay fingerprint. The
     * digest is stored only when the reader has emitted part of this response.
     */
    static final class Snapshot {
        final List<Row> rows;
        final String digest;

        Snapshot(List<Row> rows) {
            this.rows = rows;
            MessageDigest id = AppcuesConfig.digest();
            for (Row row : rows) {
                byte[] bytes = row.payload.getBytes(StandardCharsets.UTF_8);
                id.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                id.update(bytes);
            }
            digest = AppcuesConfig.digestId(id.digest());
        }
    }

    /**
     * Couples the original record with the normalized field used for date filtering. The payload
     * preserves original JSON values, including the original timestamp representation.
     */
    static final class Row {
        final Instant time;
        final String id;
        final String payload;

        Row(Instant time, String id, String payload) {
            this.time = time;
            this.id = id;
            this.payload = payload;
        }
    }
}
