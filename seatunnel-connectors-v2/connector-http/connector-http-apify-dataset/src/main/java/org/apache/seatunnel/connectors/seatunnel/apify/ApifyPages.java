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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.apify.ApifyDatasetConfig.Resource;

import org.apache.http.Header;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Validates response envelopes and keeps raw scan positions separate from emitted record counts. A
 * page response is normalized once before its rows become visible to the engine.
 */
final class ApifyPages {
    private static final String HEADER_PREFIX = "X-Apify-Pagination-";
    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);

    private ApifyPages() {}

    /**
     * Validates the full page and establishes or checks the first page's scan boundary. An append
     * can increase provider total, while the job remains bounded by its first page.
     */
    static Page parse(
            Resource resource,
            HttpResponse response,
            JsonNode root,
            String datasetId,
            long offset,
            int limit,
            long fixedEnd)
            throws IOException {
        if (root == null) {
            throw invalid("expected a JSON response");
        }
        if (resource == Resource.DATASET) {
            JsonNode data = root.path("data");
            Row row = metadata(data);
            return new Page(1, 1, Collections.singletonList(row));
        }

        JsonNode records;
        long total;
        long responseOffset;
        long responseLimit;
        long scanned;
        if (resource == Resource.DATASET_COLLECTION) {
            JsonNode data = root.path("data");
            records = data.path("items");
            total = nonnegative(data.path("total"), "dataset collection total");
            responseOffset = nonnegative(data.path("offset"), "dataset collection offset");
            responseLimit = nonnegative(data.path("limit"), "dataset collection limit");
            scanned = nonnegative(data.path("count"), "dataset collection count");
            if (!data.path("desc").isBoolean() || data.path("desc").booleanValue()) {
                throw invalid("unexpected dataset collection sort order");
            }
            verifyHeader(response, "Total", total);
            verifyHeader(response, "Offset", responseOffset);
            verifyHeader(response, "Limit", responseLimit);
            verifyHeader(response, "Count", scanned);
        } else {
            records = root;
            total = parseInteger(header(response, "Total"), "pagination total");
            responseOffset = parseInteger(header(response, "Offset"), "pagination offset");
            responseLimit = parseInteger(header(response, "Limit"), "pagination limit");
            scanned = parseInteger(header(response, "Count"), "pagination count");
        }
        String desc = header(response, "Desc");
        if (desc != null && !"false".equalsIgnoreCase(desc) && !"0".equals(desc)) {
            throw invalid("unexpected pagination sort order");
        }
        if (!records.isArray()
                || records.size() > scanned
                || (resource == Resource.DATASET_COLLECTION && scanned != records.size())) {
            throw invalid("pagination count disagrees with the response records");
        }
        if (responseOffset != offset || responseLimit < 1 || responseLimit > limit) {
            throw invalid("unexpected pagination offset or limit");
        }
        long end = fixedEnd < 0 ? total : fixedEnd;
        if (total < end) {
            throw invalid("resource became shorter during the bounded read");
        }
        if (offset > end || scanned > responseLimit || scanned > end - offset) {
            throw invalid("pagination count exceeds the bounded scan");
        }
        if (scanned == 0 && offset < end) {
            throw invalid("pagination made no progress before the scan boundary");
        }

        List<Row> rows = new ArrayList<>(records.size());
        for (JsonNode record : records) {
            if (!record.isObject()) {
                throw invalid("expected an object for each dataset record");
            }
            if (resource == Resource.DATASET_COLLECTION) {
                rows.add(metadata(record));
            } else {
                JsonNode value = canonical(record);
                if (resource == Resource.ITEM_COLLECTION) {
                    value = NODES.objectNode().set("data", value);
                }
                rows.add(new Row(datasetId, value.toString()));
            }
        }
        return new Page(end, offset + scanned, rows);
    }

    private static Row metadata(JsonNode record) throws IOException {
        JsonNode id = record.path("id");
        if (!record.isObject() || !id.isTextual() || id.textValue().isEmpty()) {
            throw invalid("expected dataset metadata with an id");
        }
        ObjectNode safe = NODES.objectNode().setAll((ObjectNode) record);
        // These top-level fields grant dataset access. A business item can use either name.
        safe.remove("urlSigningSecretKey");
        safe.remove("itemsPublicUrl");
        return new Row(id.textValue(), canonical(safe).toString());
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, JsonNode> ordered = new TreeMap<>();
            node.fields().forEachRemaining(field -> ordered.put(field.getKey(), field.getValue()));
            ObjectNode copy = NODES.objectNode();
            ordered.forEach((key, value) -> copy.set(key, canonical(value)));
            return copy;
        }
        if (node.isArray()) {
            ArrayNode copy = NODES.arrayNode();
            node.forEach(value -> copy.add(canonical(value)));
            return copy;
        }
        return node;
    }

    private static void verifyHeader(HttpResponse response, String key, long expected)
            throws IOException {
        String value = header(response, key);
        if (value != null && parseInteger(value, "pagination " + key) != expected) {
            throw invalid("pagination headers disagree with dataset collection metadata");
        }
    }

    private static String header(HttpResponse response, String key) throws IOException {
        Header[] values = response.getHeaders(HEADER_PREFIX + key);
        if (values.length > 1) {
            throw invalid("duplicate pagination header");
        }
        return values.length == 0 ? null : values[0].getValue();
    }

    private static long nonnegative(JsonNode node, String name) throws IOException {
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 0) {
            throw invalid("invalid " + name);
        }
        return node.longValue();
    }

    private static long parseInteger(String value, String name) throws IOException {
        try {
            if (value != null
                    && !value.isEmpty()
                    && value.chars().allMatch(c -> c >= '0' && c <= '9')) {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException ignored) {
            // Reject numeric overflow without logging response values.
        }
        throw invalid("invalid " + name);
    }

    private static IOException invalid(String reason) {
        return new IOException(reason);
    }

    /**
     * Carries one validated page, its underlying next offset, and the immutable job boundary. The
     * digest identifies the ordered emitted rows and the consumed raw range for restore.
     */
    static final class Page {
        final long end;
        final long nextOffset;
        final List<Row> rows;
        final String digest;

        Page(long end, long nextOffset, List<Row> rows) {
            this.end = end;
            this.nextOffset = nextOffset;
            this.rows = rows;
            MessageDigest identity = ApifyDatasetConfig.digest();
            identity.update(ByteBuffer.allocate(Long.BYTES).putLong(nextOffset).array());
            for (Row row : rows) {
                update(identity, row.datasetId);
                update(identity, row.payload);
            }
            digest = ApifyDatasetConfig.digestId(identity.digest());
        }

        private static void update(MessageDigest identity, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            identity.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            identity.update(bytes);
        }
    }

    /**
     * Represents one provider record in the connector's fixed table schema. Payload is a complete,
     * resource-specific JSON object bounded by its page size.
     */
    static final class Row {
        final String datasetId;
        final String payload;

        Row(String datasetId, String payload) {
            this.datasetId = datasetId;
            this.payload = payload;
        }
    }
}
