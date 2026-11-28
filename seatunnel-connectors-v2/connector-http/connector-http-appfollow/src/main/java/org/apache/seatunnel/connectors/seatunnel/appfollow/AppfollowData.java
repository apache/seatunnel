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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Decodes the five v2 response shapes and retains the collection-to-app relationships. The graph is
 * based on provider IDs while each emitted JSON record keeps its complete fields.
 */
final class AppfollowData {
    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);
    private static final Pattern COLLECTION_ID = Pattern.compile("[0-9]{1,40}");
    private static final String HEX = "0123456789ABCDEF";

    private AppfollowData() {}

    /**
     * Parses a complete response before publishing any rows or discovered IDs. Discovery is
     * independent of whether the parent resource's rows are selected.
     */
    static Batch parse(Request request, JsonNode root, AppfollowConfig config) throws IOException {
        Resource resource = request.resource;
        if (root == null || (!root.isArray() && !root.isObject()) || hasReportedError(root)) {
            throw new IOException("expected an AppFollow response object or array");
        }
        if (root.isObject() && continuation(root)) {
            throw new IOException("unexpected continuation for an unpaginated resource");
        }
        JsonNode records = resource.field == null ? root : root.path(resource.field);
        boolean isList = records.isArray();
        if (!isList && !records.isObject()) {
            throw new IOException("invalid " + resource.key() + " response shape");
        }
        if (isList && records.size() > config.maxRecords) {
            throw new IOException("response exceeds max_records");
        }
        Iterable<JsonNode> values = isList ? records : Collections.singletonList(records);
        List<Row> rows = new ArrayList<>();
        List<String> collections = new ArrayList<>();
        List<Binding> bindings = new ArrayList<>();
        boolean output = config.outputs.contains(resource);
        for (JsonNode record : values) {
            if (!record.isObject()) {
                throw new IOException("expected a JSON object for each record");
            }
            String collection = request.collection;
            String external = request.external;
            JsonNode payload = record;
            switch (resource) {
                case USERS:
                    if (!record.has("id") && !record.has("email") && !record.has("name")) {
                        throw new IOException("expected user details");
                    }
                    break;
                case APP_COLLECTIONS:
                    collection = collectionId(record.get("id"));
                    if (config.reads(Resource.APP_LISTS)) {
                        collections.add(collection);
                    }
                    break;
                case APP_LISTS:
                    external = externalId(record.path("app").get("ext_id"));
                    verifyAppContext(record, collection, external);
                    if (external == null && config.readsMetrics()) {
                        throw new IOException(
                                "application has no external ID for selected metrics");
                    }
                    if (config.readsMetrics()) {
                        bindings.add(new Binding(collection, external));
                    }
                    if (output) {
                        payload = addContext(record, collection, external);
                    }
                    break;
                case STAT_REVIEWS:
                    validateMetric(record, "reviews_stat", external);
                    break;
                case RATINGS:
                    validateMetric(record, "ratings", external);
                    JsonNode list = record.path("ratings").get("list");
                    if (list != null && !list.isNull() && !list.isArray()) {
                        throw new IOException("expected an array or null for ratings.list");
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported resource");
            }
            if (output) {
                rows.add(new Row(collection, external, canonical(payload).toString()));
            }
        }
        if (collections.size() > config.maxEntities || bindings.size() > config.maxEntities) {
            throw new IOException("discovery exceeds max_entities");
        }
        if (collections.size() > 1 && new HashSet<>(collections).size() != collections.size()) {
            throw new IOException("duplicate collection ID in discovery response");
        }
        return new Batch(rows, collections, bindings);
    }

    private static boolean hasReportedError(JsonNode root) {
        JsonNode error = root.get("error");
        return error != null
                && !error.isNull()
                && !(error.isBoolean() && !error.booleanValue())
                && !(error.isTextual() && error.textValue().isEmpty());
    }

    private static boolean continuation(JsonNode root) {
        return hasNext(root) || hasNext(root.path("pagination"));
    }

    private static boolean hasNext(JsonNode node) {
        for (String key : new String[] {"next", "next_page", "next_cursor", "next_page_token"}) {
            JsonNode value = node.get(key);
            if (value != null
                    && !value.isNull()
                    && !(value.isTextual() && value.textValue().isEmpty())
                    && !(value.isBoolean() && !value.booleanValue())) {
                return true;
            }
        }
        return node.path("has_more").asBoolean(false);
    }

    private static void validateMetric(JsonNode record, String field, String external)
            throws IOException {
        JsonNode data = record.get(field);
        if (data == null || (!data.isObject() && !data.isNull())) {
            throw new IOException("expected an object or null for " + field);
        }
        verifyExternal(record.get("ext_id"), external);
        verifyExternal(data.get("ext_id"), external);
    }

    private static void verifyExternal(JsonNode id, String expected) throws IOException {
        String reported = externalId(id);
        if (reported != null && !reported.equals(expected)) {
            throw new IOException("response external ID differs from the requested app");
        }
    }

    private static void verifyAppContext(JsonNode value, String collection, String external)
            throws IOException {
        JsonNode previousCollection = value.get("app_collection_id");
        if (previousCollection != null
                && !previousCollection.isNull()
                && !collection.equals(collectionId(previousCollection))) {
            throw new IOException("response collection ID differs from the requested collection");
        }
        verifyExternal(value.get("ext_id"), external);
    }

    private static JsonNode addContext(JsonNode value, String collection, String external) {
        ObjectNode augmented = NODES.objectNode().setAll((ObjectNode) value);
        augmented.put("app_collection_id", new BigInteger(collection));
        augmented.put("ext_id", external);
        return augmented;
    }

    private static String collectionId(JsonNode id) throws IOException {
        if (id == null
                || (!id.isIntegralNumber() && !id.isTextual())
                || !COLLECTION_ID.matcher(id.asText()).matches()) {
            throw new IOException("expected a numeric collection id");
        }
        return new BigInteger(id.asText()).toString();
    }

    private static String externalId(JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText();
        if ((!node.isTextual() && !node.isIntegralNumber())
                || value.isBlank()
                || value.length() > 256
                || value.codePoints()
                        .anyMatch(c -> Character.isISOControl(c) || (c >= 0xd800 && c <= 0xdfff))) {
            throw new IOException("invalid app external ID");
        }
        return value;
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, JsonNode> ordered = new TreeMap<>();
            node.fields().forEachRemaining(entry -> ordered.put(entry.getKey(), entry.getValue()));
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

    private static String encode(String id) {
        StringBuilder value = new StringBuilder();
        for (byte raw : id.getBytes(StandardCharsets.UTF_8)) {
            int ch = raw & 255;
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '-'
                    || ch == '.'
                    || ch == '_'
                    || ch == '~') {
                value.append((char) ch);
            } else {
                value.append('%').append(HEX.charAt(ch >>> 4)).append(HEX.charAt(ch & 15));
            }
        }
        return value.toString();
    }

    /**
     * Defines the fixed traversal order and documented v2 route for each selected resource.
     * Dependent routes receive only the ID query parameter defined by their endpoint.
     */
    enum Resource {
        USERS("/account/users", null, null),
        APP_COLLECTIONS("/account/apps", "apps", null),
        APP_LISTS("/account/apps/app", "apps_app", "apps_id"),
        STAT_REVIEWS("/reviews/stats", null, "ext_id"),
        RATINGS("/meta/ratings", null, "ext_id");

        private final String path;
        private final String field;
        private final String parameter;

        Resource(String path, String field, String parameter) {
            this.path = path;
            this.field = field;
            this.parameter = parameter;
        }

        String key() {
            return name().toLowerCase(Locale.ROOT);
        }

        static Resource named(String name) {
            try {
                return valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                throw AppfollowConfig.invalid("resources contains an unsupported name");
            }
        }

        static List<String> names() {
            List<String> resources = new ArrayList<>();
            for (Resource resource : values()) {
                resources.add(resource.key());
            }
            return Collections.unmodifiableList(resources);
        }
    }

    /**
     * Associates a request with its original collection and external app ID. Keeping the collection
     * in this value avoids cross-collection deduplication of the same app.
     */
    static final class Request {
        final Resource resource;
        final String collection;
        final String external;

        Request(Resource resource, String collection, String external) {
            this.resource = resource;
            this.collection = collection;
            this.external = external;
        }

        String target() {
            String base = "/api/v2" + resource.path;
            if (resource.parameter == null) {
                return base;
            }
            return base
                    + "?"
                    + resource.parameter
                    + "="
                    + encode(resource == Resource.APP_LISTS ? collection : external);
        }
    }

    /**
     * Tracks one application's membership in a collection in checkpoint state. It stores only the
     * IDs needed to build its metrics requests.
     */
    static final class Binding implements Serializable {
        private static final long serialVersionUID = 1L;
        final String collection;
        final String external;

        Binding(String collection, String external) {
            this.collection = collection;
            this.external = external;
        }
    }

    /**
     * Holds one emitted provider record and the exact collection and app context for its row. The
     * intrinsic provider object is preserved as JSON text.
     */
    static final class Row {
        final String collection;
        final String external;
        final String payload;

        Row(String collection, String external, String payload) {
            this.collection = collection;
            this.external = external;
            this.payload = payload;
        }
    }

    /**
     * Keeps one bounded response and associated discovery staged until its rows finish emitting.
     * The digest covers both output and discovery so partial recovery cannot silently change
     * either.
     */
    static final class Batch {
        final List<Row> rows;
        final List<String> collections;
        final List<Binding> bindings;
        final String digest;

        Batch(List<Row> rows, List<String> collections, List<Binding> bindings) {
            this.rows = rows;
            this.collections = collections;
            this.bindings = bindings;
            MessageDigest digest = AppfollowConfig.digest();
            updateCount(digest, rows.size());
            rows.forEach(row -> update(digest, row.payload));
            updateCount(digest, collections.size());
            collections.forEach(collection -> update(digest, collection));
            updateCount(digest, bindings.size());
            bindings.forEach(
                    binding -> {
                        update(digest, binding.collection);
                        update(digest, binding.external);
                    });
            this.digest = AppfollowConfig.digestId(digest.digest());
        }

        private static void update(MessageDigest digest, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            updateCount(digest, bytes.length);
            digest.update(bytes);
        }

        private static void updateCount(MessageDigest digest, int size) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(size).array());
        }
    }
}
