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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates Calendly response envelopes and projects their records without coercing JSON values.
 *
 * <p>Number precision, null values, nested objects and provider metadata survive the projection.
 * Only canonical API resource URIs participate in scope and identifiers.
 */
final class CalendlyJson {
    static final String API_HOST = "api.calendly.com";
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(32 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final Pattern API_PATH =
            Pattern.compile("/[a-zA-Z][a-zA-Z0-9_]*(/[a-zA-Z0-9_-]+){1,4}");

    private CalendlyJson() {}

    /**
     * Parses bounded, strictly decoded UTF-8 object JSON from either fixed origin.
     *
     * @param bytes the complete decoded message
     * @return the parsed object
     * @throws Failure if JSON framing, encoding or length is invalid
     */
    static JsonNode parse(byte[] bytes) throws Failure {
        JsonNode root;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            root = JSON.readTree(bytes);
        } catch (CharacterCodingException ignored) {
            throw failure("response is not valid UTF-8 JSON");
        } catch (IOException | RuntimeException ignored) {
            throw failure("response is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw failure("response is not a JSON object");
        }
        return root;
    }

    /**
     * Validates the page and hashes all its ordered raw records, including filtered rows.
     *
     * <p>Response URLs are never request targets. Only next_page_token can advance a cursor.
     */
    static Page page(JsonNode root, CalendlyResource resource, int count) throws Failure {
        JsonNode collection = root.get("collection");
        if (collection == null
                || !collection.isArray()
                || (resource.counted && collection.size() > count)) {
            throw failure("invalid or oversized collection");
        }
        JsonNode pagination = root.get("pagination");
        if (resource.counted && (pagination == null || !pagination.isObject())) {
            throw failure("paged response is missing its pagination object");
        }
        if (pagination != null && !pagination.isNull() && !pagination.isObject()) {
            throw failure("pagination must be an object or null");
        }
        JsonNode token = pagination == null ? null : pagination.get("next_page_token");
        String next = token == null || token.isNull() ? null : token.textValue();
        if (token != null
                && !token.isNull()
                && (next == null || (!next.isEmpty() && !CalendlyConfig.validCursor(next)))) {
            throw failure("invalid next_page_token");
        }
        if ("".equals(next)) {
            next = null;
        }
        if (!resource.pageable() && next != null) {
            throw failure("unexpected continuation for an unpaged resource");
        }
        List<JsonNode> rows = new ArrayList<>(collection.size());
        MessageDigest digest = sha256();
        update(digest, next == null ? "" : next);
        for (JsonNode row : collection) {
            if (!row.isObject()) {
                throw failure("collection contains a non-object record");
            }
            rows.add(row);
            update(digest, encode(row));
        }
        return new Page(rows, next, hex(digest.digest()));
    }

    static JsonNode single(JsonNode root) throws Failure {
        JsonNode value = root.get("resource");
        if (value == null || !value.isObject()) {
            throw failure("missing resource object");
        }
        return value;
    }

    /**
     * Extracts a canonical, same-origin API identifier without following the provider URI.
     *
     * @param node the provider URI as a JSON field
     * @param collection the fixed expected path prefix, or null for a generic projection
     * @return the validated reference
     * @throws Failure if the URI is noncanonical or crosses collections or origins
     */
    static Reference reference(JsonNode node, String collection) throws Failure {
        String value = node == null ? null : node.textValue();
        if (value != null && value.length() <= 1024) {
            try {
                URI uri = URI.create(value);
                String path = uri.getRawPath();
                if ("https".equals(uri.getScheme())
                        && API_HOST.equals(uri.getRawAuthority())
                        && uri.getRawQuery() == null
                        && uri.getRawFragment() == null
                        && path != null
                        && API_PATH.matcher(path).matches()
                        && (collection == null
                                || (path.startsWith(collection + "/")
                                        && path.indexOf('/', collection.length() + 1) < 0))) {
                    return new Reference(value, path.substring(path.lastIndexOf('/') + 1));
                }
            } catch (IllegalArgumentException ignored) {
                // Provider-controlled URI text must stay out of diagnostics.
            }
        }
        throw failure("record contains an invalid or out-of-scope resource URI");
    }

    /**
     * Projects one raw record or filters an incremental cursor outside the fixed read window.
     *
     * <p>Projection does not read URLs. When two parents reuse an ID the parent column preserves
     * the identities and the raw page offset still counts unselected rows.
     */
    static Record project(
            JsonNode wire,
            CalendlyResource resource,
            Reference scope,
            CalendlyConfig config,
            Instant ceiling)
            throws Failure {
        String timestamp =
                optionalText(
                        wire.get(
                                resource.cursorField == null
                                        ? "updated_at"
                                        : resource.cursorField));
        if (resource.cursorField != null) {
            Instant instant;
            try {
                instant = timestamp == null ? null : OffsetDateTime.parse(timestamp).toInstant();
            } catch (DateTimeException ignored) {
                throw failure("record has an invalid resource cursor timestamp");
            }
            if (instant == null) {
                throw failure("record is missing its resource cursor timestamp");
            }
            if (instant.isBefore(config.lower) || instant.isAfter(ceiling)) {
                return null;
            }
            timestamp = instant.toString();
        }
        JsonNode rawUri = wire.get("uri");
        String expectedCollection = null;
        if (resource == CalendlyResource.SCHEDULED_EVENTS) {
            expectedCollection = "/scheduled_events";
        } else if (resource == CalendlyResource.EVENT_INVITEES) {
            expectedCollection = "/scheduled_events/" + scope.id + "/invitees";
        } else if (resource == CalendlyResource.ORGANIZATION_INVITATIONS) {
            expectedCollection = "/organizations/" + scope.id + "/invitations";
        }
        Reference entity =
                (rawUri == null || rawUri.isNull()) && expectedCollection == null
                        ? null
                        : reference(rawUri, expectedCollection);
        String id = entity == null ? null : entity.id;
        checkScope(wire, resource, scope);
        JsonNode data = wire;
        switch (resource) {
            case SCHEDULED_EVENTS:
                data = ((ObjectNode) wire).deepCopy().put("id", id);
                break;
            case EVENT_INVITEES:
                data =
                        ((ObjectNode) wire)
                                .deepCopy()
                                .put("id", id)
                                .put("scheduled_event_id", scope.id);
                break;
            case EVENT_TYPE_AVAILABILITY_SCHEDULES:
                JsonNode rule = wire.path("availability_rule");
                if (!rule.isObject()) {
                    throw failure("record is missing its availability rule object");
                }
                JsonNode link = rule.get("uri");
                Reference availability =
                        link == null || link.isNull()
                                ? null
                                : reference(link, "/user_availability_schedules");
                id = availability == null ? null : availability.id;
                data =
                        ((ObjectNode) wire)
                                .deepCopy()
                                .put(
                                        "availability_rule_uri",
                                        availability == null ? null : availability.uri);
                break;
            case LOCATIONS:
                data = ((ObjectNode) wire).deepCopy().put("user", scope.uri);
                break;
            default:
                break;
        }
        return new Record(id, scope == null ? null : scope.id, timestamp, encode(data));
    }

    static void checkScope(JsonNode wire, CalendlyResource resource, Reference scope)
            throws Failure {
        if (scope == null || resource == CalendlyResource.LOCATIONS) {
            return;
        }
        String field = resource.scopeParameter;
        if (resource == CalendlyResource.EVENT_INVITEES) {
            field = "event";
        } else if (resource == CalendlyResource.ORGANIZATION_INVITATIONS) {
            field = "organization";
        } else if (resource == CalendlyResource.ROUTING_FORM_SUBMISSIONS) {
            field = "routing_form";
        }
        JsonNode value = field == null ? null : wire.get(field);
        if (value != null && !value.isNull() && !scope.uri.equals(value.textValue())) {
            throw failure("record does not belong to the requested scope");
        }
    }

    static String optionalText(JsonNode value) throws Failure {
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw failure("expected a string field");
        }
        return value.textValue();
    }

    /**
     * Encodes only the durable OAuth state, keeping the bearer token out of the store.
     *
     * @param refreshToken next refresh token, or null to write the pending marker
     * @return UTF-8 store document
     * @throws Failure if serialization fails
     */
    static byte[] refreshDocument(String refreshToken) throws Failure {
        ObjectNode state = JSON.createObjectNode().put("version", 1);
        if (refreshToken == null) {
            state.put("pending", true);
        } else {
            state.put("refresh_token", refreshToken);
        }
        try {
            return JSON.writeValueAsBytes(state);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not encode OAuth token state");
        }
    }

    private static String encode(JsonNode node) throws Failure {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a Calendly record");
        }
    }

    static String fingerprint(List<String> values) {
        MessageDigest digest = sha256();
        for (String value : values) {
            update(digest, value);
        }
        return hex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 15, 16));
            result.append(Character.forDigit(value & 15, 16));
        }
        return result.toString();
    }

    static Failure failure(String reason) {
        return new Failure(reason);
    }

    /**
     * Stores a checked canonical API URI and its bare identifier.
     *
     * <p>Only identity may cross pagination scopes, never a provider-supplied request URL.
     */
    static final class Reference {
        final String uri;
        final String id;

        Reference(String uri, String id) {
            this.uri = uri;
            this.id = id;
        }
    }

    /**
     * Stores an ordered page and the replay digest of every original row.
     *
     * <p>The continuation is part of the fingerprint so a changed page cannot be resumed.
     */
    static final class Page {
        final List<JsonNode> rows;
        final String next;
        final String digest;

        Page(List<JsonNode> rows, String next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Stores a validated row before publishing it at its raw page offset.
     *
     * <p>The four output strings retain the provider data and its request context separately.
     */
    static final class Record {
        final String id;
        final String parentId;
        final String timestamp;
        final String json;

        Record(String id, String parentId, String timestamp, String json) {
            this.id = id;
            this.parentId = parentId;
            this.timestamp = timestamp;
            this.json = json;
        }
    }

    /**
     * Carries a local diagnostic safe to display in engine logs.
     *
     * <p>It contains no provider response body, credentials, cursors, or request URI.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super("Calendly source: " + reason);
        }
    }
}
