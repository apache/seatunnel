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

package org.apache.seatunnel.connectors.seatunnel.brevo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Interprets endpoint-specific response envelopes and preserves provider JSON values.
 *
 * <p>Record paging uses raw response positions independently of any local time filter.
 */
final class BrevoJson {
    private static final long SECONDS_LIMIT = 100_000_000_000L;
    private static final Pattern EPOCH = Pattern.compile("-?[0-9]{1,19}");
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

    private BrevoJson() {}

    /**
     * Rejects malformed JSON and Brevo error objects, including an error served with HTTP 200.
     *
     * @param bytes the full bounded UTF-8 response
     * @return the provider response, whose shape is checked per resource
     * @throws Failure on a framing, JSON or provider error
     */
    static JsonNode parse(byte[] bytes) throws Failure {
        JsonNode root;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            root = JSON.readTree(bytes);
        } catch (CharacterCodingException ignored) {
            throw failure("response is not valid UTF-8 JSON");
        } catch (IOException | RuntimeException ignored) {
            throw failure("response is not valid Brevo JSON");
        }
        if (root == null || (!root.isObject() && !root.isArray())) {
            throw failure("response must be a Brevo object or array");
        }
        if (root.isObject() && root.hasNonNull("code")) {
            throw failure("Brevo returned an error object");
        }
        return root;
    }

    /**
     * Reads one provider page and fingerprints its raw content before applying selection.
     *
     * @param response the parsed provider response
     * @param resource the selected export or its parent inventory
     * @param limit the requested limit, zero for an unpaginated response
     * @param position provider offset or companies' page number
     * @param discovery whether only parent IDs are needed
     * @param parent optional discovered ID for a child page
     * @param from inclusive local lower bound
     * @param until inclusive local upper bound
     * @return finite selected rows and raw response progress
     * @throws Failure if a response, identity or date cannot be interpreted
     */
    static Page page(
            JsonNode response,
            BrevoResource resource,
            int limit,
            int position,
            boolean discovery,
            Long parent,
            Instant from,
            Instant until)
            throws Failure {
        Long total = response.isObject() ? nonNegative(response.get("count"), "count") : null;
        Iterable<JsonNode> values;
        int count;
        if (resource == BrevoResource.ACCOUNT) {
            if (!response.isObject()
                    || !(response.hasNonNull("organization_id")
                            || response.hasNonNull("user_id"))) {
                throw failure("invalid account export object");
            }
            values = List.of(response);
            count = 1;
        } else {
            JsonNode collection = resource.array == null ? response : response.path(resource.array);
            if (resource.array != null
                    && collection.isMissingNode()
                    && Long.valueOf(0).equals(total)) {
                collection = JSON.createArrayNode();
            }
            int cap = limit == 0 ? BrevoConfig.MAX_UNPAGED_ROWS : limit;
            if (!collection.isArray() || collection.size() > cap) {
                throw failure("invalid or oversized export collection");
            }
            values = collection;
            count = collection.size();
        }
        if (resource == BrevoResource.DOMAINS) {
            verifyDomains(response);
        }
        boolean more = hasMore(resource, count, limit, position, total);
        List<Long> ids = new ArrayList<>(discovery ? count : 0);
        List<Record> rows = new ArrayList<>(discovery ? 0 : count);
        MessageDigest raw = sha256();
        Set<Long> distinct = discovery ? new HashSet<>() : Set.of();
        for (JsonNode value : values) {
            if (!value.isObject()) {
                throw failure("export collection contains a non-object record");
            }
            if (discovery) {
                Long id = parentId(value.get(resource.identity));
                if (!distinct.add(id)) {
                    throw failure("parent page contains a duplicate identifier");
                }
                ids.add(id);
                frame(raw, id.toString());
            } else {
                String original = encode(value);
                frame(raw, original);
                Record record = record(value, original, resource, parent, from, until);
                if (record != null) {
                    rows.add(record);
                }
            }
        }
        return new Page(ids, rows, count, more, hex(raw.digest()));
    }

    private static boolean hasMore(
            BrevoResource resource, int rows, int limit, int position, Long total) {
        if (limit == 0 || rows == 0) {
            return false;
        }
        if (total == null
                && (resource == BrevoResource.COMPANIES || resource == BrevoResource.DEALS)) {
            // The current reference does not publish a maximum for these two collection limits.
            return true;
        }
        long offset =
                resource == BrevoResource.COMPANIES ? (long) (position - 1) * limit : position;
        return rows == limit || (total != null && total > offset + rows);
    }

    private static void verifyDomains(JsonNode response) throws Failure {
        Long current = nonNegative(response.get("current_page"), "current_page");
        Long total = nonNegative(response.get("total_pages"), "total_pages");
        if ((current == null) != (total == null)) {
            throw failure("incomplete sender domain pagination metadata");
        }
        if (current != null && current < total) {
            // The provider's route has no published request parameter for a subsequent page.
            throw failure(
                    "sender domains response reports additional pages without a documented cursor");
        }
    }

    private static Long nonNegative(JsonNode value, String key) throws Failure {
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw failure("invalid " + key + " in response");
        }
        return value.longValue();
    }

    private static Long parentId(JsonNode value) throws Failure {
        Long id = nonNegative(value, "parent ID");
        if (id == null || id == 0) {
            throw failure("parent page is missing a positive 64-bit identifier");
        }
        return id;
    }

    private static String identity(JsonNode value) throws Failure {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isTextual() || value.isIntegralNumber()) {
            return value.asText();
        }
        throw failure("invalid export record identifier");
    }

    private static Record record(
            JsonNode value,
            String original,
            BrevoResource resource,
            Long parent,
            Instant from,
            Instant until)
            throws Failure {
        ObjectNode row = (ObjectNode) value;
        JsonNode rawTime =
                resource.modifiedField == null
                        ? null
                        : resource.usesNestedUpdatedTime()
                                ? row.path("attributes").get(resource.modifiedField)
                                : row.get(resource.modifiedField);
        Instant time = timestamp(rawTime, resource);
        if (resource.modifiedField != null && (from != null || until != null)) {
            if (time == null) {
                throw failure("missing or invalid modification or event time in a bounded scan");
            }
            if ((from != null && time.isBefore(from)) || (until != null && time.isAfter(until))) {
                return null;
            }
        }

        boolean synthesizeTime =
                resource.usesNestedUpdatedTime() && rawTime != null && !row.has("last_updated_at");
        if (synthesizeTime) {
            row.set("last_updated_at", rawTime);
        }
        if (parent != null) {
            JsonNode reported = row.get(resource.parentField);
            if (reported != null && !reported.isNull() && !parent.equals(parentId(reported))) {
                throw failure("response parent does not match the requested parent");
            }
            row.put(resource.parentField, parent.longValue());
        }
        return new Record(
                identity(row.get(resource.identity)),
                parent == null ? null : parent.toString(),
                time == null ? null : time.toString(),
                synthesizeTime || parent != null ? encode(row) : original);
    }

    /**
     * Resolves timestamp formats used by different Brevo API families.
     *
     * <p>The wire value remains in the output JSON. Only the projected value is normalized to UTC.
     */
    private static Instant timestamp(JsonNode value, BrevoResource resource) {
        if (value == null || value.isNull()) {
            return null;
        }
        boolean crmEpoch = resource == BrevoResource.TASKS || resource == BrevoResource.NOTES;
        try {
            if (crmEpoch && value.isIntegralNumber() && value.canConvertToLong()) {
                return epoch(value.longValue());
            }
            if (value.isTextual()) {
                String text = value.textValue();
                if (crmEpoch && EPOCH.matcher(text).matches()) {
                    return epoch(Long.parseLong(text));
                }
                if (resource == BrevoResource.TRANSACTIONAL_EVENTS && text.length() == 10) {
                    return LocalDate.parse(text).atStartOfDay().toInstant(ZoneOffset.UTC);
                }
                int offsetIndex = text.length() - 5;
                if (offsetIndex > 18
                        && (text.charAt(offsetIndex) == '+' || text.charAt(offsetIndex) == '-')) {
                    text =
                            text.substring(0, offsetIndex + 3)
                                    + ':'
                                    + text.substring(offsetIndex + 3);
                }
                return OffsetDateTime.parse(text).toInstant();
            }
        } catch (DateTimeException | NumberFormatException ignored) {
            // The caller decides whether an unknown timestamp can be emitted with a null
            // projection.
        }
        return null;
    }

    private static Instant epoch(long value) {
        return value > -SECONDS_LIMIT && value < SECONDS_LIMIT
                ? Instant.ofEpochSecond(value)
                : Instant.ofEpochMilli(value);
    }

    private static String encode(JsonNode node) throws Failure {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a Brevo export record");
        }
    }

    static String fingerprint(List<String> values) {
        MessageDigest digest = sha256();
        for (String value : values) {
            frame(digest, value);
        }
        return hex(digest.digest());
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(64);
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
     * Delivers one selected provider record and its optional context to the engine thread.
     *
     * <p>The JSON payload retains native field types without imposing a per-resource schema.
     */
    static final class Record {
        final String id;
        final String parent;
        final String modifiedAt;
        final String json;

        Record(String id, String parent, String modifiedAt, String json) {
            this.id = id;
            this.parent = parent;
            this.modifiedAt = modifiedAt;
            this.json = json;
        }
    }

    /**
     * Holds decoded records or ordered parent identities and an immutable raw fingerprint.
     *
     * <p>rawSize includes the records excluded from output by the local date filter.
     */
    static final class Page {
        final List<Long> ids;
        final List<Record> rows;
        final int rawSize;
        final boolean more;
        final String rawDigest;
        final String replayDigest;

        Page(List<Long> ids, List<Record> rows, int rawSize, boolean more, String rawDigest) {
            this.ids = ids;
            this.rows = rows;
            this.rawSize = rawSize;
            this.more = more;
            this.rawDigest = rawDigest;
            this.replayDigest = fingerprint(List.of(rawDigest, Boolean.toString(more)));
        }
    }

    /**
     * Reports a sanitized provider failure.
     *
     * <p>Messages carry only connector-owned descriptions, never raw response bodies or IDs.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }
}
