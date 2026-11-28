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

package org.apache.seatunnel.connectors.seatunnel.braze;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.braze.BrazeConfig.Resource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates export payloads while preserving nested records and their numeric values.
 *
 * <p>Provider error text and identifiers never become part of an exception message.
 */
final class BrazeJson {
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

    private BrazeJson() {}

    /**
     * Checks the status inside a successful HTTP response before touching record data.
     *
     * @param bytes the complete bounded UTF-8 response
     * @return the successful JSON export envelope
     * @throws Failure if the response is invalid or reports a provider error
     */
    static JsonNode parse(byte[] bytes) throws Failure {
        JsonNode root;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            root = JSON.readTree(bytes);
        } catch (CharacterCodingException ignored) {
            throw failure("response is not valid UTF-8 JSON");
        } catch (IOException | RuntimeException ignored) {
            throw failure("response is not valid Braze JSON");
        }
        if (root == null || !root.isObject()) {
            throw failure("response is not a Braze JSON object");
        }
        JsonNode errors = root.path("errors");
        if (!"success".equals(root.path("message").textValue())
                || (!errors.isMissingNode()
                        && !errors.isNull()
                        && !(errors.isArray() && errors.isEmpty()))) {
            throw failure("Braze did not return a successful export envelope");
        }
        return root;
    }

    /**
     * Decodes an ordered list and produces an identity fingerprint for parent progress.
     *
     * @param root the successful provider envelope
     * @param collection the list resource
     * @param discovery whether only discovery IDs are needed
     * @return the page, including its observed continuation
     * @throws Failure when the list or its identities are malformed
     */
    static Page list(JsonNode root, Resource collection, boolean discovery) throws Failure {
        JsonNode records = root.path(collection.member);
        if (!records.isArray() || records.size() > collection.listSize) {
            throw failure("invalid or oversized " + collection.member + " collection");
        }
        List<Record> rows = new ArrayList<>(discovery ? 0 : records.size());
        List<String> ids = new ArrayList<>(records.size());
        Set<String> distinct = new HashSet<>();
        for (JsonNode value : records) {
            String id = id(collection == Resource.EVENTS ? value : value.path("id"));
            if (!distinct.add(id)) {
                throw failure("duplicate identifier in numbered page");
            }
            ids.add(id);
            if (!discovery) {
                JsonNode row =
                        collection == Resource.EVENTS
                                ? JSON.createObjectNode().put("event_name", id)
                                : value;
                rows.add(new Record(id, null, encode(row)));
            }
        }
        boolean more =
                collection == Resource.CARDS ? !ids.isEmpty() : ids.size() == collection.listSize;
        return new Page(ids, rows, more);
    }

    /**
     * Reads one details object or one analytics window and attaches its provider lineage.
     *
     * @param root the successful provider envelope
     * @param collection the resource being exported
     * @param id the selected parent ID, if any
     * @param from first day in the requested window
     * @param until final day in the requested window
     * @return ready-to-emit records for the selected parent/window
     * @throws Failure when the provider changes the response shape or lineage
     */
    static Page records(
            JsonNode root, Resource collection, String id, LocalDate from, LocalDate until)
            throws Failure {
        if (collection.days == 0) {
            if (!root.path("name").isTextual()) {
                throw failure("details response is missing its name");
            }
            return new Page(List.of(), List.of(record(root, collection, id, null)), false);
        }

        JsonNode data = collection == Resource.CANVASES_ANALYTICS ? root.path("data") : root;
        JsonNode records = data.path(collection.member);
        if (!records.isArray() || records.size() > collection.days) {
            throw failure("invalid or oversized analytics series");
        }
        List<Record> rows = new ArrayList<>(records.size());
        for (JsonNode value : records) {
            JsonNode field = value.path("time");
            LocalDate day = bucket(field);
            if (day.isBefore(from) || day.isAfter(until)) {
                throw failure("analytics bucket is outside the requested window");
            }
            rows.add(record(value, collection, id, field.textValue()));
        }
        return new Page(List.of(), rows, false);
    }

    private static Record record(JsonNode row, Resource collection, String id, String time)
            throws Failure {
        if (!row.isObject()) {
            throw failure("export record is not a JSON object");
        }
        if (collection.lineage != null) {
            JsonNode actual = row.path(collection.lineage);
            if (!actual.isMissingNode() && !actual.isNull() && !id.equals(actual.textValue())) {
                throw failure("response lineage does not match the requested parent");
            }
            ((ObjectNode) row).put(collection.lineage, id);
        }
        return new Record(id, time, encode(row));
    }

    private static String id(JsonNode field) throws Failure {
        if (!field.isTextual() || !BrazeConfig.validId(field.textValue())) {
            throw failure("invalid identifier in numbered page");
        }
        return field.textValue();
    }

    private static LocalDate bucket(JsonNode field) throws Failure {
        if (field.isTextual()) {
            String time = field.textValue();
            try {
                return time.length() == 10
                        ? LocalDate.parse(time)
                        : OffsetDateTime.parse(time).toLocalDate();
            } catch (DateTimeException ignored) {
                // Provider timestamps must not be disclosed in diagnostics.
            }
        }
        throw failure("invalid analytics time in response");
    }

    private static String encode(JsonNode row) throws Failure {
        try {
            return JSON.writeValueAsString(row);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a Braze record");
        }
    }

    static String fingerprint(List<String> values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            StringBuilder hex = new StringBuilder(64);
            for (byte value : digest.digest()) {
                hex.append(Character.forDigit((value >>> 4) & 15, 16));
                hex.append(Character.forDigit(value & 15, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static Failure failure(String reason) {
        return new Failure(reason);
    }

    /**
     * Carries one exact export row from the retrieval worker to the polling thread.
     *
     * <p>Raw field values remain in the serialized JSON document.
     */
    static final class Record {
        final String id;
        final String time;
        final String json;

        Record(String id, String time, String json) {
            this.id = id;
            this.time = time;
            this.json = json;
        }
    }

    /**
     * Holds one bounded response and its fingerprints for deterministic replay.
     *
     * <p>Identities and record payloads are measured independently.
     */
    static final class Page {
        final List<String> ids;
        final List<Record> rows;
        final String idDigest;
        final String rowDigest;
        final boolean more;

        Page(List<String> ids, List<Record> rows, boolean more) {
            this.ids = ids;
            this.rows = rows;
            this.idDigest = fingerprint(ids);
            List<String> content = new ArrayList<>(rows.size() + 1);
            content.add(Boolean.toString(more));
            for (Record row : rows) {
                content.add(row.json);
            }
            this.rowDigest = fingerprint(content);
            this.more = more;
        }
    }

    /**
     * Indicates a safe export failure that can be shown by the engine.
     *
     * <p>Messages are built from connector-controlled constants.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }
}
