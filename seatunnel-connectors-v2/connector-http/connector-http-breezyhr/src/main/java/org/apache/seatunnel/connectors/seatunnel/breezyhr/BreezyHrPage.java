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
package org.apache.seatunnel.connectors.seatunnel.breezyhr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Validates a complete API response before exposing any rows or advancing enumeration.
 *
 * <p>Page identity includes provider record content and its order. Pipeline map entry order is
 * normalized by name so a change in member order alone does not affect recovery.
 */
final class BreezyHrPage {
    private static final int MAX_PIPELINES = 10000;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    final List<SeaTunnelRow> rows;
    final BreezyHrSplit next;
    final String fingerprint;

    private BreezyHrPage(List<SeaTunnelRow> rows, BreezyHrSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Builds the candidate work and output from a fully validated parent or child response.
     *
     * <p>Candidate discovery runs for every position, even if positions are excluded from output.
     */
    static BreezyHrPage read(
            BreezyHrTransport client,
            BreezyHrConfig options,
            BreezyHrSplit state,
            String tableId,
            long deadline)
            throws BreezyHrReadException, InterruptedException {
        BreezyHrWork work = state.work.get(0);
        JsonNode result = parse(client.get(work, deadline), work);
        List<SeaTunnelRow> rows = new ArrayList<>();
        List<BreezyHrWork> next = new ArrayList<>();
        MessageDigest digest = BreezyHrConfig.sha256();
        if (work.resource == BreezyHrResource.PIPELINES) {
            pipelines(result, work, tableId, rows, digest);
        } else {
            list(result, work, options, tableId, rows, next, digest);
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || (state.published > 0 && !fingerprint.equals(state.fingerprint))) {
            throw work.failure(
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        BreezyHrTransport.checkDeadline(work, deadline);
        return new BreezyHrPage(rows, state.complete(next), fingerprint);
    }

    private static JsonNode parse(byte[] bytes, BreezyHrWork work) throws BreezyHrReadException {
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            return JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw work.failure("expected a single valid UTF-8 JSON response");
        }
    }

    private static void list(
            JsonNode result,
            BreezyHrWork work,
            BreezyHrConfig options,
            String tableId,
            List<SeaTunnelRow> rows,
            List<BreezyHrWork> next,
            MessageDigest digest)
            throws BreezyHrReadException {
        if (result == null || !result.isArray() || result.size() > options.pageSize) {
            throw work.failure("expected a JSON array no larger than page_size");
        }
        boolean positions = work.resource == BreezyHrResource.POSITIONS;
        boolean output = options.resources.contains(work.resource);
        boolean discoverCandidates =
                positions && options.resources.contains(BreezyHrResource.CANDIDATES);
        Set<String> preceding = new HashSet<>(work.previousIds);
        Set<String> unique = new HashSet<>();
        List<String> currentIds = new ArrayList<>(result.size());
        for (JsonNode item : result) {
            if (!item.isObject()) {
                throw work.failure("expected each list record to be a JSON object");
            }
            String id = id(item.get("_id"), work);
            if (discoverCandidates && !safePosition(id)) {
                throw work.failure("position identifier is unsafe in an API path segment");
            }
            String hash = BreezyHrConfig.digest(id);
            if (!unique.add(hash) || preceding.contains(hash)) {
                throw work.failure("duplicate identifier within or across adjacent pages");
            }
            currentIds.add(hash);
            if (output) {
                rows.add(row(work, id, item, tableId, digest));
            }
            if (discoverCandidates) {
                next.add(new BreezyHrWork(BreezyHrResource.CANDIDATES, id));
            }
        }
        // An empty page terminates its scope. A short page is still a valid numbered page.
        if (!currentIds.isEmpty()) {
            next.add(work.nextPage(currentIds));
        }
    }

    private static void pipelines(
            JsonNode result,
            BreezyHrWork work,
            String tableId,
            List<SeaTunnelRow> rows,
            MessageDigest digest)
            throws BreezyHrReadException {
        if (result == null || !result.isObject() || result.size() > MAX_PIPELINES) {
            throw work.failure("expected a JSON pipeline map with at most 10000 entries");
        }
        TreeMap<String, JsonNode> ordered = new TreeMap<>();
        result.fields().forEachRemaining(entry -> ordered.put(entry.getKey(), entry.getValue()));
        for (Map.Entry<String, JsonNode> entry : ordered.entrySet()) {
            String name = entry.getKey();
            JsonNode item = entry.getValue();
            if (!validIdentifier(name) || !item.isObject() || !item.path("pipeline").isArray()) {
                throw work.failure("expected a named pipeline object with a stage array");
            }
            rows.add(row(work, name, item, tableId, digest));
        }
    }

    private static String id(JsonNode node, BreezyHrWork work) throws BreezyHrReadException {
        String value = node == null ? null : node.textValue();
        if (!validIdentifier(value)) {
            throw work.failure("expected a non-empty textual _id on every list record");
        }
        return value;
    }

    private static boolean validIdentifier(String id) {
        return id != null
                && !id.isBlank()
                && id.length() <= 256
                && id.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    private static boolean safePosition(String id) {
        // Reject path separators and pre-encoded escapes before adding the verified path segment.
        return !".".equals(id)
                && !"..".equals(id)
                && id.chars()
                        .noneMatch(c -> c == '/' || c == '\\' || c == '%' || c == '?' || c == '#');
    }

    private static SeaTunnelRow row(
            BreezyHrWork work, String id, JsonNode record, String tableId, MessageDigest digest)
            throws BreezyHrReadException {
        String payload;
        try {
            payload = JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw work.failure("could not encode the provider record as JSON");
        }
        hashField(digest, id);
        hashField(digest, payload);
        boolean position = work.resource == BreezyHrResource.POSITIONS;
        JsonNode country = record.path("location").path("country");
        Object[] fields = {
            work.resource.id,
            id,
            position ? id : work.parent,
            position ? record.path("type").path("name").textValue() : null,
            position ? country.path("id").textValue() : null,
            position ? country.path("name").textValue() : null,
            work.resource == BreezyHrResource.CANDIDATES
                    ? record.path("stage").path("name").textValue()
                    : null,
            payload
        };
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tableId);
        return row;
    }

    private static void hashField(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }
}
