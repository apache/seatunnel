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
package org.apache.seatunnel.connectors.seatunnel.callrail;

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
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Validates an account response and computes bounded output before advancing source state.
 *
 * <p>The raw records and their useful continuation fields are part of the partial-page proof.
 */
final class CallRailPage {
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
    final CallRailSplit next;
    final String fingerprint;

    private CallRailPage(List<SeaTunnelRow> rows, CallRailSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Acquires and validates one entire page before any of its rows become visible.
     *
     * <p>Rows filtered by timestamp still contribute to pagination and recovery checks.
     */
    static CallRailPage read(
            CallRailTransport client,
            CallRailConfig config,
            CallRailSplit state,
            String tableId,
            long deadline)
            throws CallRailReadException, InterruptedException {
        CallRailResource resource = config.resources.get(state.resourceIndex);
        JsonNode root =
                parse(
                        client.get(
                                CallRailPaging.request(config, state, resource),
                                resource,
                                state.page,
                                deadline),
                        resource,
                        state.page);
        JsonNode array = root.path(resource.id);
        if (!array.isArray() || array.size() > config.pageSize) {
            throw resource.failure(state.page, "expected a resource array within page_size");
        }
        Instant left = config.start;
        Instant right = state.end;
        if (resource.dateFiltered) {
            Instant sliceStart = state.windowStart.atStartOfDay(ZoneOffset.UTC).toInstant();
            left = left.isAfter(sliceStart) ? left : sliceStart;
            right = state.windowEnd();
        }
        Set<String> seen = new HashSet<>(state.previousIds);
        List<String> ids = new ArrayList<>(array.size());
        List<SeaTunnelRow> rows = new ArrayList<>();
        MessageDigest digest = CallRailConfig.sha256();
        for (JsonNode item : array) {
            String id = identity(item, resource, state.page);
            String hashed = CallRailConfig.digest(id);
            if (!seen.add(hashed)) {
                throw resource.failure(
                        state.page, "duplicate identifier within or across adjacent pages");
            }
            ids.add(hashed);
            Instant time = timestamp(item, resource, state.page);
            String payload = payload(item, resource, state.page);
            hashField(digest, payload);
            if (time == null || !time.isBefore(left) && time.isBefore(right)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.id,
                                    id,
                                    config.accountId,
                                    time == null ? null : time.toString(),
                                    payload
                                });
                row.setTableId(tableId);
                rows.add(row);
            }
        }
        CallRailSplit next = CallRailPaging.advance(root, config, state, resource, ids);
        hashField(digest, Objects.toString(next.nextUrl, ""));
        hashField(
                digest,
                next.resourceIndex
                        + ":"
                        + next.windowStart
                        + ":"
                        + next.page
                        + ":"
                        + next.observedSize);
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    state.page,
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        CallRailTransport.checkDeadline(resource, state.page, deadline);
        return new CallRailPage(rows, next, fingerprint);
    }

    private static JsonNode parse(byte[] bytes, CallRailResource resource, int page)
            throws CallRailReadException {
        JsonNode root;
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            root = JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure(page, "expected a single valid UTF-8 JSON response");
        }
        if (root == null
                || !root.isObject()
                || root.hasNonNull("error")
                || root.hasNonNull("errors")) {
            throw resource.failure(page, "expected a successful account list object");
        }
        return root;
    }

    private static String identity(JsonNode record, CallRailResource resource, int page)
            throws CallRailReadException {
        JsonNode field = record.path("id");
        String id =
                field.isTextual()
                                || field.isIntegralNumber() && field.bigIntegerValue().signum() > 0
                        ? field.asText()
                        : null;
        if (!record.isObject()
                || id == null
                || id.isBlank()
                || id.length() > 256
                || id.codePoints()
                        .anyMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE)) {
            throw resource.failure(page, "expected a textual or positive integral provider ID");
        }
        return id;
    }

    private static Instant timestamp(JsonNode record, CallRailResource resource, int page)
            throws CallRailReadException {
        JsonNode field = record.path(resource.time);
        if (!resource.dateFiltered && (field.isNull() || field.isMissingNode())) {
            // A missing inventory creation date does not make the user or company inaccessible.
            return null;
        }
        String text = field.textValue();
        try {
            if (text != null && text.length() <= 48) {
                return OffsetDateTime.parse(text).toInstant();
            }
        } catch (DateTimeException invalid) {
            // Provider values can contain private account data and stay out of diagnostics.
        }
        throw resource.failure(page, "record has no valid " + resource.time + " timestamp");
    }

    private static String payload(JsonNode item, CallRailResource resource, int page)
            throws CallRailReadException {
        try {
            return JSON.writeValueAsString(item);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure(page, "could not encode the provider record as JSON");
        }
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
