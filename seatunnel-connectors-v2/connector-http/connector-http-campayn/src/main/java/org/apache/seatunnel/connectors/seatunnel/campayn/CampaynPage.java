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
package org.apache.seatunnel.connectors.seatunnel.campayn;

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
import java.util.Set;

/**
 * Validates a complete Campayn array and its list provenance before row exposure.
 *
 * <p>Parents can be discovered without publishing parent rows. A saved partial array always binds
 * both row content and order.
 */
final class CampaynPage {
    private static final int MAX_RECORDS = 50000;
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
    final CampaynSplit next;
    final String fingerprint;

    private CampaynPage(List<SeaTunnelRow> rows, CampaynSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Reads one complete collection and advances its logical request.
     *
     * <p>A child scope changes only after the current collection's final row is published.
     */
    static CampaynPage read(
            CampaynTransport client,
            CampaynConfig config,
            CampaynSplit state,
            String table,
            long deadline)
            throws CampaynReadException, InterruptedException {
        CampaynResource resource = config.plan.get(state.resourceIndex);
        String parent = state.parent(resource);
        JsonNode records = parse(client.get(resource, parent, deadline), resource);
        boolean discover =
                resource == CampaynResource.LISTS
                        && config.resources.stream().anyMatch(item -> item.listScoped);
        if (discover && records.size() > config.maxLists) {
            throw resource.failure("list discovery exceeds max_lists");
        }
        boolean emit = config.resources.contains(resource);
        List<String> discovered = discover ? new ArrayList<>(records.size()) : List.of();
        Set<String> ids = new HashSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        MessageDigest digest = CampaynConfig.sha256();
        for (JsonNode record : records) {
            String id = identifier(record.path("id"), resource);
            if (!ids.add(id)) {
                throw resource.failure("duplicate provider id within a collection");
            }
            if (discover) {
                if (!id.matches("[A-Za-z0-9_-]{1,128}")) {
                    throw resource.failure("list identifier is unsafe in an API path");
                }
                discovered.add(id);
            }
            if (resource == CampaynResource.FORMS
                    && record.hasNonNull("contact_list_id")
                    && !parent.equals(identifier(record.path("contact_list_id"), resource))) {
                throw resource.failure("form does not belong to the requested list");
            }
            if (emit) {
                String payload = payload(record, resource);
                hashField(digest, payload);
                String listId = resource == CampaynResource.LISTS ? id : parent;
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {resource.id, id, config.subDomain, listId, payload});
                row.setTableId(table);
                rows.add(row);
            }
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    "provider collection changed since checkpoint or has an invalid row offset");
        }
        CampaynTransport.checkDeadline(resource, deadline);
        return new CampaynPage(rows, state.advance(config, resource, discovered), fingerprint);
    }

    private static JsonNode parse(byte[] bytes, CampaynResource resource)
            throws CampaynReadException {
        JsonNode root;
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            root = JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("expected a single valid UTF-8 JSON response");
        }
        if (root == null || !root.isArray() || root.size() > MAX_RECORDS) {
            throw resource.failure("expected a JSON array with at most 50000 records");
        }
        for (JsonNode item : root) {
            if (!item.isObject()) {
                throw resource.failure("expected each collection record to be a JSON object");
            }
        }
        return root;
    }

    private static String identifier(JsonNode field, CampaynResource resource)
            throws CampaynReadException {
        String value =
                field.isTextual()
                                || field.isIntegralNumber() && field.bigIntegerValue().signum() > 0
                        ? field.asText()
                        : null;
        if (value == null
                || value.isBlank()
                || value.length() > 256
                || value.codePoints()
                        .anyMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE)) {
            throw resource.failure("expected a textual or positive integral provider id");
        }
        return value;
    }

    private static String payload(JsonNode record, CampaynResource resource)
            throws CampaynReadException {
        try {
            return JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not encode the provider record as JSON");
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
