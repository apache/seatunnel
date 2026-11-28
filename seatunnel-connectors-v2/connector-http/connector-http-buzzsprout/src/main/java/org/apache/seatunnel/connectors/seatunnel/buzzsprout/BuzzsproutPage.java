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
package org.apache.seatunnel.connectors.seatunnel.buzzsprout;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates a complete metadata array and prepares its dated output.
 *
 * <p>The fingerprint includes records excluded by the window so a partial checkpoint can be resumed
 * only against the same complete provider result.
 */
final class BuzzsproutPage {
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
    final BuzzsproutSplit next;
    final String fingerprint;

    private BuzzsproutPage(List<SeaTunnelRow> rows, BuzzsproutSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Parses one whole array before the reader can publish any of it.
     *
     * <p>Unchanged array identity and exact emitted offset are required for a partial restore.
     */
    static BuzzsproutPage read(
            BuzzsproutTransport client,
            BuzzsproutConfig config,
            BuzzsproutSplit state,
            String table,
            long deadline)
            throws BuzzsproutReadException, InterruptedException {
        BuzzsproutResource resource = config.resources.get(state.resourceIndex);
        JsonNode items = parse(client.get(resource, deadline), resource);
        boolean episodes = resource == BuzzsproutResource.EPISODES;
        MessageDigest digest = BuzzsproutConfig.sha256();
        Set<String> keys = new HashSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (JsonNode item : items) {
            String id = identity(item, resource);
            if (!keys.add(id)) {
                throw resource.failure("duplicate provider id in the response");
            }
            String podcast = episodes ? podcast(item, config.podcastId, resource) : id;
            Instant published = episodes ? publishedAt(item, resource) : null;
            String payload = payload(item, resource);
            hashRecord(digest, payload);
            boolean selected =
                    published == null
                            ? !episodes || config.includeUndated
                            : !published.isBefore(config.start) && published.isBefore(state.end);
            if (selected) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.id,
                                    id,
                                    podcast,
                                    published == null ? null : published.toString(),
                                    payload
                                });
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
                    "provider list changed since checkpoint or has an invalid row offset");
        }
        BuzzsproutTransport.checkDeadline(resource, deadline);
        return new BuzzsproutPage(rows, state.next(), fingerprint);
    }

    private static JsonNode parse(byte[] bytes, BuzzsproutResource resource)
            throws BuzzsproutReadException {
        JsonNode data;
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            data = JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("expected a single valid UTF-8 JSON response");
        }
        if (data == null || !data.isArray() || data.size() > MAX_RECORDS) {
            throw resource.failure("expected a JSON array with at most 50000 records");
        }
        return data;
    }

    private static String identity(JsonNode item, BuzzsproutResource resource)
            throws BuzzsproutReadException {
        if (!item.isObject()) {
            throw resource.failure("expected each list record to be a JSON object");
        }
        JsonNode id = item.path("id");
        if (!id.isIntegralNumber() || id.bigIntegerValue().signum() <= 0) {
            throw resource.failure("expected a positive integral provider id");
        }
        return id.asText();
    }

    private static String podcast(JsonNode item, String configured, BuzzsproutResource resource)
            throws BuzzsproutReadException {
        JsonNode id = item.get("podcast_id");
        if (id != null
                && !id.isNull()
                && (!(id.isTextual() || id.isIntegralNumber())
                        || !configured.equals(id.asText()))) {
            throw resource.failure("episode does not belong to the configured podcast");
        }
        return configured;
    }

    private static Instant publishedAt(JsonNode item, BuzzsproutResource resource)
            throws BuzzsproutReadException {
        JsonNode field = item.get("published_at");
        if (field == null || field.isNull()) {
            return null;
        }
        String value = field.textValue();
        try {
            if (value != null && value.length() <= 48) {
                return OffsetDateTime.parse(value).toInstant();
            }
        } catch (DateTimeException invalid) {
            // Do not include private episode metadata in the error.
        }
        throw resource.failure("record has no valid published_at timestamp");
    }

    private static String payload(JsonNode item, BuzzsproutResource resource)
            throws BuzzsproutReadException {
        try {
            return JSON.writeValueAsString(item);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not encode the provider record as JSON");
        }
    }

    private static void hashRecord(MessageDigest digest, String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }
}
