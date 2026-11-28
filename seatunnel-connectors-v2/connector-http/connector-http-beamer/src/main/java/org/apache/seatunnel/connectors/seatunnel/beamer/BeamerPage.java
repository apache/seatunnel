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
package org.apache.seatunnel.connectors.seatunnel.beamer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A verified NPS page and the position committed after publishing its rows.
 *
 * <p>The provider page is validated as a whole before any records reach the collector.
 */
final class BeamerPage {
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
    final String digest;
    final BeamerSplit next;

    private BeamerPage(List<SeaTunnelRow> rows, String digest, BeamerSplit next) {
        this.rows = rows;
        this.digest = digest;
        this.next = next;
    }

    /**
     * Parses a successful response without exposing upstream JSON or parser excerpts.
     *
     * <p>An empty HTTP body is handled later as an invalid NPS response.
     */
    static JsonNode parse(byte[] bytes, int page) throws BeamerReadException {
        try {
            return JSON.readTree(bytes);
        } catch (IOException | RuntimeException invalid) {
            throw fail(page, "response is not valid JSON");
        }
    }

    /**
     * Verifies the response shape, repeated identities and the previous checkpoint.
     *
     * <p>The wire date range is wider than the output range. Guard records still take part in
     * paging so a guard-only page cannot prematurely end the scan.
     */
    static BeamerPage verify(
            JsonNode data, BeamerConfig config, BeamerSplit current, String tableId, long deadline)
            throws BeamerReadException {
        if (data == null || !data.isArray() || data.size() > config.pageSize) {
            throw fail(current.page, "expected an NPS array within maxResults");
        }
        String digest = BeamerConfig.digest(encode(data, current.page));
        if (current.partialDigest != null && !current.partialDigest.equals(digest)) {
            throw fail(current.page, "provider page changed after checkpoint");
        }

        Set<String> ids = new HashSet<>();
        List<String> identities = new ArrayList<>(data.size());
        List<SeaTunnelRow> rows = new ArrayList<>(data.size());
        Instant queryStart = config.queryStart();
        Instant queryEnd = BeamerConfig.queryEnd(current.windowEnd);
        for (JsonNode item : data) {
            BeamerTransport.deadline(current.page, deadline);
            if (!item.isObject()) {
                throw fail(current.page, "NPS page contains a non-object record");
            }
            Instant date = created(item.path("date"), current.page);
            if (date.isBefore(queryStart) || date.isAfter(queryEnd)) {
                throw fail(current.page, "provider returned a date outside the request window");
            }
            String id = recordId(item.path("id"), current.page);
            String payload = encode(item, current.page);
            if (id == null) {
                // Anonymous responses have no stable ID. Their multiplicity is part of the page.
                identities.add("anonymous:" + BeamerConfig.digest(payload));
            } else {
                String key = BeamerConfig.digest(id);
                if (!ids.add(key) || current.previousIds.contains(key)) {
                    throw fail(current.page, "duplicate NPS response ID within adjacent pages");
                }
                identities.add("id:" + key);
            }
            if (!date.isBefore(config.start) && date.isBefore(current.windowEnd)) {
                SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, date.toString(), payload});
                row.setTableId(tableId);
                rows.add(row);
            }
        }
        BeamerTransport.deadline(current.page, deadline);
        if (data.isEmpty()) {
            return new BeamerPage(rows, digest, current.finish());
        }

        identities.sort(String::compareTo);
        String pageIdentity = BeamerConfig.digest(String.join("\n", identities));
        if (current.seenPages.contains(pageIdentity)) {
            throw fail(current.page, "provider repeated an NPS page");
        }
        return new BeamerPage(rows, digest, current.advance(pageIdentity, ids));
    }

    private static String recordId(JsonNode value, int page) throws BeamerReadException {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isTextual()) {
            String text = value.textValue();
            if (!text.isBlank()
                    && text.length() <= 512
                    && text.chars().noneMatch(Character::isISOControl)) {
                return text;
            }
        }
        throw fail(page, "invalid NPS response ID");
    }

    private static Instant created(JsonNode field, int page) throws BeamerReadException {
        if (field.isTextual()) {
            String value = field.textValue();
            try {
                if (value.length() <= 40 && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.+")) {
                    return OffsetDateTime.parse(value).toInstant();
                }
            } catch (DateTimeException invalid) {
                // The invalid feedback field is intentionally omitted from the exception.
            }
        }
        throw fail(page, "NPS response has no supported creation date");
    }

    private static String encode(JsonNode item, int page) throws BeamerReadException {
        try {
            return JSON.writeValueAsString(item);
        } catch (IOException | RuntimeException invalid) {
            throw fail(page, "could not encode NPS response");
        }
    }

    private static BeamerReadException fail(int page, String detail) {
        return new BeamerReadException(page, detail);
    }
}
