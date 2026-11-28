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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validated array response and its next immutable resource position.
 *
 * <p>Records outside the requested update window still contribute to raw offsets and ordering.
 */
final class BlueTallyPage {
    private static final ObjectMapper MAPPER =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    final List<SeaTunnelRow> rows;
    final String fingerprint;
    final BlueTallySplit next;

    private BlueTallyPage(List<SeaTunnelRow> rows, String fingerprint, BlueTallySplit next) {
        this.rows = rows;
        this.fingerprint = fingerprint;
        this.next = next;
    }

    static JsonNode parse(byte[] content, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        try {
            return MAPPER.readTree(content);
        } catch (IOException | RuntimeException failure) {
            throw fail(resource, offset, "response is not valid JSON");
        }
    }

    /**
     * Checks one page's type, identity, tenant and requested descending update order.
     *
     * <p>The raw empty array is the only terminal response. The scan never jumps over records that
     * share an update timestamp and does not infer completion from a short output page.
     */
    static BlueTallyPage verify(
            JsonNode data,
            BlueTallyConfig options,
            BlueTallySplit state,
            BlueTallyResource resource,
            String tableId,
            long expires)
            throws BlueTallyReadException {
        int offset = state.offset;
        if (data == null || !data.isArray() || data.size() > options.pageSize) {
            throw fail(resource, offset, "expected a BlueTally array within the requested limit");
        }
        String fingerprint = BlueTallyConfig.digest(encode(data, resource, offset));
        if (state.partialFingerprint != null && !state.partialFingerprint.equals(fingerprint)) {
            throw fail(resource, offset, "provider page changed after checkpoint");
        }
        if (data.isEmpty()) {
            return new BlueTallyPage(List.of(), fingerprint, state.finishResource());
        }

        Set<String> ids = new TreeSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>(data.size());
        Instant previous = state.lastUpdated;
        for (JsonNode item : data) {
            BlueTallyTransport.deadline(resource, offset, expires);
            if (!item.isObject()) {
                throw fail(resource, offset, "list contains a non-object record");
            }
            String id = recordId(item.path("id"), resource, offset);
            String key = BlueTallyConfig.digest(id);
            if (!ids.add(key) || state.previousIds.contains(key)) {
                throw fail(resource, offset, "duplicate record ID in the same or adjacent page");
            }
            verifyTenant(item.path("tenant_id"), options.tenant, resource, offset);
            Instant updated = updated(item.path("updated_at"), resource, offset);
            if (previous != null && updated.isAfter(previous)) {
                throw fail(resource, offset, "provider returned out-of-order updated_at values");
            }
            previous = updated;
            if (!updated.isBefore(options.start) && updated.isBefore(state.end)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.path,
                                    id,
                                    options.tenant,
                                    updated.toString(),
                                    encode(item, resource, offset)
                                });
                row.setTableId(tableId);
                rows.add(row);
            }
        }

        String pageIdentity = BlueTallyConfig.digest(String.join("\n", ids));
        if (state.seenPages.contains(pageIdentity)) {
            throw fail(resource, offset, "provider repeated a resource page");
        }
        BlueTallyTransport.deadline(resource, offset, expires);
        return new BlueTallyPage(
                rows, fingerprint, state.next(data.size(), previous, pageIdentity, ids));
    }

    private static String recordId(JsonNode field, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        if (!field.isIntegralNumber()) {
            throw fail(resource, offset, "record has no integer system id");
        }
        return field.asText();
    }

    private static void verifyTenant(
            JsonNode claimed, Integer configured, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        if (configured != null
                && !claimed.isMissingNode()
                && !claimed.isNull()
                && !((claimed.isIntegralNumber() || claimed.isTextual())
                        && configured.toString().equals(claimed.asText()))) {
            throw fail(resource, offset, "provider row is outside the configured tenant");
        }
    }

    private static Instant updated(JsonNode field, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        String value = field.textValue();
        if (value != null && value.length() >= 20 && value.length() <= 40) {
            try {
                return OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeException failure) {
                // Provider timestamp text may include sensitive data.
            }
        }
        throw fail(resource, offset, "record has no valid updated_at timestamp");
    }

    private static String encode(JsonNode item, BlueTallyResource resource, int offset)
            throws BlueTallyReadException {
        try {
            return MAPPER.writeValueAsString(item);
        } catch (IOException | RuntimeException failure) {
            throw fail(resource, offset, "could not encode the BlueTally response");
        }
    }

    private static BlueTallyReadException fail(
            BlueTallyResource resource, int offset, String reason) {
        return new BlueTallyReadException(resource, offset, reason);
    }
}
