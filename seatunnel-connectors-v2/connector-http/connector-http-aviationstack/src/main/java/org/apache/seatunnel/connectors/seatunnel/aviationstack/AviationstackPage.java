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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * A verified page and the cursor position available after all its rows are published.
 *
 * <p>The response advances by raw result count including filtered aircraft records. The reader
 * commits the next position only after the matching output has gone through the checkpoint lock.
 */
final class AviationstackPage {
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
    final AviationstackSplit next;

    private AviationstackPage(List<SeaTunnelRow> rows, String digest, AviationstackSplit next) {
        this.rows = rows;
        this.digest = digest;
        this.next = next;
    }

    /**
     * Parses the provider envelope without retaining response excerpts in parser errors.
     *
     * <p>The transport classifies an unparseable body using the HTTP status alone.
     */
    static JsonNode parse(byte[] bytes) {
        try {
            return JSON.readTree(bytes);
        } catch (IOException | RuntimeException invalid) {
            return null;
        }
    }

    /**
     * Verifies the echoed absolute position and stable result count before returning any rows.
     *
     * <p>This detects contradictory pages and obvious catalog movement. An offset API can still
     * reorder records while keeping the same total, so it cannot provide an isolated snapshot.
     */
    static AviationstackPage verify(
            JsonNode envelope,
            AviationstackConfig config,
            AviationstackSplit current,
            String tableId,
            long deadline)
            throws AviationstackReadException {
        AviationstackResource catalog = config.resources.get(current.resourceIndex);
        JsonNode pagination = envelope.path("pagination");
        JsonNode data = envelope.path("data");
        long offset = numeric(pagination, "offset");
        long limit = numeric(pagination, "limit");
        long count = numeric(pagination, "count");
        long total = numeric(pagination, "total");
        if (!data.isArray()
                || offset != current.offset
                || limit < 1
                || limit > config.pageSize
                || count != data.size()
                || count > limit
                || total < offset
                || count > total - offset) {
            throw fail(catalog, current, "contradictory pagination or data array");
        }
        if (current.total >= 0 && total != current.total) {
            throw fail(catalog, current, "provider catalog total changed during pagination");
        }
        if (count == 0 && offset < total) {
            throw fail(catalog, current, "empty page before the advertised total");
        }

        String digest = AviationstackConfig.digest(encode(envelope, catalog, current));
        if (current.partialDigest != null && !current.partialDigest.equals(digest)) {
            throw fail(catalog, current, "provider page changed after checkpoint");
        }
        Set<String> keys = new TreeSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>(data.size());
        for (JsonNode item : data) {
            AviationstackTransport.deadline(catalog, current.offset, deadline);
            String key = recordId(item, catalog, current);
            if (!keys.add(AviationstackConfig.digest(key))) {
                throw fail(catalog, current, "duplicate record ID within the page");
            }
            if (catalog != AviationstackResource.AIRPLANES
                    || insideDeliveryWindow(
                            item, config.start, current.windowEnd, catalog, current)) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {catalog.name, key, encode(item, catalog, current)});
                row.setTableId(tableId);
                rows.add(row);
            }
        }
        String keyDigest = AviationstackConfig.digest(String.join("\n", keys));
        if (count > 0 && current.seenPages.contains(keyDigest)) {
            throw fail(catalog, current, "provider repeated a catalog page");
        }
        AviationstackSplit next = current.advance(offset + count, total, keyDigest);
        AviationstackTransport.deadline(catalog, current.offset, deadline);
        return new AviationstackPage(rows, digest, next);
    }

    private static long numeric(JsonNode object, String name) {
        JsonNode value = object.path(name);
        return value.isIntegralNumber() && value.canConvertToLong() ? value.longValue() : -1L;
    }

    private static String recordId(
            JsonNode item, AviationstackResource catalog, AviationstackSplit state)
            throws AviationstackReadException {
        JsonNode id = item.path("id");
        String key = id.isTextual() || id.isIntegralNumber() ? id.asText() : null;
        if (!item.isObject()
                || key == null
                || key.isBlank()
                || key.length() > 256
                || key.chars().anyMatch(Character::isISOControl)) {
            throw fail(catalog, state, "catalog record is missing a usable id");
        }
        return key;
    }

    private static boolean insideDeliveryWindow(
            JsonNode item,
            Instant start,
            Instant end,
            AviationstackResource catalog,
            AviationstackSplit state)
            throws AviationstackReadException {
        if (start == null && end == null) {
            return true;
        }
        JsonNode field = item.path("delivery_date");
        if (field.isMissingNode() || field.isNull()) {
            return false;
        }
        if (!field.isTextual()) {
            throw fail(catalog, state, "delivery_date is not a supported provider date");
        }
        String value = field.textValue();
        try {
            if (value.length() < 10 || value.length() > 40) {
                throw fail(catalog, state, "delivery_date is not a supported provider date");
            }
            if (value.length() == 10) {
                Instant delivery = LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant();
                return within(delivery, start, end);
            }
            // Older responses use a space separator or an offset without the colon.
            String input =
                    value.charAt(10) == ' '
                            ? value.substring(0, 10) + "T" + value.substring(11)
                            : value;
            if (input.matches(".*[+-][0-9]{4}$")) {
                input =
                        input.substring(0, input.length() - 2)
                                + ":"
                                + input.substring(input.length() - 2);
            }
            return within(OffsetDateTime.parse(input).toInstant(), start, end);
        } catch (DateTimeException invalid) {
            throw fail(catalog, state, "delivery_date is not a supported provider date");
        }
    }

    private static boolean within(Instant value, Instant start, Instant end) {
        return (start == null || !value.isBefore(start)) && (end == null || !value.isAfter(end));
    }

    private static String encode(
            JsonNode item, AviationstackResource catalog, AviationstackSplit state)
            throws AviationstackReadException {
        try {
            return JSON.writeValueAsString(item);
        } catch (IOException | RuntimeException invalid) {
            throw fail(catalog, state, "could not encode catalog data as JSON");
        }
    }

    private static AviationstackReadException fail(
            AviationstackResource catalog, AviationstackSplit state, String detail) {
        return new AviationstackReadException(catalog, state.offset, detail);
    }
}
