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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * One verified Awin response ready for ordered publication by the reader.
 *
 * <p>Only the envelope and documented derived fields are interpreted. Numeric, nullable and nested
 * data from every provider entry stays in the JSON payload.
 */
final class AwinRecords {
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
    final String responseDigest;
    final AwinCursor next;

    private AwinRecords(List<SeaTunnelRow> rows, String responseDigest, AwinCursor next) {
        this.rows = rows;
        this.responseDigest = responseDigest;
        this.next = next;
    }

    /**
     * Validates the whole response and any resume digest before returning rows.
     *
     * <p>Rejected responses never advance the current window or publish a partial result.
     */
    static AwinRecords decode(
            byte[] body,
            AwinSlice slice,
            AwinSettings settings,
            AwinCursor cursor,
            String tableId,
            long deadline)
            throws AwinReadException {
        JsonNode root;
        try {
            root = JSON.readTree(body);
        } catch (IOException | RuntimeException failure) {
            throw new AwinReadException(slice, "invalid JSON response");
        }
        if (root == null) {
            throw new AwinReadException(slice, "missing JSON response");
        }
        String digest = AwinSettings.hash(encode(root, slice));
        if (cursor.digest != null && !cursor.digest.equals(digest)) {
            throw new AwinReadException(slice, "provider response changed after checkpoint");
        }
        JsonNode list = entries(root, slice);
        if (!list.isArray()) {
            throw new AwinReadException(slice, "response is missing its expected records array");
        }
        List<SeaTunnelRow> rows = new ArrayList<>(list.size());
        Set<String> keys = new HashSet<>();
        for (JsonNode entry : list) {
            AwinTransport.checkDeadline(slice, deadline);
            if (!entry.isObject()) {
                throw new AwinReadException(slice, "report entries must be objects");
            }
            SeaTunnelRow row = toRow((ObjectNode) entry, slice, settings);
            if (!keys.add((String) row.getField(1))) {
                throw new AwinReadException(slice, "response repeated a complete record identity");
            }
            row.setTableId(tableId);
            rows.add(row);
        }
        AwinTransport.checkDeadline(slice, deadline);
        return new AwinRecords(rows, digest, cursor.advance(slice, settings));
    }

    private static JsonNode entries(JsonNode root, AwinSlice slice) throws AwinReadException {
        checkEnvelope(root, slice);
        JsonNode body = root;
        if (root.isObject()
                && (root.has("statusCodeValue") || root.has("statusCode"))
                && root.has("body")) {
            body = root.path("body");
            checkEnvelope(body, slice);
        }
        if (slice.partition.resource == AwinResource.CAMPAIGN) {
            return body.path("result");
        }
        if (slice.partition.resource == AwinResource.ACCOUNTS) {
            return body.path("accounts");
        }
        return body;
    }

    private static void checkEnvelope(JsonNode root, AwinSlice slice) throws AwinReadException {
        if (!root.isObject()) {
            return;
        }
        JsonNode errors = root.path("errors");
        if (root.hasNonNull("error")
                || (root.hasNonNull("errors") && (!errors.isArray() || !errors.isEmpty()))
                || (root.has("success")
                        && !(root.path("success").isBoolean()
                                && root.path("success").booleanValue()))
                || (root.has("statusCodeValue")
                        && !(root.path("statusCodeValue").isIntegralNumber()
                                && root.path("statusCodeValue").canConvertToInt()
                                && root.path("statusCodeValue").intValue() == 200))
                || (root.has("statusCode")
                        && !root.path("statusCode").asText().matches("200( OK)?|OK"))) {
            throw new AwinReadException(
                    slice, "provider returned a failed or partial report envelope");
        }
    }

    private static SeaTunnelRow toRow(ObjectNode raw, AwinSlice slice, AwinSettings settings)
            throws AwinReadException {
        AwinResource resource = slice.partition.resource;
        String advertiser = settings.advertiser == null ? null : settings.advertiser.toString();
        String region = null;
        LocalDate date = null;
        String id;
        ObjectNode row = raw.deepCopy();
        if (resource != AwinResource.ACCOUNTS
                && raw.hasNonNull("advertiserId")
                && !advertiser.equals(identity(raw, "advertiserId", slice))) {
            throw new AwinReadException(slice, "record belongs to a different advertiser");
        }
        switch (resource) {
            case ACCOUNTS:
                if (raw.hasNonNull("accountType")
                        && !"advertiser".equalsIgnoreCase(raw.path("accountType").asText())) {
                    throw new AwinReadException(
                            slice, "accounts response contains a non-advertiser account");
                }
                id = identity(raw, "accountId", slice);
                advertiser = id;
                break;
            case PUBLISHERS:
                id = identity(raw, "id", slice);
                break;
            case TRANSACTIONS:
                id = identity(raw, "id", slice);
                date = transactionDate(raw, slice);
                row.put("is_deleted", "deleted".equals(raw.path("commissionStatus").asText()));
                break;
            case CAMPAIGN:
                date = reportDate(raw, slice, true);
                id =
                        key(
                                slice,
                                date.toString(),
                                identity(raw, "publisherId", slice),
                                optionalString(raw, "campaign", slice));
                break;
            case PUBLISHER_REPORT:
                date = reportDate(raw, slice, false);
                row.put("date", date.toString());
                region = optionalString(raw, "region", slice);
                id =
                        key(
                                slice,
                                date.toString(),
                                identity(raw, "publisherId", slice),
                                region,
                                optionalString(raw, "currency", slice));
                break;
            case CREATIVE:
                date = reportDate(raw, slice, false);
                region = slice.partition.region;
                if (raw.hasNonNull("region")
                        && !region.equals(optionalString(raw, "region", slice))) {
                    throw new AwinReadException(
                            slice, "creative result belongs to a different region");
                }
                row.put("date", date.toString());
                row.put("region", region);
                id =
                        key(
                                slice,
                                date.toString(),
                                region,
                                identity(raw, "creativeId", slice),
                                identity(raw, "publisherId", slice),
                                optionalString(raw, "currency", slice));
                break;
            default:
                throw new AwinReadException(slice, "missing resource record contract");
        }
        return new SeaTunnelRow(
                new Object[] {
                    resource.key,
                    id,
                    advertiser,
                    region,
                    date == null ? null : date.toString(),
                    encode(row, slice)
                });
    }

    private static LocalDate reportDate(ObjectNode row, AwinSlice slice, boolean required)
            throws AwinReadException {
        if (!required && !row.hasNonNull("date")) {
            return slice.first;
        }
        try {
            String raw = row.path("date").asText();
            if (raw.length() == 10) {
                LocalDate date = LocalDate.parse(raw);
                if (!date.isBefore(slice.first) && !date.isAfter(slice.last)) {
                    return date;
                }
            }
        } catch (DateTimeException invalid) {
            // Diagnostic omits the report entry.
        }
        throw new AwinReadException(slice, "report date is outside the requested UTC window");
    }

    private static LocalDate transactionDate(ObjectNode row, AwinSlice slice)
            throws AwinReadException {
        String input = row.path("transactionDate").asText();
        try {
            if (input.length() >= 19 && input.length() <= 40) {
                LocalDateTime time;
                try {
                    time =
                            OffsetDateTime.parse(input)
                                    .withOffsetSameInstant(ZoneOffset.UTC)
                                    .toLocalDateTime();
                } catch (DateTimeException noOffset) {
                    time = LocalDateTime.parse(input);
                }
                LocalDateTime second = time.truncatedTo(ChronoUnit.SECONDS);
                if (!second.isBefore(slice.first.atStartOfDay())
                        && !second.isAfter(slice.transactionEnd)) {
                    return time.toLocalDate();
                }
            }
        } catch (DateTimeException invalid) {
            // The full transaction is never included in diagnostics.
        }
        throw new AwinReadException(slice, "transactionDate is outside the requested UTC window");
    }

    private static String identity(ObjectNode row, String field, AwinSlice slice)
            throws AwinReadException {
        JsonNode value = row.path(field);
        String text = value.isTextual() || value.isIntegralNumber() ? value.asText() : "";
        if (text.isBlank()
                || text.length() > 256
                || text.chars().anyMatch(Character::isISOControl)) {
            throw new AwinReadException(slice, "missing or invalid record field " + field);
        }
        return text;
    }

    private static String optionalString(ObjectNode row, String field, AwinSlice slice)
            throws AwinReadException {
        if (!row.hasNonNull(field)) {
            return null;
        }
        JsonNode value = row.path(field);
        if (!value.isTextual() || value.textValue().length() > 4096) {
            throw new AwinReadException(slice, "invalid report dimension " + field);
        }
        return value.textValue();
    }

    private static String key(AwinSlice slice, String... dimensions) throws AwinReadException {
        ArrayNode out = JSON.createArrayNode();
        for (String dimension : dimensions) {
            out.add(dimension);
        }
        return encode(out, slice);
    }

    private static String encode(JsonNode node, AwinSlice slice) throws AwinReadException {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException invalid) {
            throw new AwinReadException(slice, "could not encode report data");
        }
    }
}
