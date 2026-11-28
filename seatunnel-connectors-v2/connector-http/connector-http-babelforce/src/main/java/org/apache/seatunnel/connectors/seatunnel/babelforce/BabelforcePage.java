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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.net.URI;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validates a simple call report page before making any of its data visible.
 *
 * <p>Pagination follows 1-based advertised pages. An optional next pointer is checked as an
 * assertion only. The requester always constructs the authoritative URL locally.
 */
final class BabelforcePage {
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
    final BabelforceCheckpoint next;

    private BabelforcePage(List<SeaTunnelRow> rows, String digest, BabelforceCheckpoint next) {
        this.rows = rows;
        this.digest = digest;
        this.next = next;
    }

    /**
     * Checks the report envelope and creation window and retains exact JSON payloads.
     *
     * <p>A provider that ignores the chosen date parameters must fail instead of silently returning
     * a claimed bounded snapshot across unrelated calls.
     */
    static BabelforcePage read(
            byte[] bytes,
            BabelforceOptions options,
            BabelforceCheckpoint current,
            String tableId,
            long deadline)
            throws BabelforceException {
        JsonNode root;
        try {
            root = JSON.readTree(bytes);
        } catch (IOException | RuntimeException invalid) {
            throw failure(current, "invalid JSON response");
        }
        JsonNode items = validatedItems(root, current);
        int count = items.size();
        if (count > options.pageSize) {
            throw failure(current, "provider returned more rows than requested per page");
        }
        String digest = BabelforceOptions.hash(encode(root, current));
        if (current.partialDigest != null && !current.partialDigest.equals(digest)) {
            throw failure(current, "provider response changed after checkpoint");
        }

        JsonNode pagination = root.path("pagination");
        int pages = pages(pagination, current);
        if (pages > options.maxPages) {
            throw failure(current, "advertised pages exceed max_pages");
        }
        long total = total(pagination, current);
        boolean last = current.page >= pages;
        validatePagination(pagination, options, current, count, pages, total, last);
        List<SeaTunnelRow> rows = new ArrayList<>(count);
        Set<String> identifiers = new TreeSet<>();
        for (JsonNode item : items) {
            BabelforceTransport.deadline(current, deadline);
            String id = identifier(item.path("id"), current, "id");
            if (!item.isObject() || !identifiers.add(BabelforceOptions.hash(id))) {
                throw failure(current, "call items must be objects with unique IDs");
            }
            JsonNode lineage = item.path("parentId");
            String parent =
                    lineage.isMissingNode() || lineage.isNull()
                            ? null
                            : identifier(lineage, current, "parentId");
            Instant date = creation(item.path("dateCreated"), current);
            if (date.getEpochSecond() < options.from || date.getEpochSecond() > current.until) {
                throw failure(
                        current, "dateCreated falls outside the requested Unix-second window");
            }
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {id, parent, date.toString(), encode(item, current)});
            row.setTableId(tableId);
            rows.add(row);
        }
        String identity =
                count == 0 ? null : BabelforceOptions.hash(String.join("\n", identifiers));
        if (identity != null && current.pageKeys.contains(identity)) {
            throw failure(current, "provider repeated a previously read page");
        }
        BabelforceTransport.deadline(current, deadline);
        return new BabelforcePage(
                rows, digest, current.advance(count, pages, total, identity, last));
    }

    private static JsonNode validatedItems(JsonNode root, BabelforceCheckpoint current)
            throws BabelforceException {
        if (root == null || !root.isObject()) {
            throw failure(current, "expected an object containing the calls items array");
        }
        JsonNode errors = root.path("errors");
        if (root.hasNonNull("error")
                || (root.hasNonNull("errors") && (!errors.isArray() || !errors.isEmpty()))
                || (root.has("success")
                        && (!root.path("success").isBoolean()
                                || !root.path("success").booleanValue()))) {
            throw failure(current, "provider reported a failed call-report envelope");
        }
        JsonNode records = root.path("items");
        JsonNode pagination = root.path("pagination");
        if (!records.isArray()
                || (!pagination.isMissingNode()
                        && !pagination.isNull()
                        && !pagination.isObject())) {
            throw failure(current, "invalid items or pagination in the call report");
        }
        return records;
    }

    private static int pages(JsonNode pagination, BabelforceCheckpoint current)
            throws BabelforceException {
        long announced = optionalNumber(pagination, "pages", current);
        if (announced > Integer.MAX_VALUE
                || (announced >= 0 && current.pages >= 0 && announced != current.pages)) {
            throw failure(current, "provider pagination page count changed or is invalid");
        }
        if (announced >= 0) {
            return (int) announced;
        }
        return current.pages < 0 ? 1 : current.pages;
    }

    private static long total(JsonNode pagination, BabelforceCheckpoint current)
            throws BabelforceException {
        long announced = optionalNumber(pagination, "total", current);
        if (announced >= 0 && current.total >= 0 && announced != current.total) {
            throw failure(current, "provider pagination total changed");
        }
        return announced < 0 ? current.total : announced;
    }

    private static void validatePagination(
            JsonNode pagination,
            BabelforceOptions options,
            BabelforceCheckpoint current,
            int count,
            int pages,
            long total,
            boolean last)
            throws BabelforceException {
        long serverPage = optionalNumber(pagination, "current", current);
        long serverMax = optionalNumber(pagination, "max", current);
        boolean emptyWindow = pages == 0 && current.page == 1 && count == 0;
        if ((!emptyWindow && current.page > pages)
                || (serverPage >= 0
                        && serverPage != current.page
                        && !(emptyWindow && serverPage == 0))
                || (serverMax >= 0 && (serverMax < count || serverMax > options.pageSize))) {
            throw failure(current, "contradictory pagination position or size");
        }
        long delivered = current.received + count;
        if (total >= 0 && (delivered > total || (last && delivered != total))) {
            throw failure(current, "records do not match the provider pagination total");
        }
        JsonNode next = pagination.path("next");
        if (next.isMissingNode()
                || next.isNull()
                || (next.isTextual() && next.textValue().isBlank())) {
            return;
        }
        if (last
                || !next.isTextual()
                || !nextMatches(next.textValue(), options.origin, current.page + 1)) {
            throw failure(current, "contradictory or unsafe pagination continuation");
        }
    }

    private static boolean nextMatches(String next, String origin, int expected) {
        if (next.equals(Integer.toString(expected))) {
            return true;
        }
        try {
            if (next.length() > 4096) {
                return false;
            }
            URI link = URI.create(next);
            URI endpoint = URI.create(origin);
            if (link.getRawFragment() != null || link.getRawUserInfo() != null) {
                return false;
            }
            if (link.isAbsolute()) {
                int port = link.getPort() == -1 ? 443 : link.getPort();
                int wanted = endpoint.getPort() == -1 ? 443 : endpoint.getPort();
                if (!endpoint.getScheme().equalsIgnoreCase(link.getScheme())
                        || !endpoint.getHost().equalsIgnoreCase(link.getHost())
                        || port != wanted) {
                    return false;
                }
            } else if (link.getRawAuthority() != null) {
                return false;
            }
            String path = link.getRawPath();
            if (path == null || (!path.isEmpty() && !BabelforceTransport.CALL_ROUTE.equals(path))) {
                return false;
            }
            if (link.getRawQuery() == null) {
                return false;
            }
            String actual = null;
            for (String param : link.getRawQuery().split("&")) {
                if (param.startsWith("page=")) {
                    if (actual != null) {
                        return false;
                    }
                    actual = param.substring(5);
                }
            }
            return Objects.equals(Integer.toString(expected), actual);
        } catch (IllegalArgumentException invalid) {
            // A provider-controlled continuation can contain a URL or credentials.
            return false;
        }
    }

    private static long optionalNumber(JsonNode parent, String name, BabelforceCheckpoint current)
            throws BabelforceException {
        JsonNode raw = parent.path(name);
        if (raw.isMissingNode() || raw.isNull()) {
            return -1;
        }
        if (!raw.isIntegralNumber() || !raw.canConvertToLong() || raw.longValue() < 0) {
            throw failure(current, "invalid pagination field " + name);
        }
        return raw.longValue();
    }

    private static String identifier(JsonNode value, BabelforceCheckpoint current, String name)
            throws BabelforceException {
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > 256
                || value.textValue().chars().anyMatch(Character::isISOControl)) {
            throw failure(current, "missing or invalid call field " + name);
        }
        return value.textValue();
    }

    private static Instant creation(JsonNode value, BabelforceCheckpoint current)
            throws BabelforceException {
        try {
            if (value.isIntegralNumber() && value.canConvertToLong()) {
                return Instant.ofEpochSecond(value.longValue());
            }
            if (value.isTextual() && value.textValue().length() <= 40) {
                String original = value.textValue();
                if (original.matches("-?[0-9]{1,18}")) {
                    return Instant.ofEpochSecond(Long.parseLong(original));
                }
                String time =
                        original.matches(".*[+-][0-9]{4}$")
                                ? original.substring(0, original.length() - 2)
                                        + ":"
                                        + original.substring(original.length() - 2)
                                : original;
                try {
                    return OffsetDateTime.parse(time).toInstant();
                } catch (DateTimeException withoutOffset) {
                    return LocalDateTime.parse(time).toInstant(ZoneOffset.UTC);
                }
            }
        } catch (DateTimeException | NumberFormatException invalid) {
            // Call metadata is never included in a timestamp diagnostic.
        }
        throw failure(current, "dateCreated is not a supported timestamp");
    }

    private static String encode(JsonNode value, BabelforceCheckpoint current)
            throws BabelforceException {
        try {
            return JSON.writeValueAsString(value);
        } catch (IOException | RuntimeException invalid) {
            throw failure(current, "could not encode call JSON");
        }
    }

    private static BabelforceException failure(BabelforceCheckpoint current, String reason) {
        return new BabelforceException(current.page, reason);
    }
}
