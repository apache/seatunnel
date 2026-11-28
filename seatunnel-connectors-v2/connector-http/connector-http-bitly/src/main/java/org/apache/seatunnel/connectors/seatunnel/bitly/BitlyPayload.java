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
package org.apache.seatunnel.connectors.seatunnel.bitly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bitly.BitlyWork.Stage;

import okhttp3.HttpUrl;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Validates a complete Bitly operation before exposing its rows or discovered descendants.
 *
 * <p>The original provider objects remain intact as JSON, including nested metrics and QR image
 * metadata. Each output identifies the parent instance from which it was read.
 */
final class BitlyPayload {
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
    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_-]{1,128}");
    private static final Pattern OFFSET = Pattern.compile("[+-][0-9]{4}$");
    private static final Pattern DOMAIN =
            Pattern.compile(
                    "(?i)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?");
    private static final Pattern LINK_KEY = Pattern.compile("[\\p{L}\\p{N}_~.-]{1,200}");

    final List<SeaTunnelRow> rows;
    final String fingerprint;
    final BitlySplit next;

    private BitlyPayload(List<SeaTunnelRow> rows, String fingerprint, BitlySplit next) {
        this.rows = rows;
        this.fingerprint = fingerprint;
        this.next = next;
    }

    static JsonNode parse(byte[] data, BitlyWork work) throws BitlyReadException {
        try {
            return JSON.readTree(data);
        } catch (IOException | RuntimeException failure) {
            throw work.failure("response is not valid JSON");
        }
    }

    /**
     * Retrieves one scoped result and validates the complete output and traversal transition.
     *
     * <p>The optional QR image is part of the same acquisition and never enters checkpoint state.
     */
    static BitlyPayload read(
            BitlyTransport client,
            BitlyConfig config,
            BitlySplit state,
            String tableId,
            long expires)
            throws BitlyReadException, InterruptedException {
        BitlyWork work = state.work.get(0);
        HttpUrl url = client.url(work);
        JsonNode root = object(client.get(url, work, expires), work);
        List<SeaTunnelRow> rows = new ArrayList<>();
        List<BitlyWork> children = new ArrayList<>();
        JsonNode image = null;

        switch (work.stage) {
            case BSDS:
                checkOptionalArray(root, "bsds", work);
                add(rows, work, tableId, "bsds", null, null, null, root);
                break;
            case CAMPAIGNS:
            case CHANNELS:
                datedCollection(root, work, config, rows, tableId, expires);
                break;
            case ORGANIZATIONS:
            case GROUPS:
                parents(root, work, config, children, rows, tableId, expires);
                break;
            case GROUP_PREFERENCES:
                if (!work.group.equals(guid(root.path("group_guid"), work))) {
                    throw work.failure("group preferences belong to a different group");
                }
                add(rows, work, tableId, work.group, work.org, work.group, null, root);
                break;
            case ORGANIZATION_COUNTS:
            case GROUP_COUNTS:
            case CLICKS:
                metrics(root, work, rows, tableId);
                break;
            case BITLINKS:
            case QR_LIST:
                paginated(root, work, url, config, children, rows, tableId, expires);
                break;
            case QR_DETAIL:
                if (matchesQr(root, work, config)) {
                    if (state.used >= config.maxRequests - 1) {
                        throw work.failure("max_requests exhausted before QR image");
                    }
                    HttpUrl imageUrl =
                            url.newBuilder()
                                    .addPathSegment("image")
                                    .addQueryParameter("format", "svg")
                                    .build();
                    image = object(client.get(imageUrl, work, expires), work);
                    checkImage(image.path("qr_code_image"), work);
                    ObjectNode value = JSON.createObjectNode();
                    value.set("metadata", root);
                    value.set("image", image);
                    add(rows, work, tableId, work.qr, work.org, work.group, work.link, value);
                }
                break;
            case QR_LEGACY:
                String group = optionalGuid(root.path("group_guid"), work);
                if (group != null && !work.group.equals(group)) {
                    throw work.failure("legacy QR response belongs to a different group");
                }
                String id = guid(root.path("id"), work);
                if (config.inside(date(root.path("modified"), work))) {
                    add(rows, work, tableId, id, work.org, work.group, work.link, root);
                }
                break;
            default:
                throw new IllegalStateException("Unknown Bitly response stage");
        }

        String digest =
                BitlyConfig.digest(
                        encode(root, work) + (image == null ? "" : "\n" + encode(image, work)));
        if (state.fingerprint != null && !state.fingerprint.equals(digest)) {
            throw work.failure("provider page changed after checkpoint");
        }
        BitlySplit next = state.complete(children, image == null ? 1 : 2);
        BitlyTransport.deadline(work, expires);
        return new BitlyPayload(rows, digest, next);
    }

    private static void datedCollection(
            JsonNode root,
            BitlyWork work,
            BitlyConfig config,
            List<SeaTunnelRow> rows,
            String tableId,
            long expires)
            throws BitlyReadException {
        Set<String> ids = new HashSet<>();
        for (JsonNode value : array(root, work.stage.resource.name, work)) {
            BitlyTransport.deadline(work, expires);
            JsonNode item = record(value, work);
            String id = guid(item.path("guid"), work);
            unique(ids, id, work);
            if (config.inside(date(item.path("modified"), work))) {
                String group = optionalGuid(item.path("group_guid"), work);
                add(rows, work, tableId, id, null, group, null, item);
            }
        }
    }

    private static void parents(
            JsonNode root,
            BitlyWork work,
            BitlyConfig config,
            List<BitlyWork> children,
            List<SeaTunnelRow> rows,
            String tableId,
            long expires)
            throws BitlyReadException {
        Set<String> ids = new HashSet<>();
        for (JsonNode value : array(root, work.stage.resource.name, work)) {
            BitlyTransport.deadline(work, expires);
            JsonNode item = record(value, work);
            String id = guid(item.path("guid"), work);
            unique(ids, id, work);
            if (work.stage == Stage.ORGANIZATIONS) {
                if (config.includes(BitlyResource.ORGANIZATIONS)
                        && config.inside(date(item.path("modified"), work))) {
                    add(rows, work, tableId, id, id, null, null, item);
                }
                if (config.includes(BitlyResource.ORGANIZATION_COUNTS)) {
                    children.add(new BitlyWork(Stage.ORGANIZATION_COUNTS, id, null, null, null));
                }
                if (config.groupsNeeded()) {
                    children.add(new BitlyWork(Stage.GROUPS, id, null, null, null));
                }
            } else {
                String claimedOrg = optionalGuid(item.path("organization_guid"), work);
                if (claimedOrg != null && !work.org.equals(claimedOrg)) {
                    throw work.failure("group belongs to a different organization");
                }
                if (config.includes(BitlyResource.GROUPS)
                        && config.inside(date(item.path("modified"), work))) {
                    add(rows, work, tableId, id, work.org, id, null, item);
                }
                for (Stage stage : List.of(Stage.GROUP_PREFERENCES, Stage.GROUP_COUNTS)) {
                    if (config.includes(stage.resource)) {
                        children.add(new BitlyWork(stage, work.org, id, null, null));
                    }
                }
                if (config.bitlinksNeeded()) {
                    children.add(new BitlyWork(Stage.BITLINKS, work.org, id, null, null));
                }
                if (config.includes(BitlyResource.QR_CODES) && !config.legacyQr) {
                    children.add(new BitlyWork(Stage.QR_LIST, work.org, id, null, null));
                }
            }
            if (children.size() > BitlySplit.MAX_PENDING) {
                throw work.failure("account fan-out exceeds the pending work limit");
            }
        }
    }

    private static void metrics(
            JsonNode root, BitlyWork work, List<SeaTunnelRow> rows, String tableId)
            throws BitlyReadException {
        boolean clicks = work.stage == Stage.CLICKS;
        checkOptionalArray(root, clicks ? "link_clicks" : "metrics", work);
        String id = clicks ? work.link : (work.group == null ? work.org : work.group);
        add(rows, work, tableId, id, work.org, work.group, work.link, root);
    }

    private static void paginated(
            JsonNode root,
            BitlyWork work,
            HttpUrl url,
            BitlyConfig config,
            List<BitlyWork> children,
            List<SeaTunnelRow> rows,
            String tableId,
            long expires)
            throws BitlyReadException {
        boolean links = work.stage == Stage.BITLINKS;
        JsonNode items = array(root, links ? "links" : "qr_codes", work);
        if (items.size() > config.pageSize) {
            throw work.failure("provider returned more items than requested");
        }
        Set<String> ids = new TreeSet<>();
        for (JsonNode value : items) {
            BitlyTransport.deadline(work, expires);
            JsonNode item = record(value, work);
            if (links) {
                String group = optionalGuid(item.path("group_guid"), work);
                if (group != null && !work.group.equals(group)) {
                    throw work.failure("Bitlink belongs to a different group");
                }
                String id = bitlink(item.path("id"), work);
                unique(ids, id, work);
                if (config.includes(BitlyResource.BITLINKS)
                        && config.inside(date(item.path("created_at"), work))) {
                    add(rows, work, tableId, id, work.org, work.group, id, item);
                }
                if (config.includes(BitlyResource.CLICKS)) {
                    children.add(new BitlyWork(Stage.CLICKS, work.org, work.group, id, null));
                }
                if (config.includes(BitlyResource.QR_CODES) && config.legacyQr) {
                    children.add(new BitlyWork(Stage.QR_LEGACY, work.org, work.group, id, null));
                }
            } else {
                String id = guid(item.path("qrcode_id"), work);
                String group = optionalGuid(item.path("group_guid"), work);
                if (group != null && !work.group.equals(group)) {
                    throw work.failure("QR list contains a code from another group");
                }
                String link = bitlink(item.path("bitlink_id"), work);
                unique(ids, id, work);
                children.add(new BitlyWork(Stage.QR_DETAIL, work.org, work.group, link, id));
            }
        }

        String identity =
                ids.isEmpty() ? null : "page:" + BitlyConfig.digest(String.join("\n", ids));
        if (identity != null && work.history.contains(identity)) {
            throw work.failure("provider repeated a page for this parent");
        }
        String cursor = continuation(root.path("pagination"), work, url);
        if (cursor != null) {
            if (work.page + 1 >= config.maxPages) {
                throw work.failure("max_pages_per_parent exhausted before terminal continuation");
            }
            children.add(work.next(cursor, identity, ids));
        }
    }

    private static String continuation(JsonNode value, BitlyWork work, HttpUrl request)
            throws BitlyReadException {
        if (!value.isObject()) {
            throw work.failure("pagination object is missing or invalid");
        }
        String cursor = optionalCursor(value.path("search_after"), work);
        String next = optionalCursor(value.path("next"), work);
        if (next == null) {
            return cursor;
        }
        HttpUrl target = request.resolve(next);
        if (target == null
                || !target.scheme().equals(request.scheme())
                || !target.host().equals(request.host())
                || target.port() != request.port()
                || !target.encodedPath().equals(request.encodedPath())
                || !target.username().isEmpty()
                || !target.password().isEmpty()
                || target.fragment() != null) {
            throw work.failure("pagination link does not match the API request scope");
        }
        for (String name : target.queryParameterNames()) {
            List<String> values = target.queryParameterValues(name);
            if (values.size() != 1
                    || (!"search_after".equals(name)
                            && !values.equals(request.queryParameterValues(name)))) {
                throw work.failure("pagination link changes the request parameters");
            }
        }
        String nextCursor = validateCursor(target.queryParameter("search_after"), work);
        if (nextCursor == null || (cursor != null && !cursor.equals(nextCursor))) {
            throw work.failure("pagination link and continuation cursor disagree");
        }
        return nextCursor;
    }

    private static String optionalCursor(JsonNode value, BitlyWork work) throws BitlyReadException {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw work.failure("invalid pagination token or link");
        }
        return validateCursor(value.textValue(), work);
    }

    private static String validateCursor(String value, BitlyWork work) throws BitlyReadException {
        if (value == null || value.isEmpty()) {
            return null;
        }
        if (value.isBlank()
                || value.length() > 4096
                || value.chars().anyMatch(Character::isISOControl)) {
            throw work.failure("invalid pagination token or link");
        }
        return value;
    }

    private static boolean matchesQr(JsonNode response, BitlyWork work, BitlyConfig config)
            throws BitlyReadException {
        if (!work.qr.equals(guid(response.path("qrcode_id"), work))
                || !work.group.equals(guid(response.path("group_guid"), work))
                || !work.link.equals(bitlink(response.path("bitlink_id"), work))) {
            throw work.failure("QR detail does not match the discovered group and Bitlink");
        }
        return config.inside(date(response.path("modified"), work));
    }

    private static void checkImage(JsonNode field, BitlyWork work) throws BitlyReadException {
        if (!field.isTextual()
                || !field.textValue().startsWith("data:image/svg+xml;base64,")
                || field.textValue().length() <= "data:image/svg+xml;base64,".length()) {
            throw work.failure("QR image response is not inline SVG data");
        }
    }

    private static void unique(Set<String> seen, String id, BitlyWork work)
            throws BitlyReadException {
        String hash = BitlyConfig.digest(id);
        if (!seen.add(hash) || work.previousIds.contains(hash)) {
            throw work.failure("duplicate record ID within or across adjacent pages");
        }
    }

    private static String guid(JsonNode id, BitlyWork work) throws BitlyReadException {
        String text = id.textValue();
        if (text == null || text.length() > 128 || !SAFE_ID.matcher(text).matches()) {
            throw work.failure("invalid provider identifier");
        }
        return text;
    }

    private static String optionalGuid(JsonNode id, BitlyWork work) throws BitlyReadException {
        return id.isMissingNode() || id.isNull() ? null : guid(id, work);
    }

    private static String bitlink(JsonNode id, BitlyWork work) throws BitlyReadException {
        String text = id.textValue();
        if (text != null && text.length() <= 454) {
            int slash = text.indexOf('/');
            if (slash > 0 && slash <= 253 && slash == text.lastIndexOf('/')) {
                String domain = text.substring(0, slash);
                String key = text.substring(slash + 1);
                if (DOMAIN.matcher(domain).matches()
                        && !".".equals(key)
                        && !"..".equals(key)
                        && LINK_KEY.matcher(key).matches()) {
                    return text;
                }
            }
        }
        throw work.failure("invalid Bitlink domain and hash");
    }

    private static Instant date(JsonNode field, BitlyWork work) throws BitlyReadException {
        String value = field.textValue();
        if (value != null && value.length() >= 20 && value.length() <= 40) {
            try {
                if (OFFSET.matcher(value).find()) {
                    value =
                            value.substring(0, value.length() - 2)
                                    + ":"
                                    + value.substring(value.length() - 2);
                }
                return OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeException failure) {
                // Provider date text is not safe to include in the diagnostic.
            }
        }
        throw work.failure("response is missing a valid provider timestamp");
    }

    private static JsonNode object(JsonNode value, BitlyWork work) throws BitlyReadException {
        if (value == null
                || !value.isObject()
                || value.hasNonNull("message")
                || value.hasNonNull("error")
                || (value.hasNonNull("errors") && !value.path("errors").isEmpty())
                || (value.has("success") && !value.path("success").asBoolean(false))) {
            throw work.failure("expected a successful Bitly JSON object");
        }
        return value;
    }

    private static JsonNode record(JsonNode value, BitlyWork work) throws BitlyReadException {
        if (!value.isObject()) {
            throw work.failure("resource array contains a non-object record");
        }
        return value;
    }

    private static JsonNode array(JsonNode value, String field, BitlyWork work)
            throws BitlyReadException {
        JsonNode array = value.path(field);
        if (!array.isArray() || array.size() > BitlySplit.MAX_PENDING) {
            throw work.failure("missing or oversized " + field + " array");
        }
        return array;
    }

    private static void checkOptionalArray(JsonNode value, String field, BitlyWork work)
            throws BitlyReadException {
        if (!value.path(field).isNull() && !value.path(field).isArray()) {
            throw work.failure("expected the " + field + " array or null");
        }
    }

    private static void add(
            List<SeaTunnelRow> rows,
            BitlyWork work,
            String tableId,
            String id,
            String org,
            String group,
            String link,
            JsonNode item)
            throws BitlyReadException {
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            work.stage.resource.name, id, org, group, link, encode(item, work)
                        });
        row.setTableId(tableId);
        rows.add(row);
    }

    private static String encode(JsonNode item, BitlyWork work) throws BitlyReadException {
        try {
            return JSON.writeValueAsString(item);
        } catch (IOException | RuntimeException failure) {
            throw work.failure("could not encode Bitly response data");
        }
    }
}
