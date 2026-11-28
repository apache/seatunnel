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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Resource;
import org.apache.seatunnel.connectors.seatunnel.boldsign.BoldSignConfig.Window;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates provider envelopes and retains record values in their native JSON representations.
 *
 * <p>No response bodies or request paths are included in diagnostics.
 */
final class BoldSignJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(8 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private BoldSignJson() {}

    static JsonNode parse(byte[] bytes) throws IOException {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
        } catch (CharacterCodingException e) {
            throw failure("response is not valid UTF-8 JSON");
        }
        try {
            JsonNode node = JSON.readTree(bytes);
            if (node != null && node.isObject()) {
                return node;
            }
        } catch (IOException | RuntimeException ignored) {
            // Do not propagate response data as part of a parse error.
        }
        throw failure("response is not a valid BoldSign JSON object");
    }

    /**
     * Reads the endpoint's actual array and paging shape.
     *
     * <p>Brand and custom-field lists have no pageDetails. Teams use the plural array.
     */
    static RawPage page(JsonNode root, Resource resource, int page, int size, int maxUnpaged)
            throws IOException {
        JsonNode rows = root.path(resource.member);
        int cap = resource.paged ? size : maxUnpaged;
        if (root.has("error") || !rows.isArray() || rows.size() > cap) {
            throw failure("invalid or oversized " + resource.member + " collection");
        }
        List<JsonNode> records = new ArrayList<>(rows.size());
        for (JsonNode row : rows) {
            if (!row.isObject()) {
                throw failure("response collection contains a non-object record");
            }
            records.add(row);
        }
        if (!resource.paged) {
            return new RawPage(records, false);
        }
        JsonNode metadata = root.path("pageDetails");
        if (!metadata.isObject()
                || !matches(metadata.path("page"), page)
                || !matches(metadata.path("pageSize"), size)) {
            throw failure("response pageDetails does not match the requested page and size");
        }
        Long total = integral(metadata.path("totalRecordsCount"), "totalRecordsCount");
        Long pages = integral(metadata.path("totalPages"), "totalPages");
        if ((total != null && total < 0) || (pages != null && pages < 0)) {
            throw failure("invalid negative pageDetails count");
        }
        long end = (long) page * size;
        boolean more =
                total == null && pages == null
                        ? records.size() == size
                        : (total != null && end < total) || (pages != null && page < pages);
        if (records.isEmpty() && more) {
            throw failure("empty numbered page has contradictory continuation metadata");
        }
        return new RawPage(records, more);
    }

    private static boolean matches(JsonNode node, int requested) {
        return node.isIntegralNumber() && node.canConvertToInt() && node.intValue() == requested;
    }

    static List<String> brands(List<JsonNode> records) throws IOException {
        List<String> result = new ArrayList<>(records.size());
        Set<String> distinct = new HashSet<>();
        for (JsonNode row : records) {
            String id = id(row.path("brandId"), "brandId");
            if (!distinct.add(id)) {
                throw failure("duplicate brand ID in discovery");
            }
            result.add(id);
        }
        return result;
    }

    static String brand(JsonNode row, Resource resource, String selectedBrand) throws IOException {
        if (resource == Resource.BRANDS) {
            return id(row.path("brandId"), "brandId");
        }
        if (resource == Resource.CUSTOM_FIELDS
                && row.hasNonNull("brandId")
                && !selectedBrand.equals(id(row.path("brandId"), "brandId"))) {
            throw failure("custom field brand does not match its requested brand");
        }
        return selectedBrand;
    }

    static String id(JsonNode field, String name) throws IOException {
        if (!field.isTextual() || !BoldSignConfig.validId(field.textValue())) {
            throw failure("invalid " + name + " in response");
        }
        return field.textValue();
    }

    static Long documentCursor(JsonNode row) throws IOException {
        Long current = integral(row.path("cursor"), "document cursor");
        Long alias = integral(row.path("nextCursor"), "document nextCursor");
        if (current != null && alias != null && !current.equals(alias)) {
            throw failure("conflicting document cursor fields");
        }
        return current == null ? alias : current;
    }

    /**
     * Applies only the local creation-time window, independently of the provider's transport dates.
     *
     * <p>Missing timestamps are valid in an unfiltered read and ambiguous within a selected window.
     */
    static boolean selected(JsonNode row, Window window) throws IOException {
        Long created = integral(row.path("createdDate"), "createdDate");
        if (window.from == null && window.until == null) {
            return true;
        }
        if (created == null) {
            throw failure("createdDate is required when a creation window is configured");
        }
        return (window.from == null || created >= window.from)
                && (window.until == null || created <= window.until);
    }

    private static Long integral(JsonNode node, String field) throws IOException {
        if (node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isIntegralNumber() && node.canConvertToLong()) {
            return node.longValue();
        }
        throw failure("invalid signed 64-bit " + field + " in response");
    }

    static String encode(JsonNode row) throws IOException {
        try {
            return JSON.writeValueAsString(row);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a provider record");
        }
    }

    static String fingerprint(List<String> items, String continuation) {
        MessageDigest digest = sha256();
        frame(digest, continuation);
        for (String item : items) {
            frame(digest, item);
        }
        return hex(digest.digest());
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    static String digest(String value) {
        return hex(sha256().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] & 0xff) >>> 4];
            result[i * 2 + 1] = digits[bytes[i] & 0xf];
        }
        return new String(result);
    }

    static Failure failure(String reason) {
        return new Failure("BoldSign: " + reason);
    }

    /**
     * A locally defined failure class safe for job diagnostics.
     *
     * <p>It carries neither payload data nor details of credentials and record IDs.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }

    /**
     * Validated provider rows and the numbered-page continuation indication.
     *
     * <p>Recovery and row filtering are decided separately from the provider envelope.
     */
    static final class RawPage {
        final List<JsonNode> rows;
        final boolean more;

        RawPage(List<JsonNode> rows, boolean more) {
            this.rows = rows;
            this.more = more;
        }
    }
}
