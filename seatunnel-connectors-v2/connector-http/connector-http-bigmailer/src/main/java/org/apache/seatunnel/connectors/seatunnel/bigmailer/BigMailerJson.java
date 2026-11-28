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

package org.apache.seatunnel.connectors.seatunnel.bigmailer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

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
 * Decodes one BigMailer page and preserves dynamic record types and fields.
 *
 * <p>Provider response fragments are never included in exceptions.
 */
final class BigMailerJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(4 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private BigMailerJson() {}

    static JsonNode parse(byte[] bytes) throws IOException {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
        } catch (CharacterCodingException e) {
            throw failure("response is not valid UTF-8 JSON");
        }
        try {
            JsonNode root = JSON.readTree(bytes);
            if (root != null && root.isObject()) {
                return root;
            }
        } catch (IOException | RuntimeException ignored) {
            // The parser's message can contain response data.
        }
        throw failure("response is not a valid JSON page object");
    }

    /**
     * Validates the full page before it can emit rows or begin child discovery.
     *
     * <p>The provider leaves a cursor on some last pages. Only has_more permits another request.
     */
    static Page page(
            JsonNode root,
            BigMailerConfig.Resource resource,
            int pageSize,
            String brand,
            boolean discovering)
            throws IOException {
        JsonNode more = root.path("has_more");
        JsonNode data = root.path("data");
        if (!more.isBoolean() || !data.isArray() || data.size() > pageSize) {
            throw failure("invalid pagination response data or has_more");
        }
        JsonNode rawCursor = root.path("cursor");
        String next = more.asBoolean() && rawCursor.isTextual() ? rawCursor.textValue() : null;
        if (more.asBoolean() && !BigMailerConfig.validCursor(next)) {
            throw failure("has_more requires a valid continuation cursor");
        }
        List<Row> rows = new ArrayList<>(data.size());
        Set<String> pageBrands = new HashSet<>();
        MessageDigest digest = sha256();
        addDigest(digest, next == null ? "terminal" : "next:" + next);
        for (JsonNode record : data) {
            if (!record.isObject()) {
                throw failure("response data contains a non-object record");
            }
            String brandId = brand;
            if (resource == BigMailerConfig.Resource.BRANDS) {
                brandId = record.path("id").asText(null);
                if (!record.path("id").isTextual() || !BigMailerConfig.validBrand(brandId)) {
                    throw failure("invalid brand UUID in response");
                }
                if (!pageBrands.add(brandId.toLowerCase(java.util.Locale.ROOT))) {
                    throw failure("duplicate brand UUID within a page");
                }
            } else if (brand != null && record.hasNonNull("brand_id")) {
                JsonNode actual = record.path("brand_id");
                if (!actual.isTextual() || !brand.equalsIgnoreCase(actual.textValue())) {
                    throw failure("record brand_id does not match the requested brand");
                }
            }
            String json = discovering ? null : encode(record);
            rows.add(new Row(brandId, json));
            addDigest(digest, discovering ? brandId : json);
        }
        return new Page(rows, next, hex(digest.digest()));
    }

    private static String encode(JsonNode node) throws IOException {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a provider record");
        }
    }

    private static void addDigest(MessageDigest digest, String value) {
        digest.update(value.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
    }

    static String digestText(String value) {
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
        return new Failure("BigMailer: " + reason);
    }

    /**
     * A diagnostic containing only a locally defined reason.
     *
     * <p>Remote IDs, response bodies and headers are kept out of job logs.
     */
    static final class Failure extends IOException {
        Failure(String message) {
            super(message);
        }
    }

    /**
     * One transient provider record or a brand key from discovery.
     *
     * <p>Discovery does not retain the brand's response object.
     */
    static final class Row {
        final String brand;
        final String data;

        Row(String brand, String data) {
            this.brand = brand;
            this.data = data;
        }
    }

    /**
     * The validated provider page and its replay fingerprint.
     *
     * <p>Contents are retained only until the owning paginator advances.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final String digest;

        Page(List<Row> rows, String next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }
}
