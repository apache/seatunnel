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

package org.apache.seatunnel.connectors.seatunnel.bunnyinc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes fixed Bunny GraphQL queries and validates their connection envelopes.
 *
 * <p>Billing amounts and dynamically structured JSON are retained without converting them through
 * double or an object model. Provider messages and query values are excluded from diagnostics.
 */
final class BunnyIncJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(32 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private BunnyIncJson() {}

    /**
     * Sends cursor values as GraphQL variables represented by a JSON object.
     *
     * @param resource fixed connection query
     * @param size first parameter for this page
     * @param after optional opaque continuation
     * @return the UTF-8 request body for the GraphQL endpoint
     * @throws Failure if the request cannot be encoded
     */
    static byte[] request(BunnyIncResource resource, int size, String after) throws Failure {
        ObjectNode root = JSON.createObjectNode();
        root.put("query", resource.query());
        root.put("operationName", "SeaTunnelRead");
        ObjectNode variables = root.putObject("variables");
        variables.put("first", size);
        variables.put("after", after);
        try {
            return JSON.writeValueAsBytes(root);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not encode the GraphQL query");
        }
    }

    /**
     * Rejects any nonempty GraphQL errors, including partial data with HTTP 200.
     *
     * @param bytes the complete bounded response
     * @return the successful GraphQL response object
     * @throws Failure if the encoding, JSON shape or operation result is invalid
     */
    static JsonNode parse(byte[] bytes) throws Failure {
        JsonNode root;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
            root = JSON.readTree(bytes);
        } catch (CharacterCodingException ignored) {
            throw failure("response is not valid UTF-8 JSON");
        } catch (IOException | RuntimeException ignored) {
            throw failure("response is not valid GraphQL JSON");
        }
        if (root == null || !root.isObject()) {
            throw failure("response is not a GraphQL JSON object");
        }
        JsonNode errors = root.get("errors");
        if (errors != null && !(errors.isArray() && errors.isEmpty())) {
            throw failure("GraphQL query returned errors");
        }
        if (!root.path("data").isObject()) {
            throw failure("GraphQL response is missing its data object");
        }
        return root;
    }

    /**
     * Checks the provider's cursor and the complete page before any rows are published.
     *
     * @param root the successful GraphQL response
     * @param resource the fixed selected connection field
     * @param limit the requested maximum number of nodes
     * @return a page with the exact ordered rows and optional continuation
     * @throws Failure if the provider result is missing or inconsistent
     */
    static Page page(JsonNode root, BunnyIncResource resource, int limit) throws Failure {
        JsonNode data = root.path("data").path(resource.field);
        JsonNode nodes = data.path("nodes");
        JsonNode info = data.path("pageInfo");
        JsonNode more = info.path("hasNextPage");
        JsonNode cursor = info.path("endCursor");
        if (!data.isObject() || !nodes.isArray() || nodes.size() > limit) {
            throw failure("invalid or oversized GraphQL nodes");
        }
        if (!info.isObject() || !more.isBoolean()) {
            throw failure("GraphQL pageInfo is missing a boolean hasNextPage");
        }
        String next = more.booleanValue() ? cursor.textValue() : null;
        if (more.booleanValue() && !BunnyIncConfig.validCursor(next)) {
            throw failure("GraphQL pageInfo is missing a valid endCursor");
        }
        if (!cursor.isMissingNode() && !cursor.isNull() && !cursor.isTextual()) {
            throw failure("GraphQL endCursor must be a string or null");
        }

        List<Record> records = new ArrayList<>(nodes.size());
        List<String> digestInputs = new ArrayList<>(nodes.size() + 1);
        digestInputs.add(next == null ? "" : next);
        for (JsonNode node : nodes) {
            if (!node.isObject()) {
                throw failure("GraphQL nodes contains a non-object record");
            }
            JsonNode id = node.path("id");
            if (!id.isMissingNode() && !id.isNull() && !id.isTextual() && !id.isIntegralNumber()) {
                throw failure("GraphQL ID must be an opaque string or integral value");
            }
            String dataJson = encode(node);
            records.add(
                    new Record(id.isMissingNode() || id.isNull() ? null : id.asText(), dataJson));
            digestInputs.add(dataJson);
        }
        return new Page(records, next, fingerprint(digestInputs));
    }

    private static String encode(JsonNode node) throws Failure {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a Bunny billing record");
        }
    }

    static String fingerprint(List<String> values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) {
                result.append(Character.forDigit((value >>> 4) & 15, 16));
                result.append(Character.forDigit(value & 15, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static Failure failure(String reason) {
        return new Failure(reason);
    }

    /**
     * Carries a billing record ready for parsing by the destination's chosen schema.
     *
     * <p>The provider ID and full JSON are independent string columns.
     */
    static final class Record {
        final String id;
        final String json;

        Record(String id, String json) {
            this.id = id;
            this.json = json;
        }
    }

    /**
     * Represents one bounded cursor page after validating its complete payload.
     *
     * <p>The replay digest covers ordered records and the continuation.
     */
    static final class Page {
        final List<Record> rows;
        final String next;
        final String digest;

        Page(List<Record> rows, String next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Indicates a safe source failure that the engine can report.
     *
     * <p>Messages are fixed locally and never include a provider error body or billable data.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }
}
