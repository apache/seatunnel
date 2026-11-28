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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;

/**
 * Encodes deterministic document identities and parses lossless bounded provider JSON. All parse
 * failures omit the content supplied by users and remote services.
 */
final class AstraJson {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private AstraJson() {}

    static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    static ArrayNode array() {
        return MAPPER.createArrayNode();
    }

    static ObjectNode readObject(byte[] bytes) throws IOException {
        try {
            JsonNode node = MAPPER.readTree(bytes);
            if (node != null && node.isObject()) {
                return (ObjectNode) node;
            }
        } catch (IOException | RuntimeException ignored) {
            // The exception can contain the JSON body or part of a source record.
        }
        throw new IOException("Astra sink: expected a valid JSON object");
    }

    static String identity(Object value) {
        JsonNode node = MAPPER.valueToTree(value);
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(sha256(bytes(canonical(node))));
    }

    static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static byte[] bytes(JsonNode node) {
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static JsonNode canonical(JsonNode input) {
        if (input.isObject()) {
            Map<String, JsonNode> fields = new TreeMap<>();
            input.fields()
                    .forEachRemaining(
                            field -> fields.put(field.getKey(), canonical(field.getValue())));
            return object().setAll(fields);
        }
        if (input.isArray()) {
            ArrayNode result = array();
            input.forEach(value -> result.add(canonical(value)));
            return result;
        }
        return input;
    }
}
