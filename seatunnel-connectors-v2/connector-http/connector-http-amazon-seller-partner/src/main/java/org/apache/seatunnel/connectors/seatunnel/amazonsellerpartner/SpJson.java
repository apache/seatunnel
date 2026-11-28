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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;

/**
 * Parses and serializes the provider's JSON without rounding numeric fields.
 *
 * <p>Duplicate object fields and trailing documents fail instead of replacing provider data.
 */
final class SpJson {
    static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private SpJson() {}

    static JsonNode read(byte[] bytes, String operation) throws SpFailure {
        try {
            JsonNode value = JSON.readTree(bytes);
            if (value != null && !value.isNull()) {
                return value;
            }
        } catch (IOException | RuntimeException ignored) {
            // The parser's exception can include a portion of the input document.
        }
        throw new SpFailure(operation, "expected a complete JSON response");
    }

    static String write(Object value, String operation) throws SpFailure {
        try {
            return JSON.writeValueAsString(value);
        } catch (IOException | RuntimeException e) {
            throw new SpFailure(operation, "could not serialize data");
        }
    }

    static String text(JsonNode tree, String key, int max, String operation) throws SpFailure {
        JsonNode value = tree.path(key);
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > max
                || value.textValue().chars().anyMatch(Character::isISOControl)) {
            throw new SpFailure(operation, "missing or invalid " + key);
        }
        return value.textValue();
    }
}
