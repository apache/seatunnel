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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;

/**
 * Exact JSON representation shared among Box list and extraction responses.
 *
 * <p>JSON parsing discards excerpts from diagnostics and rejects ambiguous duplicate keys.
 */
final class BoxJson {
    static final ObjectMapper MAPPER =
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

    private BoxJson() {}

    static JsonNode read(byte[] data, String operation) throws BoxReadException {
        try {
            return MAPPER.readTree(data);
        } catch (IOException | RuntimeException failure) {
            throw new BoxReadException(operation, "response is not valid JSON");
        }
    }

    static String write(JsonNode data, String operation) throws BoxReadException {
        try {
            return MAPPER.writeValueAsString(data);
        } catch (IOException | RuntimeException failure) {
            throw new BoxReadException(operation, "could not encode Box JSON data");
        }
    }

    static JsonNode object(JsonNode data, String operation) throws BoxReadException {
        if (data == null
                || !data.isObject()
                || "error".equals(data.path("type").asText())
                || data.hasNonNull("error")
                || data.path("status").asInt(0) >= 400) {
            throw new BoxReadException(operation, "expected a successful Box JSON object");
        }
        return data;
    }
}
