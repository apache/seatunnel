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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;

/**
 * Parses strict JSON without changing decimal precision or dropping unknown provider fields.
 *
 * <p>Only sanitized diagnostics leave this layer, including for malformed HTTP 200 bodies.
 */
final class AppfiguresJson {
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

    private AppfiguresJson() {}

    static JsonNode object(byte[] bytes, AppfiguresResource resource) throws AppfiguresFailure {
        JsonNode value;
        try {
            value = MAPPER.readTree(bytes);
        } catch (IOException | RuntimeException e) {
            throw new AppfiguresFailure(resource, "invalid JSON response");
        }
        if (value == null || !value.isObject()) {
            throw new AppfiguresFailure(resource, "expected a JSON object");
        }
        JsonNode errors = value.path("errors");
        if (value.hasNonNull("error")
                || (value.hasNonNull("errors") && (!errors.isArray() || !errors.isEmpty()))
                || (value.has("message") && value.path("status").asInt(0) >= 400)) {
            throw new AppfiguresFailure(
                    resource, "provider returned an error in the response body");
        }
        return value;
    }

    static String write(Object object, AppfiguresResource resource) throws AppfiguresFailure {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (IOException | RuntimeException e) {
            throw new AppfiguresFailure(resource, "could not serialize provider record");
        }
    }
}
