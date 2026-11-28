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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Decodes official success and error envelopes while retaining exact JSON values.
 *
 * <p>Only recognized error codes are allowed in diagnostics. A body can contain candidate data even
 * when its outer HTTP status indicates a failure.
 */
final class AshbyJson {
    static final ObjectMapper JSON =
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
    private static final Map<String, String> ERRORS =
            Map.of(
                    "sync_token_expired", "stored sync token expired",
                    "sync_token_invalid", "stored sync token is invalid",
                    "next_cursor_expired", "stored page cursor expired",
                    "cursor_invalid", "stored page cursor is invalid",
                    "invalid_next_cursor", "stored page cursor is incompatible with this pass",
                    "incremental_sync_too_large", "too many changes for the stored sync token");

    private AshbyJson() {}

    static JsonNode envelope(byte[] bytes, int status, AshbyResource target) throws AshbyFailure {
        JsonNode body = null;
        try {
            body = JSON.readTree(bytes);
        } catch (IOException | RuntimeException ignored) {
            // Parser exceptions may contain response excerpts.
        }
        if (body != null && body.isObject() && body.path("success").isBoolean()) {
            if (!body.path("success").booleanValue()) {
                String primary = body.path("errorInfo").path("code").asText();
                String diagnosis = diagnosis(target, primary);
                if (diagnosis != null) {
                    throw new AshbyFailure(target, diagnosis);
                }
                for (JsonNode error : body.path("errors")) {
                    String code = error.isTextual() ? error.asText() : error.path("code").asText();
                    diagnosis = diagnosis(target, code);
                    if (diagnosis != null) {
                        throw new AshbyFailure(target, diagnosis);
                    }
                }
                if (status == 200) {
                    throw new AshbyFailure(target, "provider response reported success=false");
                }
            } else if (status == 200 && body.path("results").isArray()) {
                JsonNode errors = body.path("errors");
                if (body.hasNonNull("errorInfo")
                        || (body.hasNonNull("errors")
                                && (!errors.isArray() || !errors.isEmpty()))) {
                    throw new AshbyFailure(
                            target, "provider returned contradictory success and error data");
                }
                return body;
            }
        }
        if (status == 401 || status == 403) {
            throw new AshbyFailure(
                    target, "HTTP " + status + " requires an active key with " + target.permission);
        }
        if (status != 200) {
            throw new AshbyFailure(target, "unexpected HTTP " + status);
        }
        throw new AshbyFailure(target, "expected an Ashby success envelope with a results array");
    }

    private static String diagnosis(AshbyResource target, String code) {
        if ("missing_endpoint_permission".equals(code)) {
            return "missing endpoint permission " + target.permission;
        }
        String message = ERRORS.get(code);
        return message == null ? null : message + " (" + code + ")";
    }

    static String encode(JsonNode value, AshbyResource resource) throws AshbyFailure {
        try {
            return JSON.writeValueAsString(value);
        } catch (IOException | RuntimeException e) {
            throw new AshbyFailure(resource, "could not encode provider data");
        }
    }
}
