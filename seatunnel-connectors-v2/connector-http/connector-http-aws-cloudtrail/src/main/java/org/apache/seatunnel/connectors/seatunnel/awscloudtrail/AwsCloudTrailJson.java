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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.awscloudtrail.AwsCloudTrailConfig.Window;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Serializes lookup parameters once and validates bounded event pages before emission. An exact
 * digest binds the page contents and its continuation to a partial checkpoint.
 */
final class AwsCloudTrailJson {
    static final int MAX_TOKEN_CHARS = 16384;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(1048576)
                                                    .build())
                                    .build())
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String[] OPTIONAL_TEXT = {
        "AccessKeyId",
        "CloudTrailEvent",
        "EventId",
        "EventName",
        "EventSource",
        "ReadOnly",
        "Username"
    };

    private AwsCloudTrailJson() {}

    static byte[] request(AwsCloudTrailConfig config, Window window, String token) {
        ObjectNode request =
                JSON.createObjectNode()
                        .put("StartTime", window.start)
                        .put("EndTime", window.end)
                        .put("MaxResults", config.pageSize);
        if (!config.attribute.isEmpty()) {
            request.putArray("LookupAttributes")
                    .addObject()
                    .put("AttributeKey", config.attribute)
                    .put("AttributeValue", config.value);
        }
        if (token != null) {
            request.put("NextToken", token);
        }
        return request.toString().getBytes(StandardCharsets.UTF_8);
    }

    static ObjectNode parse(byte[] bytes) throws IOException {
        try {
            JsonNode root = JSON.readTree(bytes);
            if (root != null && root.isObject()) {
                return (ObjectNode) root;
            }
        } catch (IOException | RuntimeException ignored) {
            // AWS parser diagnostics can expose event and authentication payloads.
        }
        throw failure("expected a valid JSON object");
    }

    /**
     * Preserves optional values, exact epoch decimals and the nested event's original string. Known
     * event times must be inside the frozen interval and remain newest-first.
     */
    static Page page(
            ObjectNode body, AwsCloudTrailConfig config, Window window, BigDecimal preceding)
            throws IOException {
        if (body.hasNonNull("__type")
                || body.hasNonNull("Error")
                || body.hasNonNull("error")
                || body.hasNonNull("errors")
                || body.hasNonNull("ErrorCode")) {
            throw failure("provider returned an error in an HTTP 200 response");
        }
        JsonNode raw = body.path("Events");
        if (!raw.isArray() || raw.size() > config.pageSize) {
            throw failure("invalid Events page size or shape");
        }
        String next = continuation(body.path("NextToken"));
        List<Event> events = new ArrayList<>(raw.size());
        BigDecimal lower = BigDecimal.valueOf(window.start);
        BigDecimal upper = BigDecimal.valueOf(window.end);
        BigDecimal last = preceding;
        for (JsonNode row : raw) {
            if (!row.isObject()) {
                throw failure("non-object event in the Events array");
            }
            for (String field : OPTIONAL_TEXT) {
                if (row.hasNonNull(field) && !row.path(field).isTextual()) {
                    throw failure("an optional event text field has an invalid type");
                }
            }
            JsonNode resources = row.path("Resources");
            if (!resources.isMissingNode() && !resources.isNull() && !resources.isArray()) {
                throw failure("event Resources has an invalid type");
            }
            for (JsonNode resource : resources) {
                if (!resource.isNull()
                        && (!resource.isObject()
                                || resource.hasNonNull("ResourceName")
                                        && !resource.path("ResourceName").isTextual()
                                || resource.hasNonNull("ResourceType")
                                        && !resource.path("ResourceType").isTextual())) {
                    throw failure("event Resources contains an invalid resource");
                }
            }
            JsonNode time = row.path("EventTime");
            BigDecimal epoch = null;
            if (!time.isMissingNode() && !time.isNull()) {
                if (!time.isNumber()) {
                    throw failure("event EventTime must be a number");
                }
                epoch = time.decimalValue();
                if (epoch.scale() > 9 || epoch.compareTo(lower) < 0 || epoch.compareTo(upper) > 0) {
                    throw failure("event time is outside the requested bounds or precision");
                }
                if (last != null && epoch.compareTo(last) > 0) {
                    throw failure("events are not newest-first");
                }
                last = epoch;
            }
            events.add(
                    new Event(
                            row.path("EventId").textValue(),
                            epoch == null ? null : epoch.toPlainString(),
                            row.path("CloudTrailEvent").textValue(),
                            row.toString()));
        }
        return new Page(events, next, identity(Arrays.asList(raw, next)), last);
    }

    static String continuation(JsonNode value) throws IOException {
        if (value.isNull()
                || value.isMissingNode()
                || value.isTextual() && value.textValue().isEmpty()) {
            return null;
        }
        if (!value.isTextual() || !validToken(value.textValue())) {
            throw failure("invalid continuation token");
        }
        return value.textValue();
    }

    static boolean validToken(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= MAX_TOKEN_CHARS
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    static String identity(Object value) {
        JsonNode tree = JSON.valueToTree(value);
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        sha256(canonical(tree).toString().getBytes(StandardCharsets.UTF_8)));
    }

    static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static JsonNode canonical(JsonNode input) {
        if (input.isObject()) {
            Map<String, JsonNode> fields = new TreeMap<>();
            input.fields()
                    .forEachRemaining(
                            field -> fields.put(field.getKey(), canonical(field.getValue())));
            return JSON.createObjectNode().setAll(fields);
        }
        if (input.isArray()) {
            ArrayNode items = JSON.createArrayNode();
            input.forEach(value -> items.add(canonical(value)));
            return items;
        }
        return input;
    }

    static Failure failure(String safe) {
        return new Failure("AWS CloudTrail LookupEvents: " + safe);
    }

    /**
     * Carries a fixed, sanitized failure that may pass through the wire worker to job diagnostics.
     * Provider error bodies and credentials are never attached as causes.
     */
    static final class Failure extends IOException {
        Failure(String message) {
            super(message);
        }
    }

    /**
     * Holds the typed row projection and original event serialized as a JSON string. Keeping
     * CloudTrailEvent textual preserves the provider's audit-event contract.
     */
    static final class Event {
        final String id;
        final String time;
        final String audit;
        final String json;

        Event(String id, String time, String audit, String json) {
            this.id = id;
            this.time = time;
            this.audit = audit;
            this.json = json;
        }
    }

    /**
     * Contains one validated provider page and its opaque next token. The digest covers order and
     * values even when several events share one timestamp.
     */
    static final class Page {
        final List<Event> events;
        final String next;
        final String digest;
        final BigDecimal oldest;

        Page(List<Event> events, String next, String digest, BigDecimal oldest) {
            this.events = events;
            this.next = next;
            this.digest = digest;
            this.oldest = oldest;
        }
    }
}
