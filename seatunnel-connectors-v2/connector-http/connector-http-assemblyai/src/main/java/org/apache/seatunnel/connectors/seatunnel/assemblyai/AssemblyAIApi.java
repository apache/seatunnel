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
package org.apache.seatunnel.connectors.seatunnel.assemblyai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements the AssemblyAI transcript listing and three child API responses.
 *
 * <p>Only fixed same-origin reads use the API token. Arbitrary provider links are treated as output
 * metadata unless a validated older-page cursor can be rebuilt on the list route.
 */
final class AssemblyAIApi {
    static final String LIST_PATH = "/v2/transcript";
    private static final Set<String> STATUSES =
            Set.of("queued", "processing", "completed", "error");
    private static final String DELETED_AUDIO = "http://deleted_by_user";

    private final AssemblyAIConfig config;
    private final AssemblyAIHttp http;

    AssemblyAIApi(AssemblyAIConfig config, AssemblyAIHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Discovers transcripts from newest toward older pages.
     *
     * <p>Timestamp and echoed count validation prevents a malformed page from advancing source
     * progress. The current and newer links never drive requests.
     */
    Page list(String before, Instant previousTail, long deadline) throws InterruptedException {
        AssemblyAIResource resource = AssemblyAIResource.TRANSCRIPTS;
        String target = LIST_PATH + "?limit=" + config.pageSize;
        if (before != null) {
            if (!validId(before)) {
                throw resource.failure("saved before_id is invalid");
            }
            target += "&before_id=" + before;
        }
        JsonNode root = http.get(resource, target, deadline);
        checkError(resource, root);
        JsonNode transcripts = root.path("transcripts");
        JsonNode detail = root.path("page_details");
        if (!transcripts.isArray() || transcripts.size() > config.pageSize || !detail.isObject()) {
            throw resource.failure("invalid transcript list or pagination metadata");
        }
        verifyCounter(resource, detail, "limit", config.pageSize);
        verifyCounter(resource, detail, "result_count", transcripts.size());
        String cursor = older(resource, detail.path("prev_url"));
        List<Parent> results = new ArrayList<>(transcripts.size());
        Set<String> ids = new HashSet<>();
        ArrayNode stable = http.json.createArrayNode();
        Instant tail = previousTail;
        for (JsonNode item : transcripts) {
            Parent parent = parent(resource, item);
            if (!ids.add(parent.id)) {
                throw resource.failure("transcript page repeats an ID");
            }
            if (tail != null && parent.created.isAfter(tail)) {
                throw resource.failure("transcript pages are not ordered newest to oldest");
            }
            tail = parent.created;
            results.add(parent);
            ObjectNode fingerprint = ((ObjectNode) item).deepCopy();
            fingerprint.remove(List.of("audio_url", "resource_url", "error"));
            fingerprint.put("deleted_audio", parent.deleted);
            stable.add(fingerprint);
        }
        http.remaining(resource, deadline);
        return new Page(
                results, cursor, tail, AssemblyAIConfig.hash(stable.toString() + "\n" + cursor));
    }

    /**
     * Accepts only an older cursor on the exact configured list route.
     *
     * <p>Reconstructing the request from the validated before_id prevents passing credentials to an
     * external address or accepting supplemental filters from pagination metadata.
     */
    private String older(AssemblyAIResource resource, JsonNode link) {
        if (link.isMissingNode() || link.isNull()) {
            return null;
        }
        if (!link.isTextual() || link.textValue().isEmpty() || link.textValue().length() > 8192) {
            throw resource.failure("invalid transcript continuation URI");
        }
        try {
            String value = link.textValue();
            URI url = config.api.resolve(value.startsWith("?") ? LIST_PATH + value : value);
            int port = url.getPort() == 443 ? -1 : url.getPort();
            if (!"https".equalsIgnoreCase(url.getScheme())
                    || url.getHost() == null
                    || !config.api.getHost().equalsIgnoreCase(url.getHost())
                    || config.api.getPort() != port
                    || url.getRawUserInfo() != null
                    || url.getRawFragment() != null
                    || !LIST_PATH.equals(url.getRawPath())
                    || url.getRawQuery() == null) {
                throw resource.failure(
                        "continuation escaped the configured HTTPS origin or transcript list path");
            }
            Map<String, String> query = new LinkedHashMap<>();
            for (String raw : url.getRawQuery().split("&", -1)) {
                String[] parts = raw.split("=", 2);
                if (parts.length != 2) {
                    throw resource.failure(
                            "continuation changed query scope or contains duplicate parameters");
                }
                String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                String field = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                if (!("before_id".equals(key) || "limit".equals(key))
                        || query.putIfAbsent(key, field) != null) {
                    throw resource.failure(
                            "continuation changed query scope or contains duplicate parameters");
                }
            }
            String pageLimit = query.get("limit");
            if (pageLimit != null && !Integer.toString(config.pageSize).equals(pageLimit)
                    || !validId(query.get("before_id"))) {
                throw resource.failure(
                        "continuation changed query scope or contains duplicate parameters");
            }
            return query.get("before_id");
        } catch (IllegalArgumentException ex) {
            throw resource.failure("invalid transcript continuation URI");
        }
    }

    /**
     * Reads the selected full child response.
     *
     * <p>Redacted audio checks the transcript option on the detail route before fetching the signed
     * URL metadata. No media resource is downloaded.
     */
    Child child(AssemblyAIResource resource, Parent parent, long deadline)
            throws InterruptedException {
        String transcript = LIST_PATH + "/" + parent.id;
        if (resource == AssemblyAIResource.REDACTED_AUDIO) {
            JsonNode details = http.get(resource, transcript, deadline);
            checkResponseId(resource, details, parent.id, true);
            if (!"completed".equals(status(resource, details)) || deleted(details)) {
                return Child.empty();
            }
            checkError(resource, details);
            JsonNode enabled = details.path("redact_pii_audio");
            if (!enabled.isMissingNode() && !enabled.isNull() && !enabled.isBoolean()) {
                throw resource.failure("invalid redact_pii_audio flag in transcript detail");
            }
            if (!enabled.asBoolean(false)) {
                return Child.empty();
            }
            JsonNode document = http.get(resource, transcript + "/" + resource.route, deadline);
            checkError(resource, document);
            checkResponseId(resource, document, parent.id, false);
            if (!"redacted_audio_ready".equals(document.path("status").asText(""))
                    || !signedUrl(document.path("redacted_audio_url"))) {
                throw resource.failure("redacted audio response lacks ready metadata");
            }
            ObjectNode stable = ((ObjectNode) document).deepCopy();
            stable.remove("redacted_audio_url");
            return new Child(List.of(document), AssemblyAIConfig.hash(stable.toString()));
        }

        JsonNode response = http.get(resource, transcript + "/" + resource.route, deadline);
        checkError(resource, response);
        checkResponseId(resource, response, parent.id, false);
        JsonNode records = response.path(resource.route);
        if (!records.isArray()) {
            throw resource.failure("child response does not contain the requested array");
        }
        List<JsonNode> rows = new ArrayList<>(records.size());
        for (JsonNode row : records) {
            if (!row.isObject()) {
                throw resource.failure("child array contains a non-object record");
            }
            if ((rows.size() & 127) == 0) {
                http.remaining(resource, deadline);
            }
            rows.add(row);
        }
        return new Child(rows, AssemblyAIConfig.hash(records.toString()));
    }

    private static boolean signedUrl(JsonNode value) {
        if (!value.isTextual() || value.textValue().length() > 16384) {
            return false;
        }
        try {
            URI url = URI.create(value.textValue());
            return "https".equalsIgnoreCase(url.getScheme())
                    && url.getHost() != null
                    && url.getRawUserInfo() == null
                    && url.getRawFragment() == null;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static void checkResponseId(
            AssemblyAIResource resource, JsonNode response, String expected, boolean required) {
        JsonNode id = response.path("id");
        if (!required && id.isMissingNode()) {
            return;
        }
        if (!id.isTextual() || !expected.equals(id.textValue())) {
            throw resource.failure("response transcript ID does not match the parent");
        }
    }

    private static void checkError(AssemblyAIResource resource, JsonNode object) {
        if (object.hasNonNull("error")) {
            throw resource.failure("provider returned an error response");
        }
    }

    private static Parent parent(AssemblyAIResource resource, JsonNode source) {
        JsonNode id = source.path("id");
        if (!source.isObject() || !id.isTextual() || !validId(id.textValue())) {
            throw resource.failure("transcript summary has an invalid ID or shape");
        }
        JsonNode created = source.path("created");
        if (!created.isTextual() || created.textValue().length() > 64) {
            throw resource.failure("invalid transcript creation timestamp");
        }
        Instant time = creation(resource, created.textValue());
        return new Parent(id.textValue(), time, status(resource, source), deleted(source), source);
    }

    private static Instant creation(AssemblyAIResource resource, String text) {
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeException noOffset) {
            try {
                return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC);
            } catch (DateTimeException invalid) {
                throw resource.failure("invalid transcript creation timestamp");
            }
        }
    }

    private static String status(AssemblyAIResource resource, JsonNode source) {
        String status = source.path("status").asText("");
        if (!STATUSES.contains(status)) {
            throw resource.failure("unknown transcript status");
        }
        return status;
    }

    private static boolean deleted(JsonNode source) {
        return DELETED_AUDIO.equals(source.path("audio_url").asText(""));
    }

    private static boolean validId(String id) {
        return id != null && id.matches("[A-Za-z0-9_-]{1,200}");
    }

    private static void verifyCounter(
            AssemblyAIResource resource, JsonNode object, String name, int expected) {
        JsonNode value = object.path(name);
        if (!value.isMissingNode()
                && (!value.isIntegralNumber()
                        || !value.canConvertToInt()
                        || value.intValue() != expected)) {
            throw resource.failure("transcript pagination counters are inconsistent");
        }
    }

    /**
     * Holds one bounded list page and the older cursor.
     *
     * <p>The digest compares stable transcript metadata without hashing temporary media links.
     */
    static final class Page {
        final List<Parent> rows;
        final String older;
        final Instant tail;
        final String digest;

        Page(List<Parent> rows, String older, Instant tail, String digest) {
            this.rows = rows;
            this.older = older;
            this.tail = tail;
            this.digest = digest;
        }
    }

    /**
     * Binds each selected child to the same transcript and creation timestamp.
     *
     * <p>A completed transcript marked as deleted remains a valid summary.
     */
    static final class Parent {
        final String id;
        final Instant created;
        final String status;
        final boolean deleted;
        final JsonNode record;

        Parent(String id, Instant created, String status, boolean deleted, JsonNode record) {
            this.id = id;
            this.created = created;
            this.status = status;
            this.deleted = deleted;
            this.record = record;
        }

        boolean hasChildren() {
            return !deleted && "completed".equals(status);
        }
    }

    /**
     * Holds response records and the fingerprint checked before resuming a saved offset.
     *
     * <p>Records and any redacted audio URL stay in memory and are never serialized as state.
     */
    static final class Child {
        final List<JsonNode> rows;
        final String digest;

        Child(List<JsonNode> rows, String digest) {
            this.rows = rows;
            this.digest = digest;
        }

        static Child empty() {
            return new Child(List.of(), AssemblyAIConfig.hash("[]"));
        }
    }
}
