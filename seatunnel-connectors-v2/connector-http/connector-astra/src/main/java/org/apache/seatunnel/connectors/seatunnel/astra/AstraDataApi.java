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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Issues the bounded Astra collection and document operations used by the sink. Positive
 * acknowledgement requires the expected Data API status and document identities.
 */
final class AstraDataApi {
    static final String SCOPE = "_st_scope";
    static final String RECORD_ID = "_st_record_id";
    static final String HASH = "_st_payload_hash";
    private static final int WRITE_BATCH = 20;
    private static final int MAX_INSERT_BYTES = 4194304;
    private static final int MAX_DOCUMENT_CHARS = 4000000;
    private final AstraConfig config;
    private final AstraHttp http;

    AstraDataApi(AstraConfig config, AstraHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Verifies an existing collection or creates a compatible collection in the configured
     * keyspace. It never mutates or replaces a collection whose vector settings differ.
     */
    void ensureCollection() throws IOException {
        JsonNode collection = collection();
        if (collection == null) {
            ObjectNode request = AstraJson.object();
            request.putObject("createCollection")
                    .put("name", config.collection)
                    .putObject("options")
                    .putObject("vector")
                    .put("dimension", config.dimensions)
                    .put("metric", "cosine");
            JsonNode result = post(config.keyspaceUrl, request, "createCollection", false);
            JsonNode errors = errors(result, "createCollection");
            if (errors.isEmpty()) {
                JsonNode ok = result.path("status").path("ok");
                if (!ok.isIntegralNumber() || !ok.canConvertToInt() || ok.intValue() != 1) {
                    throw failure(
                            "createCollection", "provider did not acknowledge collection creation");
                }
            } else if (!only(errors, "COLLECTION_ALREADY_EXISTS")) {
                throw failure("createCollection", "provider returned errors");
            }
            collection = collection();
        }
        JsonNode vector =
                collection == null ? AstraJson.object() : collection.path("options").path("vector");
        JsonNode dimensions = vector.path("dimension");
        if (!dimensions.isIntegralNumber()
                || !dimensions.canConvertToInt()
                || dimensions.intValue() != config.dimensions
                || !"cosine".equals(vector.path("metric").textValue())) {
            throw failure(
                    "findCollections",
                    "existing collection vector dimension or metric is incompatible");
        }
    }

    private JsonNode collection() throws IOException {
        ObjectNode request = AstraJson.object();
        request.putObject("findCollections").putObject("options").put("explain", true);
        JsonNode result = post(config.keyspaceUrl, request, "findCollections", true);
        requireNoErrors(result, "findCollections");
        JsonNode list = result.path("status").path("collections");
        if (!list.isArray()) {
            throw failure("findCollections", "provider omitted the detailed collection list");
        }
        JsonNode found = null;
        for (JsonNode collection : list) {
            JsonNode name = collection.path("name");
            if (!name.isTextual()) {
                throw failure("findCollections", "provider returned an invalid collection entry");
            }
            if (config.collection.equals(name.textValue())) {
                if (found != null) {
                    throw failure("findCollections", "provider returned a duplicate collection");
                }
                found = collection;
            }
        }
        return found;
    }

    /**
     * Deletes only documents in the exact owned scope, optionally narrowed to one logical record.
     * Each continuation must acknowledge progress within the API's documented 20-row limit.
     */
    void delete(String scope, String record) throws IOException {
        ObjectNode request = AstraJson.object();
        ObjectNode filter = request.putObject("deleteMany").putObject("filter").put(SCOPE, scope);
        if (record != null) {
            filter.put(RECORD_ID, record);
        }
        for (int attempt = 0; attempt < config.maxDelete; attempt++) {
            JsonNode result = post(config.documentsUrl, request, "deleteMany", false);
            requireNoErrors(result, "deleteMany");
            JsonNode status = result.path("status");
            JsonNode count = status.path("deletedCount");
            JsonNode more = status.path("moreData");
            if (!count.isIntegralNumber()
                    || !count.canConvertToInt()
                    || count.intValue() < 0
                    || count.intValue() > 20
                    || (!more.isMissingNode() && !more.isBoolean())
                    || (more.asBoolean(false) && count.intValue() != 20)) {
                throw failure("deleteMany", "invalid scoped delete acknowledgement");
            }
            if (!more.asBoolean(false)) {
                return;
            }
        }
        throw failure("deleteMany", "filtered deletion exceeded max_delete_requests");
    }

    /**
     * Splits documents by count and bytes and verifies every acknowledged document ID. A repeat is
     * accepted only when existing duplicates have the exact expected payload digest.
     */
    void insert(List<ObjectNode> documents) throws IOException {
        int start = 0;
        while (start < documents.size()) {
            int end = Math.min(start + WRITE_BATCH, documents.size());
            ObjectNode request = insertCommand(documents.subList(start, end));
            int bound = Math.min(config.maxBody, MAX_INSERT_BYTES);
            while (AstraJson.bytes(request).length > bound) {
                if (end == start + 1) {
                    throw failure(
                            "insertMany", "single document exceeds the permitted request size");
                }
                end = start + (end - start) / 2;
                request = insertCommand(documents.subList(start, end));
            }
            JsonNode result = post(config.documentsUrl, request, "insertMany", false);
            verifyInsert(result, documents.subList(start, end));
            start = end;
        }
    }

    void validateSize(ObjectNode document) throws IOException {
        if (document.toString().length() > MAX_DOCUMENT_CHARS
                || AstraJson.bytes(insertCommand(List.of(document))).length
                        > Math.min(config.maxBody, MAX_INSERT_BYTES)) {
            throw failure("insertMany", "single document exceeds the permitted request size");
        }
    }

    private static ObjectNode insertCommand(List<ObjectNode> documents) {
        ObjectNode request = AstraJson.object();
        ObjectNode command = request.putObject("insertMany");
        command.putArray("documents").addAll(documents);
        command.putObject("options").put("ordered", false);
        return request;
    }

    private void verifyInsert(JsonNode result, List<ObjectNode> documents) throws IOException {
        Map<String, String> expected = new HashMap<>();
        for (ObjectNode document : documents) {
            expected.put(document.path("_id").textValue(), document.path(HASH).textValue());
        }
        JsonNode errors = errors(result, "insertMany");
        JsonNode inserted = result.path("status").path("insertedIds");
        Set<String> seen = new HashSet<>();
        if (inserted.isArray()) {
            for (JsonNode id : inserted) {
                if (!id.isTextual()
                        || !expected.containsKey(id.textValue())
                        || !seen.add(id.textValue())) {
                    throw failure(
                            "insertMany",
                            "provider returned an unexpected or repeated inserted ID");
                }
            }
        } else if (!inserted.isMissingNode() || errors.isEmpty()) {
            throw failure("insertMany", "provider omitted the inserted IDs");
        }
        if (errors.isEmpty()) {
            if (seen.size() != documents.size()) {
                throw failure("insertMany", "provider acknowledged only part of the batch");
            }
            return;
        }
        if (!only(errors, "DOCUMENT_ALREADY_EXISTS") || seen.size() == documents.size()) {
            throw failure("insertMany", "provider returned errors or a partial write");
        }
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            if (!seen.contains(entry.getKey())
                    && !matchesExisting(entry.getKey(), entry.getValue())) {
                throw failure(
                        "insertMany", "duplicate document does not match the expected payload");
            }
        }
    }

    private boolean matchesExisting(String id, String digest) throws IOException {
        ObjectNode request = AstraJson.object();
        ObjectNode command = request.putObject("findOne");
        command.putObject("filter").put("_id", id);
        command.putObject("projection").put("_id", 1).put(HASH, 1);
        JsonNode result = post(config.documentsUrl, request, "findOne", true);
        requireNoErrors(result, "findOne");
        JsonNode existing = result.path("data").path("document");
        return id.equals(existing.path("_id").textValue())
                && digest.equals(existing.path(HASH).textValue());
    }

    private ObjectNode post(URI url, ObjectNode body, String operation, boolean safeRead)
            throws IOException {
        try {
            return http.post(url, AstraConfig.ASTRA_TOKEN, config.token, body, operation, safeRead);
        } catch (IOException e) {
            if (safeRead) {
                throw e;
            }
            throw failure(operation, "no valid mutation acknowledgement (" + e.getMessage() + ")");
        }
    }

    private static void requireNoErrors(JsonNode result, String operation) throws IOException {
        if (!errors(result, operation).isEmpty()) {
            throw failure(operation, "provider returned errors");
        }
    }

    private static JsonNode errors(JsonNode result, String operation) throws IOException {
        JsonNode errors = result.path("errors");
        if (errors.isMissingNode()) {
            return AstraJson.array();
        }
        if (!errors.isArray()) {
            throw failure(operation, "provider returned an invalid errors value");
        }
        return errors;
    }

    private static boolean only(JsonNode errors, String code) {
        for (JsonNode error : errors) {
            if (!code.equals(error.path("errorCode").textValue())) {
                return false;
            }
        }
        return !errors.isEmpty();
    }

    private static IOException failure(String operation, String reason) {
        return AstraHttp.failure(operation, reason);
    }
}
