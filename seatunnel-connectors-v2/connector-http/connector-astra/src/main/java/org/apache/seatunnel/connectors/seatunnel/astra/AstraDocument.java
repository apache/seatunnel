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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.astra.AstraConfig.Embedding;
import org.apache.seatunnel.connectors.seatunnel.astra.AstraConfig.Mode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Converts typed input records into scoped chunks and stable Data API documents. User metadata is
 * isolated from store identities, filters and vector commands.
 */
final class AstraDocument {
    private final AstraConfig config;
    private final AstraSplitter splitter;

    AstraDocument(AstraConfig config, AstraSplitter splitter) {
        this.config = config;
        this.splitter = splitter;
    }

    /**
     * Resolves scope and identity before generating any vector or touching the database. Delete
     * events keep only the keys needed to remove previous versions of their record.
     */
    Record prepare(ObjectNode input, boolean deletion, long position) throws IOException {
        Scope scope =
                new Scope(
                        route(input, config.namespaceField, config.namespace, true),
                        route(input, config.streamField, config.stream, false));
        String id = config.mode == Mode.APPEND_DEDUP ? recordId(input) : null;
        if (deletion) {
            return new Record(scope, id, position, AstraJson.object(), List.of(), 128);
        }
        ObjectNode selected = AstraPaths.select(input, config.textFields, config.rename);
        if (selected.isEmpty()) {
            throw new IOException("Astra sink: row has none of the selected text fields");
        }
        List<String> chunks = splitter.split(text(selected));
        if (chunks.isEmpty()) {
            throw new IOException("Astra sink: selected text produces no chunks");
        }
        ObjectNode metadata = AstraPaths.select(input, config.metadataFields, config.rename);
        validateMetadata(metadata, 1, new int[] {990});
        ArrayNode existing =
                config.embedding == Embedding.FROM_FIELD
                        ? AstraEmbeddings.vector(
                                AstraPaths.one(input, config.embeddingField), config.dimensions)
                        : null;
        long each = AstraJson.bytes(metadata).length + 24L * config.dimensions + 1024;
        long size = 0;
        List<Chunk> prepared = new ArrayList<>(chunks.size());
        for (String content : chunks) {
            size += each + content.getBytes(StandardCharsets.UTF_8).length;
            if (size > config.maxBuffer) {
                throw new IOException("Astra sink: expanded row exceeds max_buffer_bytes");
            }
            prepared.add(new Chunk(content, existing));
        }
        return new Record(scope, id, position, metadata, prepared, size);
    }

    Scope fixedScope() {
        return new Scope(config.namespace, config.stream);
    }

    /**
     * Materializes positively checked embeddings into indexed documents with stable IDs. The digest
     * covers the exact payload used to verify a replayed insert.
     */
    ObjectNode document(Record record, String run, int index, ArrayNode vector) {
        ObjectNode doc = AstraJson.object();
        String id =
                record.id == null
                        ? AstraJson.identity(
                                Arrays.asList(run, record.scope.key, record.position, index))
                        : AstraJson.identity(Arrays.asList(record.scope.key, record.id, index));
        doc.put("_id", id);
        doc.set("$vector", vector);
        doc.put(AstraDataApi.SCOPE, record.scope.key);
        doc.put("_st_namespace", record.scope.namespace);
        doc.put("_st_stream", record.scope.stream);
        if (record.id != null) {
            doc.put(AstraDataApi.RECORD_ID, record.id);
        }
        doc.put("_st_chunk_index", index);
        if (!config.omitText) {
            doc.put("text", record.chunks.get(index).text);
        }
        doc.set("metadata", record.metadata);
        doc.put(AstraDataApi.HASH, AstraJson.identity(doc));
        return doc;
    }

    private static String route(ObjectNode input, String path, String fallback, boolean empty)
            throws IOException {
        if (path.isEmpty()) {
            return fallback;
        }
        JsonNode value = AstraPaths.one(input, path);
        if (!value.isTextual()) {
            throw new IOException("Astra sink: row routing field must be a string");
        }
        return AstraConfig.scope(value.textValue(), empty);
    }

    private String recordId(ObjectNode input) throws IOException {
        ArrayNode keys = AstraJson.array();
        for (String key : config.primaryKeys) {
            JsonNode value = AstraPaths.one(input, key);
            if (value.isNull()
                    || value.isMissingNode()
                    || !value.isValueNode()
                    || !(value.isTextual() || value.isNumber() || value.isBoolean())) {
                throw new IOException(
                        "Astra sink: primary key components must be nonnull scalar values");
            }
            if (value.isNumber()) {
                keys.add(DecimalNode.valueOf(value.decimalValue().stripTrailingZeros()));
            } else {
                keys.add(value);
            }
        }
        return AstraJson.identity(keys);
    }

    private static String text(ObjectNode selected) {
        StringBuilder text = new StringBuilder();
        Iterator<Map.Entry<String, JsonNode>> fields = selected.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (text.length() != 0) {
                text.append('\n');
            }
            JsonNode content = field.getValue();
            text.append(field.getKey())
                    .append(": ")
                    .append(content.isTextual() ? content.textValue() : content.toString());
        }
        return text.toString();
    }

    private static void validateMetadata(JsonNode node, int nesting, int[] left)
            throws IOException {
        if (nesting > 16) {
            throw new IOException("Astra sink: metadata exceeds the field or nesting budget");
        }
        if (node.isObject()) {
            left[0] -= node.size();
            if (left[0] < 0) {
                throw new IOException("Astra sink: indexed metadata exceeds the field budget");
            }
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (!AstraConfig.field(field.getKey())) {
                    throw new IOException("Astra sink: metadata has an unsupported field name");
                }
                validateMetadata(field.getValue(), nesting + 1, left);
            }
        } else if (node.isArray()) {
            if (node.size() > 1000) {
                throw new IOException("Astra sink: indexed metadata array exceeds 1000 elements");
            }
            for (JsonNode value : node) {
                validateMetadata(value, nesting + 1, left);
            }
        } else if (node.isNumber() && node.toString().length() > 100) {
            throw new IOException("Astra sink: metadata numeric value exceeds 100 characters");
        }
    }

    /**
     * Represents a namespace and stream independently from their hash used by filtered deletes.
     * Length-delimited JSON input avoids concatenation collisions between names.
     */
    static final class Scope {
        final String namespace;
        final String stream;
        final String key;

        Scope(String namespace, String stream) {
            this.namespace = namespace;
            this.stream = stream;
            key = AstraJson.identity(Arrays.asList(namespace, stream));
        }
    }

    /**
     * Holds text and its validated existing vector when the source provides embeddings. Remote
     * embeddings fill the corresponding position later in the flush.
     */
    static final class Chunk {
        final String text;
        final ArrayNode vector;

        Chunk(String text, ArrayNode vector) {
            this.text = text;
            this.vector = vector;
        }
    }

    /**
     * Keeps all chunks of one row together until their validation and scoped update are complete. A
     * tombstone has an exact record identity and an empty chunk list.
     */
    static final class Record {
        final Scope scope;
        final String id;
        final long position;
        final ObjectNode metadata;
        final List<Chunk> chunks;
        final long bytes;

        Record(
                Scope scope,
                String id,
                long position,
                ObjectNode metadata,
                List<Chunk> chunks,
                long bytes) {
            this.scope = scope;
            this.id = id;
            this.position = position;
            this.metadata = metadata;
            this.chunks = chunks;
            this.bytes = bytes;
        }
    }
}
