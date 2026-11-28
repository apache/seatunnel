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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.astra.AstraConfig.Embedding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Obtains one validated vector for each input chunk from the selected service or row field. Remote
 * response order and dimensions are checked before database mutations can begin.
 */
final class AstraEmbeddings {
    private static final int MAX_EMBED_BATCH = 16;
    private final AstraConfig config;
    private final AstraHttp http;

    AstraEmbeddings(AstraConfig config, AstraHttp http) {
        this.config = config;
        this.http = http;
    }

    List<ArrayNode> embed(List<AstraDocument.Chunk> chunks) throws IOException {
        if (!config.embedding.remote) {
            List<ArrayNode> result = new ArrayList<>(chunks.size());
            for (AstraDocument.Chunk chunk : chunks) {
                result.add(
                        config.embedding == Embedding.FAKE ? simulated(chunk.text) : chunk.vector);
            }
            return result;
        }
        List<ArrayNode> output = new ArrayList<>(chunks.size());
        int start = 0;
        while (start < chunks.size()) {
            int limit = Math.min(chunks.size(), start + MAX_EMBED_BATCH);
            ObjectNode request = request(chunks.subList(start, limit));
            while (AstraJson.bytes(request).length > config.maxBody && limit > start + 1) {
                limit = start + (limit - start) / 2;
                request = request(chunks.subList(start, limit));
            }
            boolean azure = config.embedding == Embedding.AZURE_OPENAI;
            String secret =
                    config.embeddingToken.isEmpty()
                            ? ""
                            : azure ? config.embeddingToken : "Bearer " + config.embeddingToken;
            JsonNode response =
                    http.post(
                            config.embeddingUrl,
                            azure ? AstraConfig.AZURE_KEY : AstraConfig.BEARER,
                            secret,
                            request,
                            "embedding",
                            true);
            output.addAll(decode(response, limit - start));
            start = limit;
        }
        return output;
    }

    private ObjectNode request(List<AstraDocument.Chunk> chunks) {
        ObjectNode request = AstraJson.object().put("model", config.embeddingModel);
        boolean cohere = config.embedding == Embedding.COHERE;
        ArrayNode texts = request.putArray(cohere ? "texts" : "input");
        for (AstraDocument.Chunk chunk : chunks) {
            texts.add(chunk.text);
        }
        if (cohere) {
            request.put("input_type", "search_document")
                    .put("truncate", "NONE")
                    .putArray("embedding_types")
                    .add("float");
        } else if (config.embedding != Embedding.OPENAI_COMPATIBLE) {
            request.put("encoding_format", "float");
        }
        return request;
    }

    private List<ArrayNode> decode(JsonNode response, int count) throws IOException {
        if (response.hasNonNull("error")
                || response.hasNonNull("errors") && !response.path("errors").isEmpty()) {
            throw AstraHttp.failure("embedding", "provider rejected the request");
        }
        boolean cohere = config.embedding == Embedding.COHERE;
        JsonNode data = cohere ? response.path("embeddings").path("float") : response.path("data");
        if (!data.isArray() || data.size() != count) {
            throw AstraHttp.failure("embedding", "response count differs from requested chunks");
        }
        List<ArrayNode> output = new ArrayList<>(Collections.nCopies(count, null));
        for (int i = 0; i < count; i++) {
            JsonNode item = data.get(i);
            if (cohere) {
                output.set(i, vector(item, config.dimensions));
            } else {
                JsonNode index = item.path("index");
                if (!index.isIntegralNumber()
                        || !index.canConvertToInt()
                        || index.intValue() < 0
                        || index.intValue() >= count
                        || output.get(index.intValue()) != null) {
                    throw AstraHttp.failure(
                            "embedding", "missing, duplicate or invalid result index");
                }
                output.set(index.intValue(), vector(item.path("embedding"), config.dimensions));
            }
        }
        return output;
    }

    /**
     * Requires the collection dimension and finite float-compatible numeric components. An all-zero
     * vector cannot provide a useful cosine score.
     */
    static ArrayNode vector(JsonNode raw, int dimensions) throws IOException {
        if (!raw.isArray() || raw.size() != dimensions) {
            throw AstraHttp.failure("embedding", "vector dimensions do not match the collection");
        }
        boolean nonzero = false;
        ArrayNode vector = AstraJson.array();
        for (JsonNode component : raw) {
            if (!component.isNumber()) {
                throw AstraHttp.failure(
                        "embedding", "vector contains a nonfinite or nonnumeric component");
            }
            float value = component.floatValue();
            if (!Float.isFinite(value)) {
                throw AstraHttp.failure(
                        "embedding", "vector contains a nonfinite or nonnumeric component");
            }
            nonzero |= value != 0.0f;
            vector.add(value);
        }
        if (!nonzero) {
            throw AstraHttp.failure("embedding", "a cosine vector must not be all zero");
        }
        return vector;
    }

    private ArrayNode simulated(String text) {
        byte[] digest = AstraJson.sha256(text.getBytes(StandardCharsets.UTF_8));
        SplittableRandom generator = new SplittableRandom(ByteBuffer.wrap(digest).getLong());
        ArrayNode result = AstraJson.array();
        for (int i = 0; i < config.dimensions; i++) {
            result.add((float) generator.nextDouble(-1.0, 1.0));
        }
        return result;
    }
}
