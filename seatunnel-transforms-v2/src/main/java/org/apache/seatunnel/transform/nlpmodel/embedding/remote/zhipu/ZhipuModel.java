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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.zhipu;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Zhipu model. Refer <a href="https://bigmodel.cn/dev/api/vector/embedding">embedding api </a> */
public class ZhipuModel extends AbstractModel {

    private final CloseableHttpClient client;
    private final String model;
    private final String apiKey;
    private final String apiPath;;
    private final Integer dimension;
    private final Integer MAX_INPUT_SIZE = 64;

    public ZhipuModel(
            String apiKey,
            String model,
            String apiPath,
            Integer dimension,
            Integer vectorizedNumber)
            throws IOException {
        super(vectorizedNumber);
        this.model = model;
        this.apiKey = apiKey;
        this.apiPath = apiPath;
        this.dimension = dimension;
        this.client = HttpClients.createDefault();
    }

    @Override
    public List<List<Double>> vector(Object[] fields) throws IOException {
        return vectorGeneration(fields);
    }

    @Override
    public Integer dimension() throws IOException {
        return dimension;
    }

    private List<List<Double>> vectorGeneration(Object[] fields) throws IOException {

        if (fields == null || fields.length > MAX_INPUT_SIZE) {
            throw new IOException(
                    "Zhipu input text for vectorization, with a maximum limit of 64 entries.");
        }
        HttpPost post = new HttpPost(apiPath);
        post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setConfig(
                RequestConfig.custom().setConnectTimeout(20000).setSocketTimeout(20000).build());

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(createJsonNodeFromData(fields)),
                        StandardCharsets.UTF_8.name()));

        CloseableHttpResponse response = client.execute(post);
        String responseStr = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to get vector from zhipu, response: " + responseStr);
        }
        JsonNode data = OBJECT_MAPPER.readTree(responseStr).get("data");
        List<List<Double>> embeddings = new ArrayList<>();

        if (data.isArray()) {
            for (JsonNode node : data) {
                JsonNode embeddingNode = node.get("embedding");
                List<Double> embedding =
                        OBJECT_MAPPER.readValue(
                                embeddingNode.traverse(), new TypeReference<List<Double>>() {});
                embeddings.add(embedding);
            }
        }
        return embeddings;
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(Object[] fields) {
        ArrayNode arrayNode = OBJECT_MAPPER.valueToTree(Arrays.asList(fields));
        return OBJECT_MAPPER
                .createObjectNode()
                .put("model", model)
                .put("dimensions", dimension)
                .set("input", arrayNode);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
