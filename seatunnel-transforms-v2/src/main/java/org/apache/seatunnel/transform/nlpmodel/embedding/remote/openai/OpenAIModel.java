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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.openai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OpenAIModel extends AbstractModel {

    private final CloseableHttpClient client;
    private final String apiKey;
    private final String model;
    private final String apiPath;

    public OpenAIModel(String apiKey, String model, String apiPath, Integer vectorizedNumber) {
        super(vectorizedNumber);
        this.apiKey = apiKey;
        this.model = model;
        this.apiPath = apiPath;
        this.client = HttpClients.createDefault();
    }

    @Override
    protected List<List<Float>> vector(Object[] fields) throws IOException {
        if (fields.length > 1) {
            throw new IllegalArgumentException("OpenAI model only supports single input");
        }
        return vectorGeneration(fields);
    }

    @Override
    public Integer dimension() throws IOException {
        return vectorGeneration(new Object[] {DIMENSION_EXAMPLE}).size();
    }

    private List<List<Float>> vectorGeneration(Object[] fields) throws IOException {
        HttpPost post = new HttpPost(apiPath);
        post.setHeader("Authorization", "Bearer " + apiKey);
        post.setHeader("Content-Type", "application/json");
        post.setConfig(
                RequestConfig.custom().setConnectTimeout(20000).setSocketTimeout(20000).build());

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(createJsonNodeFromData(fields)), "UTF-8"));

        CloseableHttpResponse response = client.execute(post);
        String responseStr = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to get vector from openai, response: " + responseStr);
        }

        JsonNode data = OBJECT_MAPPER.readTree(responseStr).get("data");
        List<List<Float>> embeddings = new ArrayList<>();

        if (data.isArray()) {
            for (JsonNode node : data) {
                JsonNode embeddingNode = node.get("embedding");
                List<Float> embedding =
                        OBJECT_MAPPER.readValue(
                                embeddingNode.traverse(), new TypeReference<List<Float>>() {});
                embeddings.add(embedding);
            }
        }
        return embeddings;
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(Object[] data) throws JsonProcessingException {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put("model", model);
        objectNode.put("input", data[0].toString());
        return objectNode;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
