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

import org.apache.seatunnel.transform.nlpmodel.ModelInvocationContext;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationErrorType;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationOptions;
import org.apache.seatunnel.transform.nlpmodel.ProviderAdapter;
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
        this(
                apiKey,
                model,
                apiPath,
                vectorizedNumber,
                ModelInvocationOptions.defaults(),
                HttpClients.createDefault());
    }

    public OpenAIModel(
            String apiKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            ModelInvocationOptions invocationOptions) {
        this(
                apiKey,
                model,
                apiPath,
                vectorizedNumber,
                invocationOptions,
                HttpClients.createDefault());
    }

    public OpenAIModel(
            String apiKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            CloseableHttpClient client) {
        this(apiKey, model, apiPath, vectorizedNumber, ModelInvocationOptions.defaults(), client);
    }

    public OpenAIModel(
            String apiKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            ModelInvocationOptions invocationOptions,
            CloseableHttpClient client) {
        super(vectorizedNumber, invocationOptions);
        this.apiKey = apiKey;
        this.model = model;
        this.apiPath = apiPath;
        this.client = client;
    }

    @Override
    protected List<List<Float>> vector(Object[] fields) throws IOException {
        if (fields.length > 1) {
            throw new IllegalArgumentException("OpenAI model only supports single input");
        }
        return invocationRuntime.invoke(
                fields,
                new ProviderAdapter<List<List<Float>>>() {
                    @Override
                    public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                            throws IOException {
                        return vectorGeneration(inputs, context.getRequestTimeoutMs());
                    }

                    @Override
                    public int getOutputCount(List<List<Float>> output) {
                        return output == null ? 0 : output.size();
                    }

                    @Override
                    public String getProvider() {
                        return "OPENAI";
                    }

                    @Override
                    public String getModel() {
                        return model;
                    }
                });
    }

    @Override
    public Integer dimension() throws IOException {
        return vector(new Object[] {DIMENSION_EXAMPLE}).get(0).size();
    }

    private List<List<Float>> vectorGeneration(Object[] fields, int requestTimeoutMs)
            throws IOException {
        HttpPost post = new HttpPost(apiPath);
        post.setHeader("Authorization", "Bearer " + apiKey);
        post.setHeader("Content-Type", "application/json");
        post.setConfig(
                RequestConfig.custom()
                        .setConnectTimeout(requestTimeoutMs)
                        .setSocketTimeout(requestTimeoutMs)
                        .build());

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(createJsonNodeFromData(fields)), "UTF-8"));

        try (CloseableHttpResponse response = client.execute(post)) {
            String responseStr = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200) {
                throw ModelInvocationException.fromHttpStatus(
                        "OPENAI", model, response.getStatusLine().getStatusCode(), responseStr);
            }

            try {
                JsonNode data = OBJECT_MAPPER.readTree(responseStr).get("data");
                List<List<Float>> embeddings = new ArrayList<>();

                if (data.isArray()) {
                    for (JsonNode node : data) {
                        JsonNode embeddingNode = node.get("embedding");
                        List<Float> embedding =
                                OBJECT_MAPPER.readValue(
                                        embeddingNode.traverse(),
                                        new TypeReference<List<Float>>() {});
                        embeddings.add(embedding);
                    }
                }
                return embeddings;
            } catch (IOException | RuntimeException e) {
                throw ModelInvocationException.nonRetryable(
                        ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                        "OPENAI",
                        model,
                        "Failed to parse OpenAI embedding response",
                        e);
            }
        }
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
