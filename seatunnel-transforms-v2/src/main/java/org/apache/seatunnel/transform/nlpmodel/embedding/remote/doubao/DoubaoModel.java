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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.embedding.FieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalFieldValue;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalModel;

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

public class DoubaoModel extends MultimodalModel {

    private final CloseableHttpClient client;
    private final String apiKey;
    private final String model;
    private final String apiPath;
    private final boolean isMultimodalFields;

    private final String BASE64_PARAM_TEMPLATE = "data:%s/%s;base64,%s";

    public DoubaoModel(String apiKey, String model, String apiPath, Integer vectorizedNumber) {
        this(apiKey, model, apiPath, vectorizedNumber, false, HttpClients.createDefault());
    }

    public DoubaoModel(
            String apiKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            boolean isMultimodalFields) {
        this(
                apiKey,
                model,
                apiPath,
                vectorizedNumber,
                isMultimodalFields,
                HttpClients.createDefault());
    }

    public DoubaoModel(
            String apiKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            boolean isMultimodalFields,
            CloseableHttpClient client) {
        super(vectorizedNumber);
        this.apiKey = apiKey;
        this.model = model;
        this.apiPath = apiPath;
        this.isMultimodalFields = isMultimodalFields;
        this.client = client;
    }

    @Override
    protected List<List<Float>> textVector(Object[] fields) throws IOException {
        return textVectorGeneration(fields);
    }

    @Override
    public List<List<Float>> multimodalVector(Object[] fields) throws IOException {
        if (singleVectorizedInputNumber > 1) {
            throw new IllegalArgumentException(
                    "Doubao does not support batch multimodal vectorization in a single request. ");
        }
        List<List<Float>> vectors = new ArrayList<>();
        for (Object field : fields) {
            vectors.add(multimodalVectorGeneration((MultimodalFieldValue) field));
        }
        return vectors;
    }

    @Override
    public Integer dimension() throws IOException {
        return isMultimodalFields
                ? multimodalVectorGeneration(
                                new MultimodalFieldValue(
                                        new FieldSpec(DIMENSION_EXAMPLE), DIMENSION_EXAMPLE))
                        .size()
                : textVectorGeneration(new Object[] {DIMENSION_EXAMPLE}).get(0).size();
    }

    private List<List<Float>> textVectorGeneration(Object[] fields) throws IOException {
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
            throw new IOException("Failed to get vector from doubao, response: " + responseStr);
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
    public ObjectNode createJsonNodeFromData(Object[] fields) {
        ArrayNode arrayNode = OBJECT_MAPPER.valueToTree(Arrays.asList(fields));
        return OBJECT_MAPPER.createObjectNode().put("model", model).set("input", arrayNode);
    }

    protected List<Float> multimodalVectorGeneration(MultimodalFieldValue field)
            throws IOException {

        HttpPost httpPost = new HttpPost(apiPath);
        httpPost.setHeader("Authorization", "Bearer " + apiKey);
        httpPost.setHeader("Content-Type", "application/json");

        StringEntity entity =
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(multimodalBody(field)),
                        StandardCharsets.UTF_8);
        httpPost.setEntity(entity);

        try (CloseableHttpResponse response = client.execute(httpPost)) {
            String responseBody =
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(
                        "HTTP error "
                                + response.getStatusLine().getStatusCode()
                                + ": "
                                + responseBody);
            }

            return parseMultimodalVectorResponse(responseBody);
        }
    }

    @VisibleForTesting
    public List<Float> parseMultimodalVectorResponse(String responseBody) throws IOException {
        JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
        if (responseJson.has("error")) {
            JsonNode error = responseJson.get("error");
            String errorMessage =
                    error.has("message") ? error.get("message").asText() : "Unknown error";
            throw new IOException("API error: " + errorMessage);
        }

        JsonNode dataNode = responseJson.get("data");
        if (dataNode == null) {
            throw new IOException("Invalid response format: missing or invalid 'data' field");
        }

        JsonNode embeddingArray = dataNode.get("embedding");
        if (embeddingArray == null || !embeddingArray.isArray()) {
            throw new IOException("Invalid response format: missing or invalid 'embedding' field");
        }

        List<Float> vector = new ArrayList<>();
        for (JsonNode value : embeddingArray) {
            vector.add(value.floatValue());
        }
        return vector;
    }

    @VisibleForTesting
    public ObjectNode multimodalBody(MultimodalFieldValue field) {
        ObjectNode requestNode = OBJECT_MAPPER.createObjectNode();
        requestNode.put("model", model);
        requestNode.put("encoding_format", "float");
        ArrayNode inputDatas = OBJECT_MAPPER.createArrayNode();
        inputDatas.add(inputRawData(field));
        requestNode.set("input", inputDatas);
        return requestNode;
    }

    protected ObjectNode inputRawData(MultimodalFieldValue field) {
        ObjectNode rawDataNode = OBJECT_MAPPER.createObjectNode();
        FieldSpec fieldSpec = field.getFieldSpec();
        String fieldValue = field.getValue().toString().trim();
        ModalityType fieldSpecModalityType = fieldSpec.getModalityType();
        String modalityParamName = getModalityParamName(fieldSpecModalityType);
        rawDataNode.put("type", modalityParamName);
        if (ModalityType.TEXT == fieldSpecModalityType) {
            rawDataNode.put(modalityParamName, fieldValue);
            return rawDataNode;
        }

        if (fieldSpec.isBinary()) {
            fieldValue =
                    String.format(
                            BASE64_PARAM_TEMPLATE,
                            fieldSpecModalityType.getGroup().name().toLowerCase(),
                            fieldSpecModalityType.getName(),
                            field.toBase64());
        }
        rawDataNode.set(modalityParamName, OBJECT_MAPPER.createObjectNode().put("url", fieldValue));

        return rawDataNode;
    }

    private String getModalityParamName(ModalityType inputType) {
        switch (inputType.getGroup()) {
            case IMAGE:
                return "image_url";
            case VIDEO:
                return "video_url";
            default:
                return "text";
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
