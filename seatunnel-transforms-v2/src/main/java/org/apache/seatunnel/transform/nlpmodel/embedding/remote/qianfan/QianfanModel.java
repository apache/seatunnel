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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.qianfan;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QianfanModel extends AbstractModel {

    private final CloseableHttpClient client;
    private final String apiKey;
    private final String secretKey;
    private final String model;
    private final String apiPath;
    private final String oauthPath;
    private final String oauthSuffixPath =
            "?grant_type=client_credentials&client_id=%s&client_secret=%s";
    private String accessToken;

    public QianfanModel(
            String apiKey,
            String secretKey,
            String model,
            String apiPath,
            String oauthPath,
            Integer vectorizedNumber)
            throws IOException {
        super(vectorizedNumber);
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        this.model = model;
        this.apiPath = apiPath;
        this.oauthPath = oauthPath;
        this.client = HttpClients.createDefault();
        this.accessToken = getAccessToken();
    }

    public QianfanModel(
            String apiKey,
            String secretKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            String oauthPath,
            String accessToken)
            throws IOException {
        super(vectorizedNumber);
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        this.model = model;
        this.apiPath = apiPath;
        this.oauthPath = oauthPath;
        this.client = HttpClients.createDefault();
        this.accessToken = accessToken;
    }

    private String getAccessToken() throws IOException {
        HttpGet get = new HttpGet(String.format(oauthPath + oauthSuffixPath, apiKey, secretKey));
        CloseableHttpResponse response = client.execute(get);
        String responseStr = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to Oauth for qianfan, response: " + responseStr);
        }
        JsonNode result = OBJECT_MAPPER.readTree(responseStr);
        return result.get("access_token").asText();
    }

    @Override
    public List<List<Float>> vector(Object[] fields) throws IOException {
        return vectorGeneration(fields);
    }

    @Override
    public Integer dimension() throws IOException {
        return vectorGeneration(new Object[] {DIMENSION_EXAMPLE}).get(0).size();
    }

    private List<List<Float>> vectorGeneration(Object[] fields) throws IOException {
        String formattedApiPath =
                String.format(
                        (apiPath.endsWith("/") ? apiPath : apiPath + "/") + "%s?access_token=%s",
                        model,
                        accessToken);
        HttpPost post = new HttpPost(formattedApiPath);
        post.setHeader("Content-Type", "application/json");
        post.setConfig(
                RequestConfig.custom().setConnectTimeout(20000).setSocketTimeout(20000).build());

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(createJsonNodeFromData(fields)), "UTF-8"));

        CloseableHttpResponse response = client.execute(post);
        String responseStr = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to get vector from qianfan, response: " + responseStr);
        }

        JsonNode result = OBJECT_MAPPER.readTree(responseStr);
        JsonNode errorCode = result.get("error_code");

        if (errorCode != null) {
            // Handle access token expiration
            if (errorCode.asInt() == 110) {
                this.accessToken = getAccessToken();
            }
            throw new IOException(
                    "Failed to get vector from qianfan, response: " + result.get("error_msg"));
        }

        List<List<Float>> embeddings = new ArrayList<>();
        JsonNode data = result.get("data");
        if (data.isArray()) {
            for (JsonNode node : data) {
                List<Float> embedding =
                        OBJECT_MAPPER.readValue(
                                node.get("embedding").traverse(),
                                new TypeReference<List<Float>>() {});
                embeddings.add(embedding);
            }
        }
        return embeddings;
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(Object[] data) {
        ArrayNode arrayNode = OBJECT_MAPPER.valueToTree(Arrays.asList(data));
        return OBJECT_MAPPER.createObjectNode().set("input", arrayNode);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
