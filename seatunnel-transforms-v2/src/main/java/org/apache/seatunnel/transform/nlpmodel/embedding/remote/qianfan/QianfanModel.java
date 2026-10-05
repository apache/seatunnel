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

import org.apache.seatunnel.transform.nlpmodel.ModelInvocationContext;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationErrorType;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationOptions;
import org.apache.seatunnel.transform.nlpmodel.ProviderAdapter;
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
        this(
                apiKey,
                secretKey,
                model,
                apiPath,
                oauthPath,
                vectorizedNumber,
                ModelInvocationOptions.defaults());
    }

    public QianfanModel(
            String apiKey,
            String secretKey,
            String model,
            String apiPath,
            String oauthPath,
            Integer vectorizedNumber,
            ModelInvocationOptions invocationOptions)
            throws IOException {
        super(vectorizedNumber, invocationOptions);
        ModelInvocationOptions resolvedOptions =
                invocationOptions == null ? ModelInvocationOptions.defaults() : invocationOptions;
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        this.model = model;
        this.apiPath = apiPath;
        this.oauthPath = oauthPath;
        this.client = HttpClients.createDefault();
        this.accessToken = getAccessToken(resolvedOptions.getRequestTimeoutMs());
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
        this(
                apiKey,
                secretKey,
                model,
                apiPath,
                vectorizedNumber,
                oauthPath,
                accessToken,
                ModelInvocationOptions.defaults());
    }

    public QianfanModel(
            String apiKey,
            String secretKey,
            String model,
            String apiPath,
            Integer vectorizedNumber,
            String oauthPath,
            String accessToken,
            ModelInvocationOptions invocationOptions)
            throws IOException {
        super(vectorizedNumber, invocationOptions);
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        this.model = model;
        this.apiPath = apiPath;
        this.oauthPath = oauthPath;
        this.client = HttpClients.createDefault();
        this.accessToken = accessToken;
    }

    private String getAccessToken(int requestTimeoutMs) throws IOException {
        HttpGet get = new HttpGet(String.format(oauthPath + oauthSuffixPath, apiKey, secretKey));
        get.setConfig(
                RequestConfig.custom()
                        .setConnectTimeout(requestTimeoutMs)
                        .setSocketTimeout(requestTimeoutMs)
                        .build());
        try (CloseableHttpResponse response = client.execute(get)) {
            String responseStr = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200) {
                throw ModelInvocationException.fromHttpStatus(
                        "QIANFAN", model, response.getStatusLine().getStatusCode(), responseStr);
            }
            try {
                JsonNode result = OBJECT_MAPPER.readTree(responseStr);
                return result.get("access_token").asText();
            } catch (IOException | RuntimeException e) {
                throw ModelInvocationException.nonRetryable(
                        ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                        "QIANFAN",
                        model,
                        "Failed to parse Qianfan OAuth response",
                        e);
            }
        }
    }

    @Override
    public List<List<Float>> vector(Object[] fields) throws IOException {
        return invocationRuntime.invoke(fields, vectorAdapter(true));
    }

    @Override
    public Integer dimension() throws IOException {
        return invocationRuntime
                .invoke(new Object[] {DIMENSION_EXAMPLE}, vectorAdapter(false))
                .get(0)
                .size();
    }

    private ProviderAdapter<List<List<Float>>> vectorAdapter(boolean validateOutputCount) {
        return new ProviderAdapter<List<List<Float>>>() {
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
                return "QIANFAN";
            }

            @Override
            public String getModel() {
                return model;
            }

            @Override
            public boolean validateOutputCount() {
                return validateOutputCount;
            }
        };
    }

    private List<List<Float>> vectorGeneration(Object[] fields, int requestTimeoutMs)
            throws IOException {
        String formattedApiPath =
                String.format(
                        (apiPath.endsWith("/") ? apiPath : apiPath + "/") + "%s?access_token=%s",
                        model,
                        accessToken);
        HttpPost post = new HttpPost(formattedApiPath);
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
                        "QIANFAN", model, response.getStatusLine().getStatusCode(), responseStr);
            }

            try {
                JsonNode result = OBJECT_MAPPER.readTree(responseStr);
                JsonNode errorCode = result.get("error_code");

                if (errorCode != null) {
                    // Handle access token expiration and let the common runtime retry the request.
                    if (errorCode.asInt() == 110) {
                        this.accessToken = getAccessToken(requestTimeoutMs);
                        throw ModelInvocationException.retryable(
                                ModelInvocationErrorType.AUTHENTICATION_ERROR,
                                "QIANFAN",
                                model,
                                "Qianfan access token expired and was refreshed",
                                null,
                                null);
                    }
                    throw ModelInvocationException.nonRetryable(
                            ModelInvocationErrorType.CONFIGURATION_ERROR,
                            "QIANFAN",
                            model,
                            "Qianfan returned error_code " + errorCode.asInt(),
                            null);
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
            } catch (ModelInvocationException e) {
                throw e;
            } catch (IOException | RuntimeException e) {
                throw ModelInvocationException.nonRetryable(
                        ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                        "QIANFAN",
                        model,
                        "Failed to parse Qianfan embedding response",
                        e);
            }
        }
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
