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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.amazon;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Implementation of Amazon Bedrock embedding models. Supports both Amazon Titan and Cohere
 * embedding models.
 */
public class BedrockModel extends AbstractModel {

    private final BedrockRuntimeClient client;
    private final String modelId;
    private final String inputType;
    private final int dimension;

    /**
     * Create a BedrockModel instance with AWS credentials and region.
     *
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     * @param region AWS region
     * @param endpoint AWS endpoint
     * @param modelId Model ID (e.g., "amazon.titan-embed-text-v1", "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     */
    public BedrockModel(
            String accessKey,
            String secretKey,
            String region,
            String endpoint,
            String modelId,
            int dimension,
            int batchSize)
            throws URISyntaxException {
        this(
                createBedrockClient(accessKey, secretKey, region, endpoint),
                modelId,
                dimension,
                batchSize);
    }

    /**
     * Create a BedrockModel instance with AWS credentials, region, and input type for Cohere
     * models.
     *
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     * @param region AWS region
     * @param modelId Model ID (e.g., "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     * @param inputType Input type for Cohere models (e.g., "search_document", "search_query")
     */
    public BedrockModel(
            String accessKey,
            String secretKey,
            String region,
            String modelId,
            String endpoint,
            int dimension,
            int batchSize,
            String inputType)
            throws URISyntaxException {
        this(
                createBedrockClient(accessKey, secretKey, region, endpoint),
                modelId,
                dimension,
                batchSize,
                inputType);
    }

    /**
     * Create a BedrockModel instance with an existing BedrockRuntimeClient.
     *
     * @param client BedrockRuntimeClient instance
     * @param modelId Model ID (e.g., "amazon.titan-embed-text-v1", "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     */
    public BedrockModel(BedrockRuntimeClient client, String modelId, int dimension, int batchSize) {
        this(
                client,
                modelId,
                dimension,
                batchSize,
                modelId.startsWith("cohere.") ? "search_document" : null);
    }

    /**
     * Create a BedrockModel instance with an existing BedrockRuntimeClient and input type.
     *
     * @param client BedrockRuntimeClient instance
     * @param modelId Model ID (e.g., "amazon.titan-embed-text-v1", "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     * @param inputType Input type for Cohere models (e.g., "search_document", "search_query")
     */
    public BedrockModel(
            BedrockRuntimeClient client,
            String modelId,
            int dimension,
            int batchSize,
            String inputType) {
        super(batchSize);
        this.client = Objects.requireNonNull(client, "BedrockRuntimeClient cannot be null");
        this.modelId = Objects.requireNonNull(modelId, "Model ID cannot be null");
        this.dimension = dimension;
        this.inputType = inputType;
    }

    @Override
    public Integer dimension() {
        return dimension;
    }

    /**
     * Create a BedrockRuntimeClient with AWS credentials and region.
     *
     * @param accessKey AWS access key
     * @param secretKey AWS secret key
     * @param region AWS region
     * @return BedrockRuntimeClient instance
     */
    public static BedrockRuntimeClient createBedrockClient(
            String accessKey, String secretKey, String region, String endpoint)
            throws URISyntaxException {
        Objects.requireNonNull(accessKey, "AWS access key cannot be null");
        Objects.requireNonNull(secretKey, "AWS secret key cannot be null");
        Objects.requireNonNull(region, "AWS region cannot be null");

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        BedrockRuntimeClientBuilder builder =
                BedrockRuntimeClient.builder()
                        .region(Region.of(region))
                        .endpointOverride(new URI(endpoint))
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .httpClientBuilder(
                                ApacheHttpClient.builder()
                                        .connectionMaxIdleTime(Duration.ofMillis(1))
                                        .useIdleConnectionReaper(false));

        return builder.build();
    }

    @Override
    protected List<List<Double>> vector(Object[] fields) throws IOException {
        if (fields == null || fields.length == 0) {
            return new ArrayList<>();
        }

        if (fields.length == 1) {
            ObjectNode requestBody = createRequestForSingleInput(fields[0]);
            String responseBody = invokeModel(requestBody);
            return parseSingleResponse(responseBody);
        } else {
            ObjectNode requestBody = createRequestForBatchInput(fields);
            String responseBody = invokeModel(requestBody);
            return parseBatchResponse(responseBody);
        }
    }

    public ObjectNode createRequestForSingleInput(Object input) {
        if (input == null) {
            throw new IllegalArgumentException("Input cannot be null");
        }

        String text = input.toString();
        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();

        if (modelId.startsWith("amazon.titan")) {
            requestBody.put("inputText", text);
        } else if (modelId.startsWith("cohere.")) {
            ArrayNode texts = requestBody.putArray("texts");
            texts.add(text);
            requestBody.put("input_type", inputType);
        } else {
            throw new IllegalArgumentException("Unsupported model ID: " + modelId);
        }

        return requestBody;
    }

    public ObjectNode createRequestForBatchInput(Object[] inputs) {
        if (inputs == null || inputs.length == 0) {
            throw new IllegalArgumentException("Inputs cannot be null or empty");
        }

        List<String> texts =
                Arrays.stream(inputs).map(Object::toString).collect(Collectors.toList());

        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();

        if (modelId.startsWith("amazon.titan")) {
            ArrayNode inputTexts = requestBody.putArray("inputTexts");
            texts.forEach(inputTexts::add);
        } else if (modelId.startsWith("cohere.")) {
            ArrayNode textsArray = requestBody.putArray("texts");
            texts.forEach(textsArray::add);
            requestBody.put("input_type", inputType);
        } else {
            throw new IllegalArgumentException("Unsupported model ID: " + modelId);
        }

        return requestBody;
    }

    private List<List<Double>> parseSingleResponse(String responseBody) throws IOException {
        try {
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            List<List<Double>> result = new ArrayList<>();

            if (modelId.startsWith("amazon.titan")) {
                JsonNode embedding = responseJson.get("embedding");
                if (embedding != null && embedding.isArray()) {
                    List<Double> vector = new ArrayList<>();
                    for (JsonNode value : embedding) {
                        vector.add(value.asDouble());
                    }
                    result.add(vector);
                }
            } else if (modelId.startsWith("cohere.")) {
                JsonNode embeddings = responseJson.get("embeddings");
                if (embeddings != null && embeddings.isArray() && !embeddings.isEmpty()) {
                    List<Double> vector = new ArrayList<>();
                    for (JsonNode value : embeddings.get(0)) {
                        vector.add(value.asDouble());
                    }
                    result.add(vector);
                }
            }

            return result;
        } catch (IOException e) {
            throw new IOException("Failed to parse single response: " + responseBody, e);
        }
    }

    private List<List<Double>> parseBatchResponse(String responseBody) throws IOException {
        try {
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            List<List<Double>> result = new ArrayList<>();
            JsonNode embeddings = responseJson.get("embeddings");
            if (embeddings != null && embeddings.isArray()) {
                if (modelId.startsWith("amazon.titan")) {
                    for (JsonNode embedding : embeddings) {
                        List<Double> vector = new ArrayList<>();
                        for (JsonNode value : embedding) {
                            vector.add(value.asDouble());
                        }
                        result.add(vector);
                    }

                } else if (modelId.startsWith("cohere.")) {
                    for (JsonNode embedding : embeddings) {
                        List<Double> vector = new ArrayList<>();
                        for (JsonNode value : embedding) {
                            vector.add(value.asDouble());
                        }
                        result.add(vector);
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new IOException("Failed to parse batch response: " + responseBody, e);
        }
    }

    private String invokeModel(ObjectNode requestBody) {
        String requestString = requestBody.toString();
        InvokeModelRequest request =
                InvokeModelRequest.builder()
                        .modelId(modelId)
                        .body(SdkBytes.fromString(requestString, StandardCharsets.UTF_8))
                        .build();

        InvokeModelResponse response = client.invokeModel(request);
        return response.body().asString(StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
