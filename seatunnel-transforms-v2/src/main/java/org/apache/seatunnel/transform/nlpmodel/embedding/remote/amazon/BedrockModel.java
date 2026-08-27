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

import org.apache.seatunnel.transform.nlpmodel.ModelInvocationContext;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationErrorType;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationOptions;
import org.apache.seatunnel.transform.nlpmodel.ProviderAdapter;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.apache.http.conn.ConnectTimeoutException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClientBuilder;
import software.amazon.awssdk.services.bedrockruntime.model.AccessDeniedException;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.ConflictException;
import software.amazon.awssdk.services.bedrockruntime.model.InternalServerException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;
import software.amazon.awssdk.services.bedrockruntime.model.ModelErrorException;
import software.amazon.awssdk.services.bedrockruntime.model.ModelNotReadyException;
import software.amazon.awssdk.services.bedrockruntime.model.ModelTimeoutException;
import software.amazon.awssdk.services.bedrockruntime.model.ResourceNotFoundException;
import software.amazon.awssdk.services.bedrockruntime.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.bedrockruntime.model.ServiceUnavailableException;
import software.amazon.awssdk.services.bedrockruntime.model.ThrottlingException;
import software.amazon.awssdk.services.bedrockruntime.model.ValidationException;

import java.io.IOException;
import java.net.SocketTimeoutException;
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
                modelId.startsWith("cohere.") ? "search_document" : null,
                ModelInvocationOptions.defaults());
    }

    /**
     * Create a BedrockModel instance with an existing BedrockRuntimeClient and invocation options.
     *
     * @param client BedrockRuntimeClient instance
     * @param modelId Model ID (e.g., "amazon.titan-embed-text-v1", "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     * @param invocationOptions Runtime options for retry, timeout, and backoff
     */
    public BedrockModel(
            BedrockRuntimeClient client,
            String modelId,
            int dimension,
            int batchSize,
            ModelInvocationOptions invocationOptions) {
        this(
                client,
                modelId,
                dimension,
                batchSize,
                modelId.startsWith("cohere.") ? "search_document" : null,
                invocationOptions);
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
        this(client, modelId, dimension, batchSize, inputType, ModelInvocationOptions.defaults());
    }

    /**
     * Create a BedrockModel instance with an existing BedrockRuntimeClient, input type, and
     * invocation options.
     *
     * @param client BedrockRuntimeClient instance
     * @param modelId Model ID (e.g., "amazon.titan-embed-text-v1", "cohere.embed-english-v3")
     * @param dimension Embedding dimension
     * @param batchSize Batch size for processing
     * @param inputType Input type for Cohere models (e.g., "search_document", "search_query")
     * @param invocationOptions Runtime options for retry, timeout, and backoff
     */
    public BedrockModel(
            BedrockRuntimeClient client,
            String modelId,
            int dimension,
            int batchSize,
            String inputType,
            ModelInvocationOptions invocationOptions) {
        super(batchSize, invocationOptions);
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
    protected List<List<Float>> vector(Object[] fields) throws IOException {
        if (fields == null || fields.length == 0) {
            return new ArrayList<>();
        }

        return invocationRuntime.invoke(
                fields,
                new ProviderAdapter<List<List<Float>>>() {
                    @Override
                    public List<List<Float>> invoke(Object[] inputs, ModelInvocationContext context)
                            throws IOException {
                        return vectorGeneration(inputs);
                    }

                    @Override
                    public int getOutputCount(List<List<Float>> output) {
                        return output == null ? 0 : output.size();
                    }

                    @Override
                    public String getProvider() {
                        return "BEDROCK";
                    }

                    @Override
                    public String getModel() {
                        return modelId;
                    }

                    @Override
                    public Integer getDimension() {
                        return dimension;
                    }
                });
    }

    private List<List<Float>> vectorGeneration(Object[] fields) throws IOException {
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

    private List<List<Float>> parseSingleResponse(String responseBody) throws IOException {
        try {
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            List<List<Float>> result = new ArrayList<>();

            if (modelId.startsWith("amazon.titan")) {
                JsonNode embedding = responseJson.get("embedding");
                if (embedding != null && embedding.isArray()) {
                    List<Float> vector = new ArrayList<>();
                    for (JsonNode value : embedding) {
                        vector.add(value.floatValue());
                    }
                    result.add(vector);
                }
            } else if (modelId.startsWith("cohere.")) {
                JsonNode embeddings = responseJson.get("embeddings");
                if (embeddings != null && embeddings.isArray() && !embeddings.isEmpty()) {
                    List<Float> vector = new ArrayList<>();
                    for (JsonNode value : embeddings.get(0)) {
                        vector.add(value.floatValue());
                    }
                    result.add(vector);
                }
            }

            return result;
        } catch (IOException e) {
            throw ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                    "BEDROCK",
                    modelId,
                    "Failed to parse Bedrock single response",
                    e);
        }
    }

    private List<List<Float>> parseBatchResponse(String responseBody) throws IOException {
        try {
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            List<List<Float>> result = new ArrayList<>();
            JsonNode embeddings = responseJson.get("embeddings");
            if (embeddings != null && embeddings.isArray()) {
                if (modelId.startsWith("amazon.titan")) {
                    for (JsonNode embedding : embeddings) {
                        List<Float> vector = new ArrayList<>();
                        for (JsonNode value : embedding) {
                            vector.add(value.floatValue());
                        }
                        result.add(vector);
                    }

                } else if (modelId.startsWith("cohere.")) {
                    for (JsonNode embedding : embeddings) {
                        List<Float> vector = new ArrayList<>();
                        for (JsonNode value : embedding) {
                            vector.add(value.floatValue());
                        }
                        result.add(vector);
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.RESPONSE_PARSE_ERROR,
                    "BEDROCK",
                    modelId,
                    "Failed to parse Bedrock batch response",
                    e);
        }
    }

    private String invokeModel(ObjectNode requestBody) throws IOException {
        String requestString = requestBody.toString();
        InvokeModelRequest request =
                InvokeModelRequest.builder()
                        .modelId(modelId)
                        .body(SdkBytes.fromString(requestString, StandardCharsets.UTF_8))
                        .build();

        try {
            InvokeModelResponse response = client.invokeModel(request);
            return response.body().asString(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            throw mapAwsException(e);
        }
    }

    private ModelInvocationException mapAwsException(RuntimeException exception) {
        if (exception instanceof ThrottlingException
                || exception instanceof ServiceQuotaExceededException) {
            return ModelInvocationException.retryable(
                    ModelInvocationErrorType.RATE_LIMIT,
                    "BEDROCK",
                    modelId,
                    exception.getMessage(),
                    statusCode(exception),
                    exception);
        }
        if (exception instanceof ModelTimeoutException || isTimeoutCause(exception)) {
            return ModelInvocationException.retryable(
                    ModelInvocationErrorType.TIMEOUT,
                    "BEDROCK",
                    modelId,
                    exception.getMessage(),
                    statusCode(exception),
                    exception);
        }
        if (exception instanceof InternalServerException
                || exception instanceof ServiceUnavailableException
                || exception instanceof ModelNotReadyException
                || exception instanceof ModelErrorException
                || exception instanceof SdkClientException) {
            return ModelInvocationException.retryable(
                    ModelInvocationErrorType.TEMPORARY_REMOTE_ERROR,
                    "BEDROCK",
                    modelId,
                    exception.getMessage(),
                    statusCode(exception),
                    exception);
        }
        if (exception instanceof AccessDeniedException) {
            return ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.AUTHENTICATION_ERROR,
                    "BEDROCK",
                    modelId,
                    exception.getMessage(),
                    statusCode(exception),
                    exception);
        }
        if (exception instanceof ValidationException
                || exception instanceof ResourceNotFoundException
                || exception instanceof ConflictException) {
            return ModelInvocationException.nonRetryable(
                    ModelInvocationErrorType.CONFIGURATION_ERROR,
                    "BEDROCK",
                    modelId,
                    exception.getMessage(),
                    statusCode(exception),
                    exception);
        }
        return ModelInvocationException.nonRetryable(
                ModelInvocationErrorType.UNKNOWN_REMOTE_ERROR,
                "BEDROCK",
                modelId,
                exception.getMessage(),
                statusCode(exception),
                exception);
    }

    private Integer statusCode(RuntimeException exception) {
        if (exception instanceof BedrockRuntimeException) {
            return ((BedrockRuntimeException) exception).statusCode();
        }
        return null;
    }

    private boolean isTimeoutCause(RuntimeException exception) {
        Throwable cause = exception.getCause();
        while (cause != null) {
            if (cause instanceof SocketTimeoutException
                    || cause instanceof ConnectTimeoutException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
