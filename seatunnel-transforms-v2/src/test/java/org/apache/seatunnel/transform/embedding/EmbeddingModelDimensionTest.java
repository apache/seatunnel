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

package org.apache.seatunnel.transform.embedding;

import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationOptions;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.Model;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.openai.OpenAIModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.qianfan.QianfanModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.zhipu.ZhipuModel;

import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EmbeddingModelDimensionTest {

    @ParameterizedTest
    @CsvSource({
        "OPENAI, false, 0",
        "CUSTOM, false, 0",
        "ZHIPU, false, 0",
        "QIANFAN, false, 0",
        "DOUBAO, false, 0",
        "DOUBAO_MULTIMODAL, false, 0",
        "OPENAI, true, 0",
        "OPENAI, false, 3000"
    })
    void testHttpProvidersPropagateRetryAfterAndStopAtCap(
            String provider, boolean largeRequest, long responseDelayMs) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/embeddings",
                exchange -> {
                    try {
                        int request = requests.incrementAndGet();
                        byte[] requestBuffer = new byte[8192];
                        while (exchange.getRequestBody().read(requestBuffer) != -1) {
                            // Consume the POST before replying to avoid transport retries on close.
                        }
                        if (request == 1 && responseDelayMs > 0) {
                            try {
                                Thread.sleep(responseDelayMs);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IOException(interrupted);
                            }
                        }
                        exchange.getResponseHeaders().set("Retry-After", "2");
                        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(429, body.length);
                        exchange.getResponseBody().write(body);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/embeddings";
        // Keep socket I/O headroom separate from the retry cap being tested.
        ModelInvocationOptions options = new ModelInvocationOptions(3, 0, 1000, 10_000);
        String document = largeRequest ? "x".repeat(8 * 1024 * 1024) : "document";
        try (Model model = httpModel(provider, endpoint, options)) {
            ModelInvocationException exception =
                    Assertions.assertThrows(
                            ModelInvocationException.class,
                            () -> {
                                if ("DOUBAO_MULTIMODAL".equals(provider)) {
                                    model.dimension();
                                } else {
                                    model.vectorization(new Object[] {document});
                                }
                            });
            Assertions.assertEquals(2000L, exception.getRetryAfterMs());
            Assertions.assertEquals(429, exception.getHttpStatus());
            Assertions.assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    private static Model httpModel(String provider, String endpoint, ModelInvocationOptions options)
            throws IOException {
        switch (provider) {
            case "OPENAI":
                return new OpenAIModel("key", "model", endpoint, 1, options);
            case "CUSTOM":
                return new CustomModel(
                        "model",
                        endpoint,
                        new HashMap<>(),
                        new HashMap<>(),
                        "$.data[*].embedding",
                        1,
                        options);
            case "ZHIPU":
                return new ZhipuModel("key", "model", endpoint, 3, 1, options);
            case "QIANFAN":
                return new QianfanModel(
                        "key", "secret", "model", endpoint, 1, endpoint, "token", options);
            case "DOUBAO":
            case "DOUBAO_MULTIMODAL":
                return new DoubaoModel(
                        "key",
                        "model",
                        endpoint,
                        1,
                        "DOUBAO_MULTIMODAL".equals(provider),
                        options,
                        HttpClients.createDefault());
            default:
                throw new IllegalArgumentException(provider);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOpenAIWaitsForRetryAfterCooldown(boolean httpDate) throws IOException {
        AtomicInteger requests = new AtomicInteger();
        AtomicLong cooldownEnd = new AtomicLong();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/embeddings",
                exchange -> {
                    try {
                        while (exchange.getRequestBody().read() != -1) {
                            // Consume the request before reusing the HTTP connection.
                        }
                        requests.incrementAndGet();
                        long now = System.currentTimeMillis();
                        cooldownEnd.compareAndSet(
                                0L, httpDate ? (now / 1000 + 2) * 1000 : now + 1000);
                        boolean coolingDown = now < cooldownEnd.get();
                        byte[] body =
                                (coolingDown ? "{}" : embeddingResponse(1, 3))
                                        .getBytes(StandardCharsets.UTF_8);
                        String retryAfter =
                                httpDate
                                        ? DateTimeFormatter.RFC_1123_DATE_TIME
                                                .withZone(ZoneOffset.UTC)
                                                .format(Instant.ofEpochMilli(cooldownEnd.get()))
                                        : "1";
                        exchange.getResponseHeaders().set("Retry-After", retryAfter);
                        exchange.sendResponseHeaders(coolingDown ? 429 : 200, body.length);
                        exchange.getResponseBody().write(body);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        try (OpenAIModel model =
                new OpenAIModel(
                        "test-key",
                        "test-model",
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/embeddings",
                        1,
                        new ModelInvocationOptions(2, 10L, 2000L, 2000))) {
            Assertions.assertEquals(1, model.vectorization(new Object[] {"document"}).size());
            Assertions.assertEquals(2, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testCustomModelDimension() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        CustomModel model =
                new CustomModel(
                        "modelName",
                        "https://api.custom.com/v1/chat/completions",
                        new HashMap<>(),
                        new HashMap<>(),
                        "$.data[*].embedding",
                        1,
                        client);

        int dimension = ThreadLocalRandom.current().nextInt(1024, 4097);
        List<Float> vector = generateVector(dimension);
        String responseStr =
                "{\"created\":\"1753944315\",\"data\":[{\"embedding\":"
                        + vector
                        + ",\"index\":0,\"object\":\"embedding\"}],\"id\":\"021753944315445384c5dcd581d413bdefc6446277658dfef1939\",\"model\":\"doubao-embedding-text-240715\",\"object\":\"list\",\"usage\":{\"completionTokens\":0,\"promptTokens\":3,\"totalTokens\":3}}";

        try (MockedStatic<EntityUtils> entityUtils = Mockito.mockStatic(EntityUtils.class)) {
            CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
            Mockito.when(client.execute(Mockito.any())).thenReturn(response);
            Mockito.when(response.getStatusLine())
                    .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
            entityUtils
                    .when(() -> EntityUtils.toString(response.getEntity()))
                    .thenReturn(responseStr);

            Assertions.assertEquals(dimension, model.dimension());
        } finally {
            model.close();
        }
    }

    @Test
    void testDoubleModelDimension() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        DoubaoModel model =
                new DoubaoModel(
                        "apikey",
                        "modelName",
                        "https://api.doubao.io/v1/chat/completions",
                        1,
                        false,
                        client);

        int dimension = ThreadLocalRandom.current().nextInt(1024, 2561);
        List<Float> vector = generateVector(dimension);
        String responseStr =
                "{\"created\":\"1753944315\",\"data\":[{\"embedding\":"
                        + vector
                        + ",\"index\":0,\"object\":\"embedding\"}],\"id\":\"021753944315445384c5dcd581d413bdefc6446277658dfef1939\",\"model\":\"doubao-embedding-text-240715\",\"object\":\"list\",\"usage\":{\"completionTokens\":0,\"promptTokens\":3,\"totalTokens\":3}}";

        try (MockedStatic<EntityUtils> entityUtils = Mockito.mockStatic(EntityUtils.class)) {
            CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
            Mockito.when(client.execute(Mockito.any())).thenReturn(response);
            Mockito.when(response.getStatusLine())
                    .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
            entityUtils
                    .when(() -> EntityUtils.toString(response.getEntity()))
                    .thenReturn(responseStr);

            Assertions.assertEquals(dimension, model.dimension());
        } finally {
            model.close();
        }
    }

    @Test
    void testDoubaoModelDimensionIgnoresBatchResponseCount() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        DoubaoModel model =
                new DoubaoModel(
                        "apikey",
                        "modelName",
                        "https://api.doubao.io/v1/chat/completions",
                        2,
                        false,
                        client);

        CloseableHttpResponse response = okResponse(embeddingResponse(2, 3));
        Mockito.when(client.execute(Mockito.any())).thenReturn(response);

        Assertions.assertEquals(3, model.dimension());
    }

    @Test
    void testCustomModelDimensionIgnoresBatchResponseCount() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        CustomModel model =
                new CustomModel(
                        "modelName",
                        "https://api.custom.com/v1/chat/completions",
                        new HashMap<>(),
                        new HashMap<>(),
                        "$.data[*].embedding",
                        2,
                        client);

        CloseableHttpResponse response = okResponse(embeddingResponse(2, 3));
        Mockito.when(client.execute(Mockito.any())).thenReturn(response);

        Assertions.assertEquals(3, model.dimension());
    }

    @Test
    void testQianfanModelDimensionIgnoresBatchResponseCount() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        try (MockedStatic<HttpClients> httpClients = Mockito.mockStatic(HttpClients.class)) {
            CloseableHttpResponse tokenResponse = okResponse("{\"access_token\":\"token\"}");
            CloseableHttpResponse embeddingResponse = okResponse(embeddingResponse(2, 3));
            httpClients.when(HttpClients::createDefault).thenReturn(client);
            Mockito.when(
                            client.execute(
                                    Mockito.argThat(
                                            (HttpUriRequest request) ->
                                                    request instanceof HttpGet)))
                    .thenReturn(tokenResponse);
            Mockito.when(
                            client.execute(
                                    Mockito.argThat(
                                            (HttpUriRequest request) ->
                                                    request instanceof HttpPost)))
                    .thenReturn(embeddingResponse);

            QianfanModel model =
                    new QianfanModel(
                            "apikey",
                            "secretKey",
                            "modelName",
                            "https://api.qianfan.io/v1/embedding",
                            "https://api.qianfan.io/oauth",
                            2);

            Assertions.assertEquals(3, model.dimension());
        }
    }

    @Test
    void testOpenAIModelDimension() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        OpenAIModel model =
                new OpenAIModel(
                        "apikey",
                        "modelName",
                        "https://api.openai.com/v1/chat/completions",
                        1,
                        client);

        int dimension = ThreadLocalRandom.current().nextInt(1024, 1537);
        List<Float> vector = generateVector(dimension);
        String responseStr =
                "{\"object\":\"list\",\"data\":[{\"object\":\"embedding\",\"embedding\":"
                        + vector
                        + ",\"index\":0}],\"model\":\"text-embedding-ada-002\",\"usage\":{\"prompt_tokens\":8,\"total_tokens\":8}}";

        try (MockedStatic<EntityUtils> entityUtils = Mockito.mockStatic(EntityUtils.class)) {
            CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
            Mockito.when(response.getStatusLine())
                    .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
            Mockito.when(client.execute(Mockito.any())).thenReturn(response);
            entityUtils
                    .when(() -> EntityUtils.toString(response.getEntity()))
                    .thenReturn(responseStr);

            Assertions.assertEquals(dimension, model.dimension());
        } finally {
            model.close();
        }
    }

    private static CloseableHttpResponse okResponse(String responseBody) {
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        HttpEntity entity = new StringEntity(responseBody, StandardCharsets.UTF_8);
        Mockito.when(response.getStatusLine())
                .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
        Mockito.when(response.getEntity()).thenReturn(entity);
        return response;
    }

    private static String embeddingResponse(int count, int dimension) {
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                data.append(",");
            }
            data.append("{\"embedding\":").append(generateVectorJson(dimension));
            data.append(",\"index\":").append(i).append(",\"object\":\"embedding\"}");
        }
        return "{\"data\":[" + data + "],\"model\":\"modelName\",\"object\":\"list\"}";
    }

    private static String generateVectorJson(int dimension) {
        StringBuilder vector = new StringBuilder("[");
        for (int i = 0; i < dimension; i++) {
            if (i > 0) {
                vector.append(",");
            }
            vector.append(i + 1).append(".0");
        }
        vector.append("]");
        return vector.toString();
    }

    private List<Float> generateVector(int dimension) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            vector.add(ThreadLocalRandom.current().nextFloat());
        }
        return vector;
    }
}
