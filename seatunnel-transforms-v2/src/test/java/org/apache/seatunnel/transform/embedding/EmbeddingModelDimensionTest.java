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

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.openai.OpenAIModel;

import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class EmbeddingModelDimensionTest {

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
        }
    }

    private List<Float> generateVector(int dimension) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; i++) {
            vector.add(ThreadLocalRandom.current().nextFloat());
        }
        return vector;
    }
}
