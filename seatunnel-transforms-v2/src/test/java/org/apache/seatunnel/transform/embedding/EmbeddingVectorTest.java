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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class EmbeddingVectorTest {
    private static class MockApiModel extends AbstractModel {

        public MockApiModel() {
            super(1);
        }

        @Override
        protected List<List<Float>> vector(Object[] fields) throws IOException {
            String mockApiResponse = createMockApiResponse(fields);
            return parseApiResponse(mockApiResponse);
        }

        private String createMockApiResponse(Object[] fields) {
            ObjectNode response = OBJECT_MAPPER.createObjectNode();
            response.put("object", "list");
            response.put("model", "text-embedding-3-small");

            ArrayNode dataArray = OBJECT_MAPPER.createArrayNode();

            for (int i = 0; i < fields.length; i++) {
                ObjectNode embeddingObj = OBJECT_MAPPER.createObjectNode();
                embeddingObj.put("object", "embedding");
                embeddingObj.put("index", i);
                ArrayNode embeddingArray = OBJECT_MAPPER.createArrayNode();
                embeddingArray.add(-0.006929283495992422);
                embeddingArray.add(-0.005336422007530928);
                embeddingArray.add(-4.547132266452536e-05);
                embeddingArray.add(-0.024047505110502243);

                embeddingObj.set("embedding", embeddingArray);
                dataArray.add(embeddingObj);
            }

            response.set("data", dataArray);

            ObjectNode usage = OBJECT_MAPPER.createObjectNode();
            usage.put("prompt_tokens", 5);
            usage.put("total_tokens", 5);
            response.set("usage", usage);

            return response.toString();
        }

        private List<List<Float>> parseApiResponse(String responseStr) throws IOException {
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseStr);
            JsonNode data = responseJson.get("data");
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

        @Override
        public Integer dimension() throws IOException {
            return 4;
        }

        @Override
        public void close() throws IOException {}
    }

    /**
     * Currently, when the embedding model returns a type of double, it gets converted to float,
     * resulting in a loss of precision.
     */
    @Test
    public void testVectorPrecision() throws IOException {
        MockApiModel model = new MockApiModel();
        Object[] inputFields = {"test input"};
        List<ByteBuffer> result = model.vectorization(inputFields);
        ByteBuffer buffer = result.get(0);
        Float[] embedding = BufferUtils.toFloatArray(buffer);
        Assertions.assertEquals(4, embedding.length);
        Assertions.assertEquals(-0.0069292835f, embedding[0]);
        Assertions.assertEquals(-0.005336422f, embedding[1]);
        Assertions.assertEquals(-4.5471323E-5f, embedding[2]);
        Assertions.assertEquals(-0.024047505f, embedding[3]);

        model.close();
    }
}
