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

package org.apache.seatunnel.transform.nlpmodel.llm.remote.kimiai;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.AbstractModel;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class KimiAIModel extends AbstractModel {

    private final CloseableHttpClient client;
    private final String apiKey;
    private final String model;
    private final String apiPath;

    public KimiAIModel(
            SeaTunnelRowType rowType,
            SqlType outputType,
            List<String> projectionColumns,
            String prompt,
            String model,
            String apiKey,
            String apiPath) {
        super(rowType, outputType, projectionColumns, prompt);
        this.apiKey = apiKey;
        this.apiPath = apiPath;
        this.model = model;
        this.client = HttpClients.createDefault();
    }

    @Override
    protected List<String> chatWithModel(String prompt, String data) throws IOException {
        HttpPost post = new HttpPost(apiPath);
        post.setHeader("Authorization", "Bearer " + apiKey);
        post.setHeader("Content-Type", "application/json");
        ObjectNode objectNode = createJsonNodeFromData(prompt, data);
        post.setEntity(new StringEntity(OBJECT_MAPPER.writeValueAsString(objectNode), "UTF-8"));
        post.setConfig(
                RequestConfig.custom().setConnectTimeout(20000).setSocketTimeout(20000).build());
        CloseableHttpResponse response = client.execute(post);
        String responseStr = EntityUtils.toString(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to chat with model, response: " + responseStr);
        }

        JsonNode result = OBJECT_MAPPER.readTree(responseStr);
        String resultData = result.get("choices").get(0).get("message").get("content").asText();
        return OBJECT_MAPPER.readValue(
                convertData(resultData), new TypeReference<List<String>>() {});
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(String prompt, String data) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put("model", model);
        ArrayNode messages = objectNode.putArray("messages");
        messages.addObject().put("role", "system").put("content", prompt);
        messages.addObject().put("role", "user").put("content", data);
        return objectNode;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
