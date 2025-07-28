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

package org.apache.seatunnel.transform.nlpmodel.embedding.remote.custom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.transform.nlpmodel.CustomConfigPlaceholder;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.AbstractModel;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.jayway.jsonpath.JsonPath;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CustomModel extends AbstractModel {

    private final CloseableHttpClient client;
    private final String model;
    private final String apiPath;
    private final Map<String, String> header;
    private final Map<String, Object> body;
    private final String parse;

    public CustomModel(
            String model,
            String apiPath,
            Map<String, String> header,
            Map<String, Object> body,
            String parse,
            Integer vectorizedNumber) {
        super(vectorizedNumber);
        this.apiPath = apiPath;
        this.model = model;
        this.header = header;
        this.body = body;
        this.parse = parse;
        this.client = HttpClients.createDefault();
    }

    @Override
    protected List<List<Float>> vector(Object[] fields) throws IOException {
        return vectorGeneration(fields);
    }

    @Override
    public Integer dimension() throws IOException {
        return vectorGeneration(new Object[] {DIMENSION_EXAMPLE}).size();
    }

    private List<List<Float>> vectorGeneration(Object[] fields) throws IOException {
        HttpPost post = new HttpPost(apiPath);
        // Construct a request with custom parameters
        for (Map.Entry<String, String> entry : header.entrySet()) {
            post.setHeader(entry.getKey(), entry.getValue());
        }

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(createJsonNodeFromData(fields)), "UTF-8"));

        CloseableHttpResponse response = client.execute(post);

        String responseStr = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to get vector from custom, response: " + responseStr);
        }

        return OBJECT_MAPPER.convertValue(
                parseResponse(responseStr), new TypeReference<List<List<Float>>>() {});
    }

    @VisibleForTesting
    public Object parseResponse(String responseStr) {
        return JsonPath.parse(responseStr).read(parse);
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(Object[] fields) throws IOException {
        JsonNode rootNode = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(body));
        Iterator<Map.Entry<String, JsonNode>> bodyFields = rootNode.fields();
        while (bodyFields.hasNext()) {
            Map.Entry<String, JsonNode> field = bodyFields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            if (fieldValue.isTextual()) {
                String value = fieldValue.asText();
                if (CustomConfigPlaceholder.findPlaceholder(
                        value, CustomConfigPlaceholder.REPLACE_PLACEHOLDER_MODEL)) {
                    ((ObjectNode) rootNode)
                            .put(
                                    fieldName,
                                    CustomConfigPlaceholder.replacePlaceholders(
                                            value,
                                            CustomConfigPlaceholder.REPLACE_PLACEHOLDER_MODEL,
                                            model,
                                            null));
                } else if (CustomConfigPlaceholder.findPlaceholder(
                        value, CustomConfigPlaceholder.REPLACE_PLACEHOLDER_INPUT)) {
                    ((ObjectNode) rootNode)
                            .put(
                                    fieldName,
                                    CustomConfigPlaceholder.replacePlaceholders(
                                            value,
                                            CustomConfigPlaceholder.REPLACE_PLACEHOLDER_INPUT,
                                            fields[0].toString(),
                                            null));
                }
            } else if (fieldValue.isArray()) {
                ArrayNode arrayNode = OBJECT_MAPPER.valueToTree(Arrays.asList(fields));
                ((ObjectNode) rootNode).set(fieldName, arrayNode);
            }
        }
        return ((ObjectNode) rootNode);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
