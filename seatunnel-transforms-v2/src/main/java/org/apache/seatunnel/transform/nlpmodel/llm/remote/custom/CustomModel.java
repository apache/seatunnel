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

package org.apache.seatunnel.transform.nlpmodel.llm.remote.custom;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.nlpmodel.CustomConfigPlaceholder;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.AbstractModel;

import org.apache.groovy.util.Maps;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.JsonPath;

import java.io.IOException;
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
            SeaTunnelRowType rowType,
            SqlType outputType,
            List<String> projectionColumns,
            String prompt,
            String model,
            String apiPath,
            Map<String, String> header,
            Map<String, Object> body,
            String parse) {
        super(rowType, outputType, projectionColumns, prompt);
        this.apiPath = apiPath;
        this.model = model;
        this.header = header;
        this.body = body;
        this.parse = parse;
        this.client = HttpClients.createDefault();
    }

    @Override
    protected List<String> chatWithModel(String promptWithLimit, String rowsJson)
            throws IOException {
        HttpPost post = new HttpPost(apiPath);
        // Construct a request with custom parameters
        for (Map.Entry<String, String> entry : header.entrySet()) {
            post.setHeader(entry.getKey(), entry.getValue());
        }

        post.setEntity(
                new StringEntity(
                        OBJECT_MAPPER.writeValueAsString(
                                createJsonNodeFromData(promptWithLimit, rowsJson)),
                        "UTF-8"));

        CloseableHttpResponse response = client.execute(post);

        String responseStr = EntityUtils.toString(response.getEntity());

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Failed to get vector from custom, response: " + responseStr);
        }
        return OBJECT_MAPPER.convertValue(
                parseResponse(responseStr), new TypeReference<List<String>>() {});
    }

    @VisibleForTesting
    public Object parseResponse(String responseStr) {
        return JsonPath.parse(responseStr).read(parse);
    }

    @VisibleForTesting
    public ObjectNode createJsonNodeFromData(String prompt, String data) throws IOException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(body));
        Map<String, String> placeholderValues =
                Maps.of(
                        CustomConfigPlaceholder.REPLACE_PLACEHOLDER_INPUT, data,
                        CustomConfigPlaceholder.REPLACE_PLACEHOLDER_PROMPT, prompt,
                        CustomConfigPlaceholder.REPLACE_PLACEHOLDER_MODEL, model);

        return (ObjectNode) replacePlaceholders(jsonNode, placeholderValues);
    }

    private static JsonNode replacePlaceholders(
            JsonNode node, Map<String, String> placeholderValues) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                objectNode.set(
                        field.getKey(), replacePlaceholders(field.getValue(), placeholderValues));
            }
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                arrayNode.set(i, replacePlaceholders(arrayNode.get(i), placeholderValues));
            }
        } else if (node.isTextual()) {
            String textValue = node.asText();
            for (Map.Entry<String, String> entry : placeholderValues.entrySet()) {
                if (CustomConfigPlaceholder.findPlaceholder(textValue, entry.getKey())) {
                    textValue =
                            CustomConfigPlaceholder.replacePlaceholders(
                                    textValue, entry.getKey(), entry.getValue(), null);
                }
            }
            return new TextNode(textValue);
        }
        return node;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
