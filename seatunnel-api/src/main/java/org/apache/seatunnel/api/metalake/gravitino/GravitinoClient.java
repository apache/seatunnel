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

package org.apache.seatunnel.api.metalake.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.metalake.MetalakeClient;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class GravitinoClient implements MetalakeClient {

    private static final String HEADER_ACCEPT = "Accept";
    private static final String MEDIA_TYPE_GRAVITINO_V1 = "application/vnd.gravitino.v1+json";
    private static final String JSON_FIELD_CATALOG = "catalog";
    private static final String JSON_FIELD_TABLE = "table";
    private static final String JSON_FIELD_PROPERTIES = "properties";
    private static final String ERROR_NO_RESPONSE_ENTITY = "No response entity";
    private static final String ERROR_MISSING_FIELD_TEMPLATE = "Response JSON has no '%s' field";
    private static final int MAX_RETRY_ATTEMPTS = 2;

    private static volatile CloseableHttpClient httpClient;

    @Override
    public String getType() {
        return MetaLakeType.GRAVITINO.getType();
    }

    @Override
    public JsonNode getMetaInfo(String sourceId, String metalakeUrl) throws IOException {
        JsonNode rootNode = executeGetRequest(metalakeUrl + sourceId);
        JsonNode catalogNode = getRequiredNode(rootNode, JSON_FIELD_CATALOG);
        return getRequiredNode(catalogNode, JSON_FIELD_PROPERTIES);
    }

    @Override
    public JsonNode getTableSchema(String schemaHttpUrl) throws IOException {
        JsonNode rootNode = executeGetRequest(schemaHttpUrl);
        return getRequiredNode(rootNode, JSON_FIELD_TABLE);
    }

    /**
     * Execute HTTP GET request and return parsed JSON response.
     *
     * @param url the request URL
     * @return parsed JSON root node
     * @throws IOException if network or parsing error occurs
     */
    private JsonNode executeGetRequest(String url) throws IOException {
        IOException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            HttpGet request = new HttpGet(url);
            request.addHeader(HEADER_ACCEPT, MEDIA_TYPE_GRAVITINO_V1);

            try (CloseableHttpResponse response = getHttpClient().execute(request)) {
                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new RuntimeException(ERROR_NO_RESPONSE_ENTITY);
                }
                try {
                    return JsonUtils.readTree(entity.getContent());
                } finally {
                    EntityUtils.consume(entity);
                }
            } catch (IOException e) {
                lastException = e;
            }
        }

        throw lastException;
    }

    /**
     * Get a required child node from parent node, throw exception if not found.
     *
     * @param parentNode the parent JSON node
     * @param fieldName the field name to retrieve
     * @return the child node
     * @throws RuntimeException if the field is not present
     */
    private JsonNode getRequiredNode(JsonNode parentNode, String fieldName) {
        JsonNode node = parentNode.get(fieldName);
        if (node == null) {
            throw new RuntimeException(String.format(ERROR_MISSING_FIELD_TEMPLATE, fieldName));
        }
        return node;
    }

    /**
     * Get or create a singleton HttpClient instance.
     *
     * @return the shared HttpClient
     */
    private CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            synchronized (GravitinoClient.class) {
                if (httpClient == null) {
                    httpClient = HttpClients.createDefault();
                }
            }
        }
        return httpClient;
    }
}
