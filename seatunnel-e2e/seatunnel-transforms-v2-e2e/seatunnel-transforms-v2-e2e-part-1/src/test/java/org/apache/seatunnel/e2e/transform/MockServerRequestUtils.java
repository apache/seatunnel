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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class MockServerRequestUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private MockServerRequestUtils() {}

    static ArrayNode retrieveRequests(
            GenericContainer<?> mockserverContainer, String method, String path)
            throws IOException {
        String endpoint =
                String.format(
                        "http://%s:%d/mockserver/retrieve?type=REQUESTS",
                        mockserverContainer.getHost(), mockserverContainer.getMappedPort(1080));

        HttpPut request = new HttpPut(endpoint);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(
                new StringEntity(createMatcher(method, path).toString(), StandardCharsets.UTF_8));

        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(request)) {
            String responseBody =
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            Assertions.assertEquals(
                    HttpStatus.SC_OK, response.getStatusLine().getStatusCode(), responseBody);
            JsonNode requests = OBJECT_MAPPER.readTree(responseBody);
            Assertions.assertTrue(requests.isArray(), responseBody);
            return (ArrayNode) requests;
        }
    }

    private static ObjectNode createMatcher(String method, String path) {
        ObjectNode matcher = OBJECT_MAPPER.createObjectNode();
        matcher.put("method", method);
        matcher.put("path", path);
        return matcher;
    }
}
