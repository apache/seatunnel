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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.common.utils.JsonUtils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class GravitinoClient implements MetalakeClient {
    private final String metalakeUrl;

    public GravitinoClient(String metalakeUrl) {
        this.metalakeUrl = metalakeUrl;
    }

    @Override
    public String getType() {
        return "gravitino";
    }

    @Override
    public JsonNode getMetaInfo(String sourceId) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(this.metalakeUrl + sourceId);
            request.addHeader("Accept", "application/vnd.gravitino.v1+json");
            try (CloseableHttpResponse response = client.execute(request)) {
                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new RuntimeException("No response entity");
                }
                JsonNode rootNode = JsonUtils.readTree(entity.getContent());
                EntityUtils.consume(entity);
                JsonNode catalogNode = rootNode.get("catalog");
                if (catalogNode == null) {
                    throw new RuntimeException("Response JSON has no 'catalog' field");
                }
                JsonNode propertiesNode = catalogNode.get("properties");
                return propertiesNode;
            }
        }
    }
}
