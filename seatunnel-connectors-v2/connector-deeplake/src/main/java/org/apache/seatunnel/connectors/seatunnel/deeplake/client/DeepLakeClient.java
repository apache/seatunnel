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

package org.apache.seatunnel.connectors.seatunnel.deeplake.client;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.deeplake.exception.DeepLakeConnectorException;

import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeepLakeClient implements Closeable {

    static final String ORGANIZATION_HEADER = "X-Activeloop-Org-Id";

    private final DeepLakeSinkConfig config;
    private final CloseableHttpClient httpClient;
    private final String queryUrl;
    private final String batchQueryUrl;

    public DeepLakeClient(DeepLakeSinkConfig config) {
        this.config = config;
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(config.getConnectTimeoutMs())
                        .setConnectionRequestTimeout(config.getConnectTimeoutMs())
                        .setSocketTimeout(config.getSocketTimeoutMs())
                        .build();
        this.httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
        String workspace = encodePathSegment(config.getWorkspace());
        this.queryUrl = config.getApiUrl() + "/workspaces/" + workspace + "/tables/query";
        this.batchQueryUrl = queryUrl + "/batch";
    }

    public void execute(String query) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query);
        post(queryUrl, body);
    }

    public void executeBatch(String query, List<List<Object>> rows) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query);
        body.put("params_batch", rows);
        post(batchQueryUrl, body);
    }

    private void post(String url, Map<String, Object> body) {
        HttpPost request = new HttpPost(url);
        request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + config.getApiKey());
        request.setHeader(ORGANIZATION_HEADER, config.getOrgId());
        request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        request.setEntity(
                new StringEntity(JsonUtils.toJsonString(body), ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            String responseBody =
                    response.getEntity() == null
                            ? ""
                            : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            if (status < 200 || status >= 300) {
                throw new DeepLakeConnectorException(
                        DeepLakeConnectorErrorCode.REQUEST_FAILED,
                        "Deep Lake request failed with HTTP "
                                + status
                                + (responseBody.isEmpty() ? "" : ": " + responseBody));
            }
        } catch (IOException e) {
            throw new DeepLakeConnectorException(
                    DeepLakeConnectorErrorCode.REQUEST_FAILED, "Deep Lake request failed", e);
        }
    }

    private static String encodePathSegment(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20");
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Deep Lake workspace", e);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
