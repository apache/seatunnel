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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode.FLUSH_DATA_FAILED;

@Slf4j
public class HttpHelper {
    private static final int DEFAULT_CONNECT_TIMEOUT = 1000000;

    public HttpEntity getHttpEntity(CloseableHttpResponse resp) {
        int code = resp.getStatusLine().getStatusCode();
        if (HttpStatus.SC_OK != code) {
            log.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            log.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }

    public String doHttpPost(String postUrl, Map<String, String> header, String postBody)
            throws IOException {
        log.info("Executing POST from {}.", postUrl);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(postUrl);
            if (null != header) {
                for (Map.Entry<String, String> entry : header.entrySet()) {
                    httpPost.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            httpPost.setEntity(new ByteArrayEntity(postBody.getBytes()));
            try (CloseableHttpResponse resp = httpClient.execute(httpPost)) {
                HttpEntity respEntity = getHttpEntity(resp);
                return respEntity != null ? EntityUtils.toString(respEntity, "UTF-8") : null;
            }
        }
    }

    public String doHttpExecute(HttpClientBuilder clientBuilder, HttpRequestBase httpRequestBase)
            throws IOException {
        if (Objects.isNull(clientBuilder)) clientBuilder = getDefaultClientBuilder();
        try (CloseableHttpClient client = clientBuilder.build()) {
            try (CloseableHttpResponse response = client.execute(httpRequestBase)) {
                return parseHttpResponse(response, httpRequestBase.getMethod());
            }
        }
    }

    public String parseHttpResponse(CloseableHttpResponse response, String requestType)
            throws StarRocksConnectorException {
        int code = response.getStatusLine().getStatusCode();
        if (307 == code) {
            String errorMsg =
                    String.format(
                            "Request %s failed because http response code is 307 which means 'Temporary Redirect'. "
                                    + "This can happen when FE responds the request slowly , you should find the reason first. The reason may be "
                                    + "StarRocks FE/ENGINE GC, network delay, or others. response status line: %s",
                            requestType, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        } else if (200 != code) {
            String errorMsg =
                    String.format(
                            "Request %s failed because http response code is not 200. response status line: %s",
                            requestType, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        }

        HttpEntity respEntity = response.getEntity();
        if (respEntity == null) {
            String errorMsg =
                    String.format(
                            "Request %s failed because response entity is null. response status line: %s",
                            requestType, response.getStatusLine());
            log.error("{}", errorMsg);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg);
        }

        try {
            return EntityUtils.toString(respEntity);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Request %s failed because fail to convert response entity to string. "
                                    + "response status line: %s, response entity: %s",
                            requestType, response.getStatusLine(), response.getEntity());
            log.error("{}", errorMsg, e);
            throw new StarRocksConnectorException(FLUSH_DATA_FAILED, errorMsg, e);
        }
    }

    public boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e) {
            log.warn("Failed to connect to address:{}", host, e);
            return false;
        }
    }

    private HttpClientBuilder getDefaultClientBuilder() {
        return HttpClients.custom()
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        });
    }
}
