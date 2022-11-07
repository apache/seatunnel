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

import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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

    public String doHttpGet(String getUrl) throws IOException {
        log.info("Executing GET from {}.", getUrl);
        try (CloseableHttpClient httpclient = buildHttpClient()) {
            HttpGet httpGet = new HttpGet(getUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    log.warn("Request failed with empty response.");
                    return null;
                }
                return EntityUtils.toString(respEntity);
            }
        }
    }

    public Map<String, Object> doHttpGet(String getUrl, Map<String, String> header) throws IOException {
        log.info("Executing GET from {}.", getUrl);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(getUrl);
            if (null != header) {
                for (Map.Entry<String, String> entry : header.entrySet()) {
                    httpGet.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (null == respEntity) {
                    log.warn("Request failed with empty response.");
                    return null;
                }
                return JsonUtils.parseObject(EntityUtils.toString(respEntity), Map.class);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> doHttpPut(String url, byte[] data, Map<String, String> header) throws IOException {
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(url);
            if (null != header) {
                for (Map.Entry<String, String> entry : header.entrySet()) {
                    httpPut.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                int code = resp.getStatusLine().getStatusCode();
                if (HttpStatus.SC_OK != code) {
                    String errorText;
                    try {
                        HttpEntity respEntity = resp.getEntity();
                        errorText = EntityUtils.toString(respEntity);
                    } catch (Exception err) {
                        errorText = "find errorText failed: " + err.getMessage();
                    }
                    log.warn("Request failed with code:{}, err:{}", code, errorText);
                    Map<String, Object> errorMap = new HashMap<>();
                    errorMap.put("Status", "Fail");
                    errorMap.put("Message", errorText);
                    return errorMap;
                }
                HttpEntity respEntity = resp.getEntity();
                if (null == respEntity) {
                    log.warn("Request failed with empty response.");
                    return null;
                }
                return JsonUtils.parseObject(EntityUtils.toString(respEntity), Map.class);
            }
        }
    }

    private CloseableHttpClient buildHttpClient() {
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });
        return httpClientBuilder.build();
    }

    public boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            log.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }
}
