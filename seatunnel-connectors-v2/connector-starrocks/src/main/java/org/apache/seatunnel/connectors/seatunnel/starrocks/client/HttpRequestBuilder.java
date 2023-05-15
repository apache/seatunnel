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

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpRequestBuilder {
    private String url;
    private Map<String, String> header;
    private HttpEntity httpEntity;
    private String methodName;
    private SinkConfig sinkConfig;
    private RequestConfig requestConfig;

    public HttpRequestBuilder() {
        this.header = new HashMap<>();
    }

    public HttpRequestBuilder(SinkConfig sinkConfig) {
        this();
        this.sinkConfig = sinkConfig;
    }

    public HttpRequestBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpRequestBuilder addBeginHeader() {
        header.put("timeout", "600");
        return this;
    }

    public HttpRequestBuilder setDefaultConfig() {
        requestConfig =
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build();
        return this;
    }

    public HttpRequestBuilder addStreamLoadProps() {
        for (Map.Entry<String, Object> entry : sinkConfig.getStreamLoadProps().entrySet()) {
            header.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return this;
    }

    public HttpRequestBuilder begin(String label) {
        HttpPost();
        setDefaultConfig();
        addBeginHeader();
        baseAuth();
        setDB(sinkConfig.getDatabase());
        setTable(sinkConfig.getTable());
        setLabel(label);
        return this;
    }

    public HttpRequestBuilder streamLoad(String label) {
        HttpPut();
        setDefaultConfig();
        baseAuth();
        addStreamLoadProps();
        header.put(HttpHeaders.EXPECT, "100-continue");
        setDB(sinkConfig.getDatabase());
        setTable(sinkConfig.getTable());
        setLabel(label);
        return this;
    }

    public HttpRequestBuilder prepare(String label) {
        HttpPost();
        setDefaultConfig();
        baseAuth();
        setDB(sinkConfig.getDatabase());
        setTable(sinkConfig.getTable());
        setLabel(label);
        return this;
    }

    public HttpRequestBuilder commit(String label) {
        HttpPost();
        setDefaultConfig();
        baseAuth();
        setDB(sinkConfig.getDatabase());
        setTable(sinkConfig.getTable());
        setLabel(label);
        return this;
    }

    public HttpRequestBuilder rollback(String label) {
        HttpPost();
        baseAuth();
        setDB(sinkConfig.getDatabase());
        setTable(sinkConfig.getTable());
        setLabel(label);
        return this;
    }

    public HttpRequestBuilder getLoadState() {
        HttpGet();
        baseAuth();
        header.put("Connection", "close");
        return this;
    }

    public HttpRequestBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpRequestBuilder setEntity(byte[] data) {
        this.httpEntity = new ByteArrayEntity(data);
        return this;
    }

    public HttpRequestBuilder setEmptyEntity() {
        try {
            this.httpEntity = new StringEntity("");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public HttpRequestBuilder addProperties(Map<String, String> properties) {
        header.putAll(properties);
        return this;
    }

    public HttpRequestBuilder HttpPost() {
        methodName = HttpPost.METHOD_NAME;
        return this;
    }

    public HttpRequestBuilder HttpGet() {
        methodName = HttpGet.METHOD_NAME;
        return this;
    }

    public HttpRequestBuilder HttpPut() {
        methodName = HttpPut.METHOD_NAME;
        return this;
    }

    public HttpRequestBuilder setDB(String db) {
        header.put("db", db);
        return this;
    }

    public HttpRequestBuilder setTable(String table) {
        header.put("table", table);
        return this;
    }

    public HttpRequestBuilder setLabel(String label) {
        header.put("label", label);
        return this;
    }

    public HttpRequestBuilder baseAuth() {
        header.put(
                HttpHeaders.AUTHORIZATION,
                getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        return this;
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public HttpRequestBase build() {
        checkNotNull(url);
        switch (methodName) {
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(url);
                if (!Objects.isNull(requestConfig)) httpPost.setConfig(requestConfig);
                header.forEach(httpPost::setHeader);
                httpPost.setEntity(httpEntity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                HttpPut httpPut = new HttpPut(url);
                if (!Objects.isNull(requestConfig)) httpPut.setConfig(requestConfig);
                header.forEach(httpPut::setHeader);
                httpPut.setEntity(httpEntity);
                return httpPut;
            default:
                HttpGet httpGet = new HttpGet(url);
                if (!Objects.isNull(requestConfig)) httpGet.setConfig(requestConfig);
                header.forEach(httpGet::setHeader);
                return httpGet;
        }
    }
}
