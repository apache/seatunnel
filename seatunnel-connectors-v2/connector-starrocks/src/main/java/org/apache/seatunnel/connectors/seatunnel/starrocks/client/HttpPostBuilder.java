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

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/** Builder for HttpPut. */
public class HttpPostBuilder {
    String url;
    Map<String, String> header;
    HttpEntity httpEntity;

    public HttpPostBuilder() {
        header = new HashMap<>();
    }

    public HttpPostBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpPostBuilder addCommonHeader() {
        header.put(HttpHeaders.EXPECT, "100-continue");
        return this;
    }

    public HttpPostBuilder baseAuth(String user, String password) {
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        header.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
        return this;
    }

    public HttpPostBuilder addLabel(String label) {
        header.put("label", label);
        return this;
    }

    public HttpPostBuilder addDatabase(String database) {
        header.put("db", database);
        return this;
    }

    public HttpPostBuilder addTable(String database) {
        header.put("table", database);
        return this;
    }

    public HttpPostBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpPostBuilder commit() {
        header.put("txn_operation", "commit");
        return this;
    }

    public HttpPostBuilder begin() {

        return this;
    }

    public HttpPostBuilder addProperties(Properties properties) {
        properties.forEach((key, value) -> header.put(String.valueOf(key), String.valueOf(value)));
        return this;
    }

    public HttpPostBuilder setLabel(String label) {
        header.put("label", label);
        return this;
    }

    public HttpPost build() {
        checkNotNull(url);
        checkNotNull(httpEntity);
        HttpPost httpPost = new HttpPost(url);
        header.forEach(httpPost::setHeader);
        httpPost.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());
        return httpPost;
    }
}
