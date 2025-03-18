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

package org.apache.seatunnel.connectors.seatunnel.http.config;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@SuppressWarnings("MagicNumber")
public class HttpParameter implements Serializable {
    protected String url;
    protected HttpRequestMethod method;
    protected Map<String, String> headers;
    protected Map<String, String> params;
    protected Map<String, Object> pageParams;
    protected boolean keepParamsAsForm = false;
    protected boolean keepPageParamAsHttpParam = false;
    protected Map<String, Object> body;
    protected int pollIntervalMillis;
    protected int retry;
    protected int retryBackoffMultiplierMillis;
    protected int retryBackoffMaxMillis;
    protected boolean enableMultilines;
    protected int connectTimeoutMs;
    protected int socketTimeoutMs;

    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        // set url
        this.setUrl(pluginConfig.get(HttpCommonOptions.URL));
        if (pluginConfig.getOptional(HttpSourceOptions.KEEP_PARAMS_AS_FORM).isPresent()) {
            this.setKeepParamsAsForm(pluginConfig.get(HttpSourceOptions.KEEP_PARAMS_AS_FORM));
        }
        if (pluginConfig.getOptional(HttpSourceOptions.KEEP_PAGE_PARAM_AS_HTTP_PARAM).isPresent()) {
            this.setKeepPageParamAsHttpParam(
                    pluginConfig.get(HttpSourceOptions.KEEP_PAGE_PARAM_AS_HTTP_PARAM));
        }
        // set method
        this.setMethod(pluginConfig.get(HttpSourceOptions.METHOD));
        // set headers
        if (pluginConfig.getOptional(HttpCommonOptions.HEADERS).isPresent()) {
            this.setHeaders(pluginConfig.get(HttpCommonOptions.HEADERS));
        }
        // set params
        if (pluginConfig.getOptional(HttpCommonOptions.PARAMS).isPresent()) {
            this.setParams(pluginConfig.get(HttpCommonOptions.PARAMS));
        }
        // set body
        if (pluginConfig.getOptional(HttpSourceOptions.BODY).isPresent()) {
            this.setBody(
                    ConfigFactory.parseString(pluginConfig.get(HttpSourceOptions.BODY)).entrySet()
                            .stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> entry.getValue().unwrapped(),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.getOptional(HttpSourceOptions.POLL_INTERVAL_MILLS).isPresent()) {
            this.setPollIntervalMillis(pluginConfig.get(HttpSourceOptions.POLL_INTERVAL_MILLS));
        }
        if (pluginConfig.getOptional(HttpCommonOptions.RETRY).isPresent()) {
            this.setRetry(pluginConfig.get(HttpCommonOptions.RETRY));
            this.setRetryBackoffMultiplierMillis(
                    pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MULTIPLIER_MS));
            this.setRetryBackoffMaxMillis(pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MAX_MS));
        }
        // set enableMultilines
        this.setEnableMultilines(pluginConfig.get(HttpSourceOptions.ENABLE_MULTI_LINES));
        this.setConnectTimeoutMs(pluginConfig.get(HttpSourceOptions.CONNECT_TIMEOUT_MS));
        this.setSocketTimeoutMs(pluginConfig.get(HttpSourceOptions.SOCKET_TIMEOUT_MS));
    }
}
