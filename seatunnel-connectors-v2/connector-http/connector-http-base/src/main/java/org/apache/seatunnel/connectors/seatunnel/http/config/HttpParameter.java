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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

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
    protected int retryBackoffMultiplierMillis = HttpConfig.DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS;
    protected int retryBackoffMaxMillis = HttpConfig.DEFAULT_RETRY_BACKOFF_MAX_MS;
    protected boolean enableMultilines;
    protected int connectTimeoutMs = HttpConfig.DEFAULT_CONNECT_TIMEOUT_MS;
    protected int socketTimeoutMs = HttpConfig.DEFAULT_SOCKET_TIMEOUT_MS;

    public void buildWithConfig(Config pluginConfig) {
        // set url
        this.setUrl(pluginConfig.getString(HttpConfig.URL.key()));
        if (pluginConfig.hasPath(HttpConfig.KEEP_PARAMS_AS_FORM.key())) {
            this.setKeepParamsAsForm(pluginConfig.getBoolean(HttpConfig.KEEP_PARAMS_AS_FORM.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.KEEP_PAGE_PARAM_AS_HTTP_PARAM.key())) {
            this.setKeepPageParamAsHttpParam(
                    pluginConfig.getBoolean(HttpConfig.KEEP_PAGE_PARAM_AS_HTTP_PARAM.key()));
        }
        // set method
        if (pluginConfig.hasPath(HttpConfig.METHOD.key())) {
            HttpRequestMethod httpRequestMethod =
                    HttpRequestMethod.valueOf(
                            pluginConfig.getString(HttpConfig.METHOD.key()).toUpperCase());
            this.setMethod(httpRequestMethod);
        } else {
            this.setMethod(HttpConfig.METHOD.defaultValue());
        }
        // set headers
        if (pluginConfig.hasPath(HttpConfig.HEADERS.key())) {
            this.setHeaders(
                    pluginConfig.getConfig(HttpConfig.HEADERS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // set params
        if (pluginConfig.hasPath(HttpConfig.PARAMS.key())) {
            this.setParams(
                    pluginConfig.getConfig(HttpConfig.PARAMS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // set body
        if (pluginConfig.hasPath(HttpConfig.BODY.key())) {

            this.setBody(
                    ConfigFactory.parseString(pluginConfig.getString(HttpConfig.BODY.key()))
                            .entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> entry.getValue().unwrapped(),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.hasPath(HttpConfig.POLL_INTERVAL_MILLS.key())) {
            this.setPollIntervalMillis(pluginConfig.getInt(HttpConfig.POLL_INTERVAL_MILLS.key()));
        }
        this.setRetryParameters(pluginConfig);
        // set enableMultilines
        if (pluginConfig.hasPath(HttpConfig.ENABLE_MULTI_LINES.key())) {
            this.setEnableMultilines(pluginConfig.getBoolean(HttpConfig.ENABLE_MULTI_LINES.key()));
        } else {
            this.setEnableMultilines(HttpConfig.ENABLE_MULTI_LINES.defaultValue());
        }
        if (pluginConfig.hasPath(HttpConfig.CONNECT_TIMEOUT_MS.key())) {
            this.setConnectTimeoutMs(pluginConfig.getInt(HttpConfig.CONNECT_TIMEOUT_MS.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.SOCKET_TIMEOUT_MS.key())) {
            this.setSocketTimeoutMs(pluginConfig.getInt(HttpConfig.SOCKET_TIMEOUT_MS.key()));
        }
    }

    public void setRetryParameters(Config pluginConfig) {
        if (pluginConfig.hasPath(HttpConfig.RETRY.key())) {
            this.setRetry(pluginConfig.getInt(HttpConfig.RETRY.key()));
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS.key())) {
                this.setRetryBackoffMultiplierMillis(
                        pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS.key()));
            }
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MAX_MS.key())) {
                this.setRetryBackoffMaxMillis(
                        pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MAX_MS.key()));
            }
        }
    }
}
