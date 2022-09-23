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

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@SuppressWarnings("MagicNumber")
public class HttpParameter implements Serializable {
    private String url;
    private String method;
    private Map<String, String> headers;
    private Map<String, String> params;
    private String body;
    private int pollIntervalMillis;
    private int retry;
    private int retryBackoffMultiplierMillis = 100;
    private int retryBackoffMaxMillis = 10000;

    public void buildWithConfig(Config pluginConfig) {
        // set url
        this.setUrl(pluginConfig.getString(HttpConfig.URL));
        // set method
        if (pluginConfig.hasPath(HttpConfig.METHOD)) {
            this.setMethod(pluginConfig.getString(HttpConfig.METHOD));
        } else {
            this.setMethod(HttpConfig.METHOD_DEFAULT_VALUE);
        }
        // set headers
        if (pluginConfig.hasPath(HttpConfig.HEADERS)) {
            this.setHeaders(pluginConfig.getConfig(HttpConfig.HEADERS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue().unwrapped()), (v1, v2) -> v2)));
        }
        // set params
        if (pluginConfig.hasPath(HttpConfig.PARAMS)) {
            this.setParams(pluginConfig.getConfig(HttpConfig.PARAMS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue().unwrapped()), (v1, v2) -> v2)));
        }
        // set body
        if (pluginConfig.hasPath(HttpConfig.BODY)) {
            this.setBody(pluginConfig.getString(HttpConfig.BODY));
        }
        if (pluginConfig.hasPath(HttpConfig.POLL_INTERVAL_MILLS)) {
            this.setPollIntervalMillis(pluginConfig.getInt(HttpConfig.POLL_INTERVAL_MILLS));
        }
        if (pluginConfig.hasPath(HttpConfig.RETRY)) {
            this.setRetry(pluginConfig.getInt(HttpConfig.RETRY));
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS)) {
                this.setRetryBackoffMultiplierMillis(pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS));
            }
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MAX_MS)) {
                this.setRetryBackoffMaxMillis(pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MAX_MS));
            }
        }
    }
}
