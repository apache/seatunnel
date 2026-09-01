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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Map;

@Data
@Slf4j
@SuppressWarnings("MagicNumber")
public class HttpParameter implements Serializable {
    protected String url;
    protected HttpRequestMethod method;
    @ToString.Exclude protected Map<String, String> headers;
    protected Map<String, String> params;
    protected Map<String, Object> pageParams;
    protected boolean keepParamsAsForm = false;
    protected boolean keepPageParamAsHttpParam = false;
    protected String body;
    protected int pollIntervalMillis;
    protected int retry;
    protected int retryBackoffMultiplierMillis;
    protected int retryBackoffMaxMillis;
    protected boolean enableMultilines;
    protected int connectTimeoutMs;
    protected int socketTimeoutMs;
    protected boolean arrayMode = false;
    protected int batchSize = 1;
    protected int requestIntervalMs = 0;
    protected boolean jsonFiledMissedReturnNull;

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
        // validate credential scheme after url and headers are set
        validateCredentialScheme();
        // set params
        if (pluginConfig.getOptional(HttpCommonOptions.PARAMS).isPresent()) {
            this.setParams(pluginConfig.get(HttpCommonOptions.PARAMS));
        }
        // set body
        if (pluginConfig.getOptional(HttpSourceOptions.BODY).isPresent()) {
            this.setBody(pluginConfig.get(HttpSourceOptions.BODY));
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
        this.setJsonFiledMissedReturnNull(
                pluginConfig.get(HttpSourceOptions.JSON_FILED_MISSED_RETURN_NULL));
    }

    /**
     * Validates that the URL scheme is HTTPS when credential headers are present.
     * Logs a warning if the URL uses plain HTTP while authorization headers are configured,
     * as this would send credentials in clear text over the network.
     */
    public void validateCredentialScheme() {
        if (StringUtils.isBlank(this.url)) {
            return;
        }
        String lowerUrl = this.url.toLowerCase();
        if (lowerUrl.startsWith("https://")) {
            return;
        }
        if (this.headers == null || this.headers.isEmpty()) {
            return;
        }
        boolean hasAuthHeader =
                this.headers.keySet().stream()
                        .anyMatch(
                                key ->
                                        key != null
                                                && key.toLowerCase().contains("authorization"));
        if (hasAuthHeader) {
            log.warn(
                    "The HTTP connector URL '{}' uses a non-HTTPS scheme while credential headers are configured. "
                            + "Credentials will be transmitted in clear text over the network. "
                            + "Consider using HTTPS for production environments.",
                    this.url);
        }
    }
}