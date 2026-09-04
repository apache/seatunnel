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

package org.apache.seatunnel.connectors.seatunnel.posthog.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class PostHogSourceParameter extends HttpParameter {

    private static final String AUTHORIZATION = "Authorization";
    private static final String ACCEPT = "Accept";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        String projectId =
                requireNonBlank(pluginConfig.get(PostHogSourceOptions.PROJECT_ID), "project_id");
        String apiKey = requireNonBlank(pluginConfig.get(PostHogSourceOptions.API_KEY), "api_key");
        String query = requireNonBlank(pluginConfig.get(PostHogSourceOptions.QUERY), "query");
        String baseUrl = normalizeBaseUrl(pluginConfig.get(PostHogSourceOptions.BASE_URL));

        setUrl(baseUrl + "/api/projects/" + encodePathSegment(projectId) + "/query/");
        setMethod(HttpRequestMethod.POST);
        setParams(Collections.emptyMap());
        setKeepParamsAsForm(false);

        Map<String, String> headers =
                new LinkedHashMap<>(
                        pluginConfig
                                .getOptional(HttpCommonOptions.HEADERS)
                                .orElse(Collections.emptyMap()));
        setHeader(headers, AUTHORIZATION, "Bearer " + apiKey);
        setHeader(headers, ACCEPT, APPLICATION_JSON);
        setHeader(headers, CONTENT_TYPE, APPLICATION_JSON);
        setHeaders(headers);

        Map<String, Object> queryRequest = new LinkedHashMap<>();
        queryRequest.put("kind", "HogQLQuery");
        queryRequest.put("query", query);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", queryRequest);
        body.put("refresh", "blocking");
        setBody(JsonUtils.toJsonString(body));

        setRetry(pluginConfig.getOptional(HttpCommonOptions.RETRY).orElse(0));
        setRetryBackoffMultiplierMillis(
                pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        setRetryBackoffMaxMillis(pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MAX_MS));
        setConnectTimeoutMs(pluginConfig.get(HttpSourceOptions.CONNECT_TIMEOUT_MS));
        setSocketTimeoutMs(pluginConfig.get(HttpSourceOptions.SOCKET_TIMEOUT_MS));
    }

    private static String normalizeBaseUrl(String baseUrl) {
        String normalized = requireNonBlank(baseUrl, "base_url");
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("PostHog option 'base_url' must not be blank");
        }
        return normalized;
    }

    private static String encodePathSegment(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding is not available", e);
        }
    }

    private static void setHeader(Map<String, String> headers, String name, String value) {
        headers.keySet().removeIf(headerName -> headerName.equalsIgnoreCase(name));
        headers.put(name, value);
    }

    private static String requireNonBlank(String value, String optionName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "PostHog option '" + optionName + "' must not be blank");
        }
        return value.trim();
    }
}
