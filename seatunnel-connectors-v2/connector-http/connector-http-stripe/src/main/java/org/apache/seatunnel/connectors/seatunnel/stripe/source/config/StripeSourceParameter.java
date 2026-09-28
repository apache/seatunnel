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

package org.apache.seatunnel.connectors.seatunnel.stripe.source.config;

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;

import java.util.LinkedHashMap;
import java.util.Map;

public class StripeSourceParameter extends HttpParameter {

    private static final String PAYMENT_INTENTS_PATH = "/v1/payment_intents";
    private static final String AUTHORIZATION = "Authorization";
    private static final String ACCEPT = "Accept";
    private static final String STRIPE_VERSION = "Stripe-Version";

    private int rateLimitMaxRetries;
    private int rateLimitBackoffMs;

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        String secretKey = pluginConfig.get(StripeSourceOptions.SECRET_KEY);
        if (Strings.isNullOrEmpty(secretKey) || secretKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Stripe option 'secret_key' must not be blank");
        }

        String apiBaseUrl = pluginConfig.get(StripeSourceOptions.API_BASE_URL);
        if (Strings.isNullOrEmpty(apiBaseUrl) || apiBaseUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Stripe option 'api_base_url' must not be blank");
        }

        int pageSize = pluginConfig.get(StripeSourceOptions.PAGE_SIZE);
        if (pageSize < 1 || pageSize > 100) {
            throw new IllegalArgumentException(
                    "Stripe option 'page_size' must be between 1 and 100");
        }

        Long createdGte = pluginConfig.getOptional(StripeSourceOptions.CREATED_GTE).orElse(null);
        Long createdLt = pluginConfig.getOptional(StripeSourceOptions.CREATED_LT).orElse(null);
        if (createdGte != null && createdGte < 0) {
            throw new IllegalArgumentException("Stripe option 'created_gte' must not be negative");
        }
        if (createdLt != null && createdLt < 0) {
            throw new IllegalArgumentException("Stripe option 'created_lt' must not be negative");
        }
        if (createdGte != null && createdLt != null && createdGte >= createdLt) {
            throw new IllegalArgumentException(
                    "Stripe option 'created_gte' must be less than 'created_lt'");
        }

        this.rateLimitMaxRetries = pluginConfig.get(StripeSourceOptions.RATE_LIMIT_MAX_RETRIES);
        this.rateLimitBackoffMs = pluginConfig.get(StripeSourceOptions.RATE_LIMIT_BACKOFF_MS);
        if (rateLimitMaxRetries < 0) {
            throw new IllegalArgumentException(
                    "Stripe option 'rate_limit_max_retries' must not be negative");
        }
        if (rateLimitBackoffMs < 0) {
            throw new IllegalArgumentException(
                    "Stripe option 'rate_limit_backoff_ms' must not be negative");
        }

        setUrl(trimTrailingSlash(apiBaseUrl.trim()) + PAYMENT_INTENTS_PATH);
        setMethod(HttpRequestMethod.GET);

        Map<String, String> requestHeaders = new LinkedHashMap<>();
        requestHeaders.put(AUTHORIZATION, "Bearer " + secretKey);
        requestHeaders.put(ACCEPT, "application/json");
        pluginConfig
                .getOptional(StripeSourceOptions.API_VERSION)
                .filter(version -> !version.trim().isEmpty())
                .ifPresent(version -> requestHeaders.put(STRIPE_VERSION, version.trim()));
        setHeaders(requestHeaders);

        Map<String, String> requestParams = new LinkedHashMap<>();
        requestParams.put("limit", Integer.toString(pageSize));
        if (createdGte != null) {
            requestParams.put("created[gte]", Long.toString(createdGte));
        }
        if (createdLt != null) {
            requestParams.put("created[lt]", Long.toString(createdLt));
        }
        setParams(requestParams);

        pluginConfig
                .getOptional(HttpCommonOptions.RETRY)
                .ifPresent(
                        retryCount -> {
                            setRetry(retryCount);
                            setRetryBackoffMultiplierMillis(
                                    pluginConfig.get(
                                            HttpCommonOptions.RETRY_BACKOFF_MULTIPLIER_MS));
                            setRetryBackoffMaxMillis(
                                    pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MAX_MS));
                        });
        setConnectTimeoutMs(pluginConfig.get(HttpSourceOptions.CONNECT_TIMEOUT_MS));
        setSocketTimeoutMs(pluginConfig.get(HttpSourceOptions.SOCKET_TIMEOUT_MS));
    }

    public int getRateLimitMaxRetries() {
        return rateLimitMaxRetries;
    }

    public int getRateLimitBackoffMs() {
        return rateLimitBackoffMs;
    }

    public void setStartingAfter(String cursor) {
        if (cursor == null) {
            getParams().remove("starting_after");
        } else {
            getParams().put("starting_after", cursor);
        }
    }

    @Override
    public String toString() {
        return "StripeSourceParameter{url='" + getUrl() + "', params=" + getParams() + "}";
    }

    private static String trimTrailingSlash(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }
}
