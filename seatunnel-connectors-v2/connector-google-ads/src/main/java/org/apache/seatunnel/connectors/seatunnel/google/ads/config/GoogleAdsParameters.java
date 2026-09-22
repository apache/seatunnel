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

package org.apache.seatunnel.connectors.seatunnel.google.ads.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class GoogleAdsParameters implements Serializable {

    private String developerToken;
    private String clientId;
    private String clientSecret;
    private String refreshToken;
    private String customerId;
    private String loginCustomerId;
    private String apiVersion;
    private int requestTimeoutMs;
    private int maxRetries;
    private long retryBackoffMs;
    private Integer pageSize;
    private String apiEndpoint;
    private String oauthEndpoint;

    public void buildWithConfig(ReadonlyConfig config) {
        this.developerToken = config.get(GoogleAdsSourceOptions.DEVELOPER_TOKEN);
        this.clientId = config.get(GoogleAdsSourceOptions.CLIENT_ID);
        this.clientSecret = config.get(GoogleAdsSourceOptions.CLIENT_SECRET);
        this.refreshToken = config.get(GoogleAdsSourceOptions.REFRESH_TOKEN);
        this.customerId = config.get(GoogleAdsSourceOptions.CUSTOMER_ID);
        this.loginCustomerId =
                config.getOptional(GoogleAdsSourceOptions.LOGIN_CUSTOMER_ID).orElse(null);
        this.apiVersion = config.get(GoogleAdsSourceOptions.API_VERSION);
        this.requestTimeoutMs = config.get(GoogleAdsSourceOptions.REQUEST_TIMEOUT_MS);
        this.maxRetries = config.get(GoogleAdsSourceOptions.MAX_RETRIES);
        this.retryBackoffMs = config.get(GoogleAdsSourceOptions.RETRY_BACKOFF_MS);
        this.pageSize = config.getOptional(GoogleAdsSourceOptions.PAGE_SIZE).orElse(null);
        this.apiEndpoint = config.get(GoogleAdsSourceOptions.API_ENDPOINT);
        this.oauthEndpoint = config.get(GoogleAdsSourceOptions.OAUTH_ENDPOINT);
    }
}
