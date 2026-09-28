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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class FacebookAdsParameters implements Serializable {

    private String accessToken;
    private String adAccountId;
    private String apiVersion;
    private int requestTimeoutMs;
    private int maxRetries;
    private long retryBackoffMs;
    private Integer pageSize;
    private String apiEndpoint;

    public void buildWithConfig(ReadonlyConfig config) {
        this.accessToken = config.get(FacebookAdsSourceOptions.ACCESS_TOKEN);
        this.adAccountId = config.get(FacebookAdsSourceOptions.AD_ACCOUNT_ID);
        this.apiVersion = config.get(FacebookAdsSourceOptions.API_VERSION);
        this.requestTimeoutMs = config.get(FacebookAdsSourceOptions.REQUEST_TIMEOUT_MS);
        this.maxRetries = config.get(FacebookAdsSourceOptions.MAX_RETRIES);
        this.retryBackoffMs = config.get(FacebookAdsSourceOptions.RETRY_BACKOFF_MS);
        this.pageSize = config.getOptional(FacebookAdsSourceOptions.PAGE_SIZE).orElse(null);
        this.apiEndpoint = config.get(FacebookAdsSourceOptions.API_ENDPOINT);
    }
}
