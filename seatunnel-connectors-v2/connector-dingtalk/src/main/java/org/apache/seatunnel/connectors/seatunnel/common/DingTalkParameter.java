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

package org.apache.seatunnel.connectors.seatunnel.common;

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @description: Ding Talk Parameter
 **/

@Data
public class DingTalkParameter implements Serializable {

    private String appKey;
    private String appSecret;
    private String accessToken;
    private String apiClient;
    private Map<String, String> params;

    public void buildParameter(Config pluginConfig, Boolean hasToken) {
        if (!hasToken) {
            // DingTalk app key
            this.setAppKey(pluginConfig.getString(DingTalkConstant.APP_KEY));
            // DingTalk app secret
            this.setAppSecret(pluginConfig.getString(DingTalkConstant.APP_SECRET));
        } else {
            // DingTalk app token
            this.setAccessToken(pluginConfig.getString(DingTalkConstant.ACCESS_TOKEN));
        }
        // DingTalk api client
        this.setApiClient(pluginConfig.getString(DingTalkConstant.API_CLIENT));
        // set params
        if (pluginConfig.hasPath(HttpConfig.PARAMS)) {
            this.setParams(pluginConfig.getConfig(HttpConfig.PARAMS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue().unwrapped()), (v1, v2) -> v2)));
        }
    }
}
