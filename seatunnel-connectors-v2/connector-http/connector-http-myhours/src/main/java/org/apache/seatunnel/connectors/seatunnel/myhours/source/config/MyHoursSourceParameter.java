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

package org.apache.seatunnel.connectors.seatunnel.myhours.source.config;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

public class MyHoursSourceParameter extends HttpParameter {
    public void buildWithConfig(Config pluginConfig, String accessToken) {
        super.buildWithConfig(pluginConfig);
        // put authorization in headers
        this.headers = this.getHeaders() == null ? new HashMap<>() : this.getHeaders();
        this.headers.put(MyHoursSourceConfig.AUTHORIZATION, MyHoursSourceConfig.ACCESS_TOKEN_PREFIX + " " + accessToken);
        this.setHeaders(this.headers);
    }

    public void buildWithLoginConfig(Config pluginConfig) {
        // set url
        this.setUrl(MyHoursSourceConfig.AUTHORIZATION_URL);
        // set method
        this.setMethod(HttpRequestMethod.valueOf(MyHoursSourceConfig.POST));
        // set body
        Map<String, String> bodyParams = new HashMap<>();
        String email = pluginConfig.getString(MyHoursSourceConfig.EMAIL.key());
        String password = pluginConfig.getString(MyHoursSourceConfig.PASSWORD.key());
        bodyParams.put(MyHoursSourceConfig.GRANT_TYPE, MyHoursSourceConfig.PASSWORD.key());
        bodyParams.put(MyHoursSourceConfig.EMAIL.key(), email);
        bodyParams.put(MyHoursSourceConfig.PASSWORD.key(), password);
        bodyParams.put(MyHoursSourceConfig.CLIENT_ID, MyHoursSourceConfig.API);
        String body = JsonUtils.toJsonString(bodyParams);
        this.setBody(body);
        this.setRetryParameters(pluginConfig);
    }
}
