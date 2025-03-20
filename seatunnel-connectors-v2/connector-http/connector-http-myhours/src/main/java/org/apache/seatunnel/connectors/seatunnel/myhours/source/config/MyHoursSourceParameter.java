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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import java.util.HashMap;
import java.util.Map;

public class MyHoursSourceParameter extends HttpParameter {
    public void buildWithConfig(ReadonlyConfig pluginConfig, String accessToken) {
        super.buildWithConfig(pluginConfig);
        // put authorization in headers
        this.headers = this.getHeaders() == null ? new HashMap<>() : this.getHeaders();
        this.headers.put(
                MyHoursSourceOptions.AUTHORIZATION,
                MyHoursSourceOptions.ACCESS_TOKEN_PREFIX + " " + accessToken);
        this.setHeaders(this.headers);
    }

    public void buildWithLoginConfig(ReadonlyConfig pluginConfig) {
        // set url
        this.setUrl(MyHoursSourceOptions.AUTHORIZATION_URL);
        // set method
        this.setMethod(HttpRequestMethod.valueOf(MyHoursSourceOptions.POST));
        // set body
        Map<String, Object> bodyParams = new HashMap<>();
        String email = pluginConfig.get(MyHoursSourceOptions.EMAIL);
        String password = pluginConfig.get(MyHoursSourceOptions.PASSWORD);
        bodyParams.put(MyHoursSourceOptions.GRANT_TYPE, MyHoursSourceOptions.PASSWORD.key());
        bodyParams.put(MyHoursSourceOptions.EMAIL.key(), email);
        bodyParams.put(MyHoursSourceOptions.PASSWORD.key(), password);
        bodyParams.put(MyHoursSourceOptions.CLIENT_ID, MyHoursSourceOptions.API);
        this.setBody(bodyParams);
        if (pluginConfig.getOptional(HttpCommonOptions.RETRY).isPresent()) {
            this.setRetry(pluginConfig.get(HttpCommonOptions.RETRY));
            this.setRetryBackoffMultiplierMillis(
                    pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MULTIPLIER_MS));
            this.setRetryBackoffMaxMillis(pluginConfig.get(HttpCommonOptions.RETRY_BACKOFF_MAX_MS));
        }
    }
}
