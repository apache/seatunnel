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

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
@SuppressWarnings("MagicNumber")
public class MyHoursSourceParameter extends HttpParameter {

    public void buildWithConfig(Config pluginConfig, String accessToken) {
        // set url
        if (pluginConfig.hasPath(MyHoursSourceConfig.PROJECTS)) {
            String projects = pluginConfig.getString(MyHoursSourceConfig.PROJECTS);
            if (projects.equals(MyHoursSourceConfig.ALL)) {
                this.setUrl(MyHoursSourceConfig.ALL_PROJECTS_URL);
            }
            else {
                this.setUrl(MyHoursSourceConfig.ACTIVE_PROJECTS_URL);
            }
        }
        else {
            String users = pluginConfig.getString(MyHoursSourceConfig.USERS);
            if (users.equals(MyHoursSourceConfig.MEMBER)) {
                this.setUrl(MyHoursSourceConfig.ALL_MEMBERS_URL);
            }
            else {
                this.setUrl(MyHoursSourceConfig.ALL_CLIENTS_URL);
            }
        }
        // set method
        this.setMethod(HttpConfig.METHOD_DEFAULT_VALUE);
        // set headers
        this.headers = new HashMap<>();
        this.headers.put(MyHoursSourceConfig.AUTHORIZATION, MyHoursSourceConfig.ACCESSTOKEN_PREFIX + " " + accessToken);
        this.setHeaders(headers);
        // set retry
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

    public void buildWithLoginConfig(Config pluginConfig) throws JsonProcessingException {
        // set url
        this.setUrl(MyHoursSourceConfig.AUTHORIZATION_URL);
        // set method
        this.setMethod(MyHoursSourceConfig.POST);
        // set body
        Map bodyParams = new HashMap();
        String email = pluginConfig.getString(MyHoursSourceConfig.EMAIL);
        String password = pluginConfig.getString(MyHoursSourceConfig.PASSWORD);
        bodyParams.put(MyHoursSourceConfig.GRANTTYPE, MyHoursSourceConfig.PASSWORD);
        bodyParams.put(MyHoursSourceConfig.EMAIL, email);
        bodyParams.put(MyHoursSourceConfig.PASSWORD, password);
        bodyParams.put(MyHoursSourceConfig.CLIENTID, MyHoursSourceConfig.API);
        ObjectMapper om = new ObjectMapper();
        String body = om.writeValueAsString(bodyParams);
        this.setBody(body);
        // set retry
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
