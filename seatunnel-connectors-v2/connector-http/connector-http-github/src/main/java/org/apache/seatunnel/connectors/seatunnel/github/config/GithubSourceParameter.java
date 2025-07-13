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

package org.apache.seatunnel.connectors.seatunnel.github.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.util.HashMap;
import java.util.Optional;

public class GithubSourceParameter extends HttpParameter {

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);
        headers = Optional.ofNullable(getHeaders()).orElse(new HashMap<>());

        // Extract the access token parameter and add it to the http OAuth
        // header when it exists.
        if (pluginConfig.getOptional(GithubSourceOptions.ACCESS_TOKEN).isPresent()) {
            String oauthToken =
                    formatOauthToken(pluginConfig.get(GithubSourceOptions.ACCESS_TOKEN));
            headers.put(GithubSourceOptions.AUTHORIZATION_KEY, oauthToken);
        }
        setHeaders(headers);
    }

    // Format the access token into oauth2 format.
    private String formatOauthToken(String accessToken) {
        return GithubSourceOptions.BEARER_KEY + " " + accessToken;
    }
}
