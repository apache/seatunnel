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

package org.apache.seatunnel.connectors.seatunnel.zendesk.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;

public class ZendeskSourceParameter extends HttpParameter {

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);
        // Zendesk API token authentication uses HTTP Basic auth with the form
        // "{email}/token:{api_token}", see https://developer.zendesk.com/api-reference
        this.headers = this.getHeaders() == null ? new HashMap<>() : this.getHeaders();
        this.headers.put(ZendeskSourceOptions.ACCEPT, ZendeskSourceOptions.APPLICATION_JSON);
        String credentials =
                pluginConfig.get(ZendeskSourceOptions.EMAIL)
                        + "/token:"
                        + pluginConfig.get(ZendeskSourceOptions.API_TOKEN);
        String encoded =
                Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        this.headers.put(ZendeskSourceOptions.AUTHORIZATION, ZendeskSourceOptions.BASIC + encoded);
        this.setHeaders(this.headers);
        validateCredentialScheme();
    }
}
