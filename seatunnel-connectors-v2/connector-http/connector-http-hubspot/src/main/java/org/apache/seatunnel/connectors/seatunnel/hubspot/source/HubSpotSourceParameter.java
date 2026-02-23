/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.util.HashMap;
import java.util.Map;

public class HubSpotSourceParameter extends HttpParameter {
    public static final String DEFAULT_CONTENT_FIELD = "$.results";
    private static final String HUBSPOT_BASE_URL = "https://api.hubapi.com/crm/v3/objects/";

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        super.buildWithConfig(pluginConfig);

        // 1. Inject Authorization Header (Blocker 1)
        Map<String, String> currentHeaders =
                this.getHeaders() == null ? new HashMap<>() : this.getHeaders();
        currentHeaders.put(
                "Authorization", "Bearer " + pluginConfig.get(HubSpotSourceOptions.ACCESS_TOKEN));
        this.setHeaders(currentHeaders);

        // 2. Construct URL from object_type if url is not explicitly provided (Blocker 2)
        if (this.getUrl() == null || this.getUrl().isEmpty()) {
            String objectType = pluginConfig.get(HubSpotSourceOptions.OBJECT_TYPE);
            if (objectType != null && !objectType.isEmpty()) {
                this.setUrl(HUBSPOT_BASE_URL + objectType);
            }
        }
    }
}
