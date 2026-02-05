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

    private final ReadonlyConfig targetConfig;

    public HubSpotSourceParameter(ReadonlyConfig originalConfig) {
        super();

        // 1. Get user inputs
        String objectType =
                originalConfig.getOptional(HubSpotSourceOptions.OBJECT_TYPE).orElse("contacts");
        String accessToken = originalConfig.get(HubSpotSourceOptions.ACCESS_TOKEN);

        // 2. Determine Base URL
        String providedUrl =
                originalConfig
                        .getOptional(
                                org.apache.seatunnel.api.configuration.Options.key("url")
                                        .stringType()
                                        .noDefaultValue())
                        .orElse(null);

        if (providedUrl != null) {
            this.url = providedUrl;
        } else {
            this.url = "https://api.hubapi.com/crm/v3/objects/" + objectType;
        }

        this.headers = new HashMap<>();
        this.headers.put("Authorization", "Bearer " + accessToken);
        this.headers.put("Content-Type", "application/json");

        // 3. Build the final config map for the parent Source
        Map<String, Object> params = new HashMap<>(originalConfig.toMap());
        params.put("url", this.url);
        params.put("headers", this.headers);
        params.put("content_field", "results");

        this.targetConfig = ReadonlyConfig.fromMap(params);
    }

    public ReadonlyConfig getConfig() {
        return targetConfig;
    }

    @Override
    public String toString() {
        return "HubSpotSourceParameter{"
                + "url='"
                + url
                + '\''
                + ", method='"
                + method
                + '\''
                + ", headers=******"
                + // Mask headers
                '}';
    }
}
