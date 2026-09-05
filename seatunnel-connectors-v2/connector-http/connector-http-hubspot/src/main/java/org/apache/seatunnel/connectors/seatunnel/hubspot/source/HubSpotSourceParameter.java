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
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;

import java.util.HashMap;
import java.util.Map;

/** HubSpot-specific HTTP parameter builder that injects runtime defaults for the shared reader. */
public class HubSpotSourceParameter extends HttpParameter {
    public static final String DEFAULT_CONTENT_FIELD = "$.results";
    private static final String HUBSPOT_BASE_URL = "https://api.hubapi.com/crm/v3/objects/";

    /**
     * Merge the HubSpot-specific defaults into the runtime config so the shared HTTP source path
     * and the HubSpot parameter object both observe the same pagination contract.
     */
    public static ReadonlyConfig buildRuntimeConfig(ReadonlyConfig pluginConfig) {
        // Preserve nested config objects such as `pageing`; `toMap()` stringifies them.
        Map<String, Object> configMap = new HashMap<>(pluginConfig.getSourceMap());
        HttpConfig.ResponseFormat responseFormat =
                pluginConfig
                        .getOptional(HttpSourceOptions.FORMAT)
                        .orElse(HttpConfig.ResponseFormat.JSON);
        configMap.putIfAbsent(HttpSourceOptions.FORMAT.key(), responseFormat.name());
        configMap.putIfAbsent(HttpSourceOptions.KEEP_PAGE_PARAM_AS_HTTP_PARAM.key(), Boolean.TRUE);
        if (responseFormat != HttpConfig.ResponseFormat.BINARY) {
            configMap.putIfAbsent(HttpSourceOptions.CONTENT_FIELD.key(), DEFAULT_CONTENT_FIELD);
            configMap.put(
                    HttpSourceOptions.PAGEING.key(),
                    mergePagingDefaults(configMap.get(HttpSourceOptions.PAGEING.key())));
        }
        return ReadonlyConfig.fromMap(configMap);
    }

    /**
     * Build a nested pageing section because HttpSource reads paging defaults from a sub-config
     * rather than from flattened dotted keys.
     */
    private static Map<String, Object> mergePagingDefaults(Object pageingConfig) {
        Map<String, Object> pageing = new HashMap<>();
        if (pageingConfig instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> existingConfig = (Map<String, Object>) pageingConfig;
            pageing.putAll(existingConfig);
        }
        pageing.putIfAbsent(HttpSourceOptions.PAGE_TYPE.key(), HttpPaginationType.CURSOR.getCode());
        pageing.putIfAbsent(HttpSourceOptions.PAGE_CURSOR_FIELD_NAME.key(), "after");
        pageing.putIfAbsent(
                HttpSourceOptions.PAGE_CURSOR_RESPONSE_FIELD.key(), "$.paging.next.after");
        return pageing;
    }

    @Override
    public void buildWithConfig(ReadonlyConfig pluginConfig) {
        ReadonlyConfig runtimeConfig = buildRuntimeConfig(pluginConfig);
        super.buildWithConfig(runtimeConfig);

        Map<String, String> currentHeaders =
                this.getHeaders() == null ? new HashMap<>() : new HashMap<>(this.getHeaders());
        currentHeaders.putIfAbsent(
                "Authorization", "Bearer " + runtimeConfig.get(HubSpotSourceOptions.ACCESS_TOKEN));
        this.setHeaders(currentHeaders);

        if (this.getUrl() == null || this.getUrl().isEmpty()) {
            String objectType = runtimeConfig.get(HubSpotSourceOptions.OBJECT_TYPE);
            if (objectType != null && !objectType.isEmpty()) {
                this.setUrl(HUBSPOT_BASE_URL + objectType);
            }
        }
    }
}
