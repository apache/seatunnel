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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HubSpotSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        Assertions.assertEquals(
                "HubSpot", factory.factoryIdentifier(), "Factory identifier should be HubSpot");
    }

    @Test
    public void testOptionRule() {
        HubSpotSourceFactory factory = new HubSpotSourceFactory();
        OptionRule optionRule = factory.optionRule();
        Assertions.assertNotNull(optionRule, "OptionRule should not be null");

        Map<String, Object> configMap = createFullConfig();

        Assertions.assertDoesNotThrow(
                () -> {
                    ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
                    ConfigValidator.of(config).validate(optionRule);
                    ConfigValidator.validateUnknownKeys(
                            config, optionRule, factory.factoryIdentifier());
                });

        Map<String, Object> missingAccessToken = new HashMap<>(configMap);
        missingAccessToken.remove("access_token");
        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(missingAccessToken))
                                .validate(optionRule));
    }

    private static Map<String, Object> createFullConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_token");
        configMap.put("object_type", "contacts");
        configMap.put("url", "https://api.hubapi.com/crm/v3/objects/contacts");
        configMap.put("method", "GET");
        configMap.put("format", HttpConfig.ResponseFormat.JSON.name());
        configMap.put("content_field", "$.results");
        configMap.put("body", "{\"archived\":false}");
        configMap.put("poll_interval_millis", 1000);
        configMap.put("retry", 3);
        configMap.put("retry_backoff_multiplier_ms", 200);
        configMap.put("retry_backoff_max_ms", 5000);
        configMap.put("binary_chunk_size", 4096L);
        configMap.put("enable_multi_lines", true);
        configMap.put("connect_timeout_ms", 15000);
        configMap.put("socket_timeout_ms", 65000);
        configMap.put("keep_params_as_form", false);
        configMap.put("keep_page_param_as_http_param", true);
        configMap.put("json_filed_missed_return_null", true);

        Map<String, Object> headers = new HashMap<>();
        headers.put("X-Debug", "true");
        configMap.put("headers", headers);

        Map<String, Object> params = new HashMap<>();
        params.put("limit", "100");
        configMap.put("params", params);

        Map<String, Object> schema = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "string");
        schema.put("fields", fields);
        configMap.put("schema", schema);

        Map<String, Object> jsonField = new HashMap<>();
        jsonField.put("id", "$.id");
        configMap.put("json_field", jsonField);

        Map<String, Object> pageing = new HashMap<>();
        pageing.put(HttpSourceOptions.PAGE_TYPE.key(), "Cursor");
        pageing.put(HttpSourceOptions.PAGE_CURSOR_FIELD_NAME.key(), "after");
        pageing.put(HttpSourceOptions.PAGE_CURSOR_RESPONSE_FIELD.key(), "$.paging.next.after");
        pageing.put(HttpSourceOptions.USE_PLACEHOLDER_REPLACEMENT.key(), true);
        configMap.put(HttpSourceOptions.PAGEING.key(), pageing);

        return configMap;
    }
}
