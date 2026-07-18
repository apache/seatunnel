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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HubSpotSourceParameterTest {

    @Test
    public void testAuthHeaderAndUrlConstruction() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");
        configMap.put("object_type", "companies");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HubSpotSourceParameter parameter = new HubSpotSourceParameter();
        parameter.buildWithConfig(config);

        // Verify Header Injection
        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(
                "Bearer test_secret_token", parameter.getHeaders().get("Authorization"));

        // Verify URL Construction
        Assertions.assertEquals(
                "https://api.hubapi.com/crm/v3/objects/companies", parameter.getUrl());
    }

    @Test
    public void testAuthorizationHeaderOverrideIsPreserved() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");

        Map<String, Object> headers = new HashMap<>();
        headers.put("Authorization", "Bearer custom_token");
        configMap.put("headers", headers);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        HubSpotSourceParameter parameter = new HubSpotSourceParameter();
        parameter.buildWithConfig(config);

        Assertions.assertEquals("Bearer custom_token", parameter.getHeaders().get("Authorization"));
    }

    @Test
    public void testBinaryFormatDoesNotInjectPagingDefaults() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");
        configMap.put("format", HttpConfig.ResponseFormat.BINARY.name());

        ReadonlyConfig runtimeConfig =
                HubSpotSourceParameter.buildRuntimeConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertFalse(runtimeConfig.getSourceMap().containsKey("pageing"));
        Assertions.assertFalse(runtimeConfig.getSourceMap().containsKey("content_field"));
    }
}
