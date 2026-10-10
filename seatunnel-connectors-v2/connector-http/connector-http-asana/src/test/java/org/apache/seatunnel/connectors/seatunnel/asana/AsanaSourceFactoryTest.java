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

package org.apache.seatunnel.connectors.seatunnel.asana;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.asana.config.AsanaSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.asana.config.AsanaSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class AsanaSourceFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        AsanaSourceFactory factory = new AsanaSourceFactory();
        Assertions.assertEquals("Asana", factory.factoryIdentifier());
    }

    @Test
    public void testParameterBuildingAndAuthHeader() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-asana-token-123");
        configMap.put("project_gid", "1234567890");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        parameter.buildWithConfig(config, config.get(AsanaSourceOptions.API_KEY));

        Assertions.assertNotNull(parameter.getHeaders());
        Assertions.assertEquals(
                "Bearer test-asana-token-123", parameter.getHeaders().get("Authorization"));
        Assertions.assertEquals(HttpRequestMethod.GET, parameter.getMethod());
        Assertions.assertEquals("1234567890", parameter.getParams().get("project"));
        Assertions.assertNotNull(parameter.getParams().get("opt_fields"));
    }

    @Test
    public void testModifiedSinceFilterOption() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("api_key", "test-asana-token-123");
        configMap.put("project_gid", "1234567890");
        configMap.put("modified_since", "2026-01-01T00:00:00.000Z");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        AsanaSourceParameter parameter = new AsanaSourceParameter();
        parameter.buildWithConfig(config, config.get(AsanaSourceOptions.API_KEY));

        Assertions.assertEquals(
                "2026-01-01T00:00:00.000Z", parameter.getParams().get("modified_since"));
    }
}
