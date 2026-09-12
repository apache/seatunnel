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

package org.apache.seatunnel.edge.agent.transport.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.util.HashMap;
import java.util.Map;

public class EdgeTransportConfigTestHelper {

    public static EdgeTransportConfig config(String endpoint, String token) {
        return EdgeTransportConfig.from(minimalMap(endpoint, token));
    }

    public static EdgeTransportConfig config(
            String endpoint, String token, Map<String, Object> overrides) {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.ENDPOINT.key(), endpoint);
        map.put(EdgeTransportOptions.TOKEN.key(), token);
        if (overrides != null) {
            map.putAll(overrides);
        }
        return EdgeTransportConfig.from(ReadonlyConfig.fromMap(map));
    }

    public static ReadonlyConfig minimalMap(String endpoint, String token) {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.ENDPOINT.key(), endpoint);
        map.put(EdgeTransportOptions.TOKEN.key(), token);
        return ReadonlyConfig.fromMap(map);
    }
}
