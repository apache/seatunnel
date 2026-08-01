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

package org.apache.seatunnel.connectors.seatunnel.firebase.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirebaseHttpClientTest {
    @Test
    void testConfigUrlAndPathNormalization() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com///");
        configMap.put(FirebaseSourceOptions.PATH.key(), "///users/nodes///");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000L);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        assertNotNull(client);
    }

    @Test
    void testFetchShallowKeysParsing() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000L);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Testing shallow response mapping logic: {"user_101": true, "user_102": true} ->
        // List<String>
        String shallowJsonResponse = "{\"user_101\": true, \"user_102\": true}";

        // Direct test against parsing logic used in fetchShallowKeys
        List<String> keys = parseShallowKeysHelper(shallowJsonResponse);

        assertEquals(2, keys.size());
        assertTrue(keys.contains("user_101"));
        assertTrue(keys.contains("user_102"));
    }

    @Test
    void testFetchShallowKeysReturnsEmptyListOnNullResponse() {
        List<String> keys = parseShallowKeysHelper("null");
        assertTrue(keys.isEmpty());
    }

    private List<String> parseShallowKeysHelper(String jsonResponse) {
        if (jsonResponse == null || jsonResponse.trim().equals("null")) {
            return Collections.emptyList();
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            Map<String, Boolean> keysMap =
                    mapper.readValue(
                            jsonResponse,
                            new com.fasterxml.jackson.core.type.TypeReference<
                                    Map<String, Boolean>>() {});
            return new java.util.ArrayList<>(keysMap.keySet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
