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
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirebaseHttpClientTest {
    @Test
    void testConfigUrlAndPathNormalization() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com///");
        configMap.put(FirebaseSourceOptions.PATH.key(), "///users/nodes///");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        String constructedUrl = client.buildUrl("users/nodes", null, false);
        assertEquals("https://test-db.firebaseio.com/users/nodes.json", constructedUrl);
    }

    @Test
    void testBuildUrlPercentEncodesDatabaseSecretWithSpecialCharacters() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);
        configMap.put(FirebaseSourceOptions.DATABASE_SECRET.key(), "sec ret+123&key=\"val\"");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        String url = client.buildUrl("users", null, false);

        // Asserts spaces (+ -> %20), quotes (%22), and special characters are percent-encoded
        assertTrue(url.contains("auth=sec%20ret%2B123%26key%3D%22val%22"));
        assertFalse(url.contains("sec ret"));
        assertFalse(url.contains("auth=sec+ret"));
    }

    @Test
    void testBuildUrlQuotesAndEncodesFilterParameters() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("orderBy", "name");
        queryParams.put("equalTo", "John Doe");
        configMap.put(FirebaseSourceOptions.QUERY_PARAMS.key(), queryParams);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        String url = client.buildUrl("users", null, true);

        // Asserts values are quote-wrapped ("name", "John Doe") and percent-encoded (%22)
        assertTrue(url.contains("orderBy=%22name%22"));
        assertTrue(url.contains("equalTo=%22John%20Doe%22"));
    }

    @Test
    void testBuildUrlShallowScanExcludesQueryParams() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("limitToFirst", "10");
        configMap.put(FirebaseSourceOptions.QUERY_PARAMS.key(), queryParams);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        // Shallow scan call: includeExtraParams = false
        String shallowUrl = client.buildUrl("users", "shallow=true", false);
        assertEquals("https://test-db.firebaseio.com/users.json?shallow=true", shallowUrl);
        assertFalse(shallowUrl.contains("limitToFirst"));

        // Regular node fetch call: includeExtraParams = true
        String nodeUrl = client.buildUrl("users", null, true);
        assertEquals("https://test-db.firebaseio.com/users.json?limitToFirst=10", nodeUrl);
    }

    @Test
    void testParseShallowKeysWithValidJson() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        String jsonResponse = "{\"user_101\": true, \"user_102\": true}";
        List<String> keys = client.parseShallowKeys(jsonResponse);

        assertEquals(2, keys.size());
        assertTrue(keys.contains("user_101"));
        assertTrue(keys.contains("user_102"));
    }

    @Test
    void testParseShallowKeysReturnsEmptyListOnNullOrNonObjectResponse() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        assertTrue(client.parseShallowKeys("null").isEmpty());
        assertTrue(client.parseShallowKeys(null).isEmpty());
        assertTrue(client.parseShallowKeys("  ").isEmpty());
        assertTrue(client.parseShallowKeys("[\"user_101\"]").isEmpty());
    }

    @Test
    void testParseShallowKeysThrowsSeaTunnelExceptionOnMalformedJson() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.TIMEOUT_MS.key(), 5000);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        FirebaseHttpClient client = new FirebaseHttpClient(config);

        assertThrows(SeaTunnelException.class, () -> client.parseShallowKeys("{invalid_json_body"));
    }
}
