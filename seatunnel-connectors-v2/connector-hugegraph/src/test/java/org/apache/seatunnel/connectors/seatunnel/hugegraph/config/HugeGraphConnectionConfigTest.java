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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins config-load-time rejection of invalid connection parameters so the job stops before opening
 * a client and surfacing a generic connection error. Each test asserts the offending option name
 * appears in the message so operators can act on it directly.
 */
class HugeGraphConnectionConfigTest {

    @Test
    void acceptsMinimalValidConfig() {
        assertDoesNotThrow(() -> HugeGraphConnectionConfig.of(config("127.0.0.1", 8080)));
    }

    @Test
    void rejectsEmptyHost() {
        Map<String, Object> map = configMap("", 8080);
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map)));
        assertTrue(ex.getMessage().contains("host"));
    }

    @Test
    void rejectsPortOutOfRange() {
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> HugeGraphConnectionConfig.of(config("host", 70000)));
        assertTrue(ex.getMessage().contains("port"));
    }

    @Test
    void rejectsPortZero() {
        assertThrows(
                HugeGraphConnectorException.class,
                () -> HugeGraphConnectionConfig.of(config("host", 0)));
    }

    @Test
    void rejectsUsernameWithoutPassword() {
        Map<String, Object> map = configMap("host", 8080);
        map.put("username", "u");
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map)));
        assertTrue(ex.getMessage().contains("username") || ex.getMessage().contains("password"));
    }

    @Test
    void rejectsPasswordWithoutUsername() {
        Map<String, Object> map = configMap("host", 8080);
        map.put("password", "p");
        assertThrows(
                HugeGraphConnectorException.class,
                () -> HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map)));
    }

    @Test
    void acceptsBothCredentialsSet() {
        Map<String, Object> map = configMap("host", 8080);
        map.put("username", "u");
        map.put("password", "p");
        HugeGraphConnectionConfig config =
                HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map));
        assertEquals("u", config.getUsername());
        assertEquals("p", config.getPassword());
    }

    @Test
    void acceptsNeitherCredentialSet() {
        // Anonymous access is legitimate for local dev / open dashboards.
        assertDoesNotThrow(() -> HugeGraphConnectionConfig.of(config("host", 8080)));
    }

    @Test
    void rejectsInvalidProtocol() {
        Map<String, Object> map = configMap("host", 8080);
        map.put("protocol", "ftp");
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map)));
        assertTrue(ex.getMessage().contains("protocol"));
    }

    @Test
    void defaultsRetryBackoffMax() {
        HugeGraphConnectionConfig config = HugeGraphConnectionConfig.of(config("host", 8080));
        assertEquals(30000, config.getRetryBackoffMaxMs());
    }

    @Test
    void rejectsNegativeRetryBackoffMax() {
        Map<String, Object> map = configMap("host", 8080);
        map.put("retry_backoff_max_ms", -1);
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () -> HugeGraphConnectionConfig.of(ReadonlyConfig.fromMap(map)));
        assertTrue(ex.getMessage().contains("retry_backoff_max_ms"));
    }

    private static ReadonlyConfig config(String host, int port) {
        return ReadonlyConfig.fromMap(configMap(host, port));
    }

    private static Map<String, Object> configMap(String host, int port) {
        Map<String, Object> map = new HashMap<>();
        map.put("host", host);
        map.put("port", port);
        map.put("graph_name", "hugegraph");
        return map;
    }
}
