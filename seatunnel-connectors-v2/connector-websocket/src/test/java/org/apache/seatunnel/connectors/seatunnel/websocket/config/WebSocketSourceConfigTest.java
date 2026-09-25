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

package org.apache.seatunnel.connectors.seatunnel.websocket.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class WebSocketSourceConfigTest {

    private static Map<String, Object> baseConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), "ws://localhost:8080/topic");
        return configMap;
    }

    private static WebSocketSourceConfig of(Map<String, Object> configMap) {
        return new WebSocketSourceConfig(ReadonlyConfig.fromMap(configMap));
    }

    @Test
    void testDefaultValues() {
        WebSocketSourceConfig config = of(baseConfig());
        Assertions.assertEquals("ws://localhost:8080/topic", config.getUrl());
        Assertions.assertEquals(WebSocketMessageFormat.JSON, config.getFormat());
        Assertions.assertEquals(",", config.getFieldDelimiter());
        Assertions.assertEquals(12000, config.getConnectTimeoutMs());
        Assertions.assertEquals(0, config.getPingIntervalMs());
        Assertions.assertTrue(config.isEnableReconnect());
        Assertions.assertEquals(3, config.getMaxReconnectTimes());
        Assertions.assertEquals(3000, config.getReconnectIntervalMs());
        Assertions.assertEquals(1024, config.getQueueCapacity());
        Assertions.assertEquals(1000, config.getPollTimeoutMs());
        Assertions.assertEquals(-1L, config.getMaxRecords());
        Assertions.assertEquals(-1, config.getReadTimeoutMs());
        // optional collections must never be null, the client iterates over them directly
        Assertions.assertTrue(config.getHeaders().isEmpty());
        Assertions.assertTrue(config.getOpenMessages().isEmpty());
    }

    @Test
    void testHeadersAndOpenMessagesArePreserved() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(
                WebSocketSourceOptions.HEADERS.key(),
                Collections.singletonMap("Authorization", "Bearer token"));
        configMap.put(
                WebSocketSourceOptions.OPEN_MESSAGES.key(), Arrays.asList("auth", "subscribe"));
        WebSocketSourceConfig config = of(configMap);
        Assertions.assertEquals("Bearer token", config.getHeaders().get("Authorization"));
        Assertions.assertEquals(Arrays.asList("auth", "subscribe"), config.getOpenMessages());
    }

    @Test
    void testSecureSchemeIsAccepted() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.URL.key(), "wss://localhost:8443/topic");
        Assertions.assertDoesNotThrow(() -> of(configMap));
    }

    @Test
    void testHttpSchemeIsRejected() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.URL.key(), "http://localhost:8080/topic");
        WebSocketConnectorException exception =
                Assertions.assertThrows(WebSocketConnectorException.class, () -> of(configMap));
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                exception.getSeaTunnelErrorCode());
    }

    @Test
    void testNonPositiveQueueCapacityIsRejected() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.QUEUE_CAPACITY.key(), 0);
        WebSocketConnectorException exception =
                Assertions.assertThrows(WebSocketConnectorException.class, () -> of(configMap));
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                exception.getSeaTunnelErrorCode());
    }

    @Test
    void testNonPositivePollTimeoutIsRejected() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.POLL_TIMEOUT_MS.key(), -1);
        Assertions.assertThrows(WebSocketConnectorException.class, () -> of(configMap));
    }

    @Test
    void testNegativeMaxReconnectTimesIsRejected() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), -1);
        Assertions.assertThrows(WebSocketConnectorException.class, () -> of(configMap));
    }

    @Test
    void testZeroMaxReconnectTimesIsAccepted() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), 0);
        Assertions.assertDoesNotThrow(() -> of(configMap));
    }
}
