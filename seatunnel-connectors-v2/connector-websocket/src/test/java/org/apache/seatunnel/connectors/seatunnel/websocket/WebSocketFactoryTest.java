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

package org.apache.seatunnel.connectors.seatunnel.websocket;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.source.WebSocketSource;
import org.apache.seatunnel.connectors.seatunnel.websocket.source.WebSocketSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class WebSocketFactoryTest {

    private WebSocketSourceFactory factory;
    private OptionRule sourceRule;

    @BeforeEach
    void setUp() {
        factory = new WebSocketSourceFactory();
        sourceRule = factory.optionRule();
    }

    private void validateSource(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule);
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceRule);
        Assertions.assertEquals("WebSocket", factory.factoryIdentifier());
        Assertions.assertEquals(WebSocketSource.class, factory.getSourceClass());
    }

    @Test
    void testSourceOptionRuleWithUrlOnly() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(WebSocketSourceOptions.URL.key(), "ws://localhost:8080/");
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));
    }

    @Test
    void testSourceOptionRuleWithoutUrlFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(WebSocketSourceOptions.QUEUE_CAPACITY.key(), 16);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }
}
