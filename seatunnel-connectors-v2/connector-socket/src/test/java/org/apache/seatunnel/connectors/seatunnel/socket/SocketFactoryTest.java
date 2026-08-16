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

package org.apache.seatunnel.connectors.seatunnel.socket;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.socket.sink.SocketSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.socket.source.SocketSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SocketFactoryTest {

    private OptionRule sinkRule;

    @BeforeEach
    void setUp() {
        sinkRule = new SocketSinkFactory().optionRule();
    }

    private Map<String, Object> baseSinkConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(SocketSinkOptions.HOST.key(), "localhost");
        cfg.put(SocketSinkOptions.PORT.key(), 8080);
        return cfg;
    }

    private void validateSink(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule);
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new SocketSourceFactory()).optionRule());
        Assertions.assertNotNull(sinkRule);
    }

    @Test
    void testSinkOptionRuleWithValidPortAndMaxRetries() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.MAX_RETRIES.key(), 5);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithZeroPortFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.PORT.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithNegativePortFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.PORT.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testSinkOptionRuleWithNegativeMaxRetriesFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(SocketSinkOptions.MAX_RETRIES.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }
}
