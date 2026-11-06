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

package org.apache.seatunnel.connectors.seatunnel.mqtt.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MqttSourceFactoryTest {

    private OptionRule sourceRule;

    @BeforeEach
    void setUp() {
        sourceRule = new MqttSourceFactory().optionRule();
    }

    private static Map<String, Object> baseSourceConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(MqttSourceOptions.URL.key(), "tcp://localhost:1883");
        cfg.put(MqttSourceOptions.TOPIC.key(), "users");
        cfg.put(
                ConnectorCommonOptions.SCHEMA.key(),
                Collections.singletonMap("fields", Collections.singletonMap("name", "string")));
        return cfg;
    }

    private void validateSource(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sourceRule);
    }

    @Test
    void testFactoryIdentifier() {
        MqttSourceFactory factory = new MqttSourceFactory();

        Assertions.assertEquals(MqttSourceOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        Assertions.assertEquals(MqttSource.class, factory.getSourceClass());
    }

    @Test
    void testOptionRule() {
        List<Option<?>> requiredOptions =
                sourceRule.getRequiredOptions().stream()
                        .flatMap(requiredOption -> requiredOption.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(requiredOptions.contains(MqttSourceOptions.URL));
        Assertions.assertTrue(requiredOptions.contains(MqttSourceOptions.TOPIC));
        Assertions.assertTrue(requiredOptions.contains(ConnectorCommonOptions.SCHEMA));

        List<Option<?>> optionalOptions = sourceRule.getOptionalOptions();
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.USERNAME));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.PASSWORD));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.QOS));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.FORMAT));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.FIELD_DELIMITER));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CLIENT_ID));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CLEAN_SESSION));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.CONNECTION_TIMEOUT));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.KEEP_ALIVE_INTERVAL));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.RECONNECT_TIMEOUT));
        Assertions.assertTrue(optionalOptions.contains(MqttSourceOptions.MAX_QUEUE_SIZE));
    }

    @Test
    void testValidBaseConfigPasses() {
        Assertions.assertDoesNotThrow(() -> validateSource(baseSourceConfig()));
    }

    @Test
    void testQosBoundariesPass() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.QOS.key(), 0);
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));

        cfg.put(MqttSourceOptions.QOS.key(), 1);
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));
    }

    @Test
    void testQosAboveRangeFails() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.QOS.key(), 2);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testNegativeQosFails() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.QOS.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testNonPositiveReconnectTimeoutFails() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.RECONNECT_TIMEOUT.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testNonPositiveMaxQueueSizeFails() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.MAX_QUEUE_SIZE.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testUnsupportedFormatFails() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.FORMAT.key(), "avro");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testFormatIsCaseInsensitive() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.FORMAT.key(), "JSON");
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));

        cfg.put(MqttSourceOptions.FORMAT.key(), "Text");
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));
    }

    @Test
    void testPersistentSessionRequiresClientId() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.CLEAN_SESSION.key(), false);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testPersistentSessionRequiresNonBlankClientId() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.CLEAN_SESSION.key(), false);
        cfg.put(MqttSourceOptions.CLIENT_ID.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(cfg));
    }

    @Test
    void testPersistentSessionWithClientIdPasses() {
        Map<String, Object> cfg = baseSourceConfig();
        cfg.put(MqttSourceOptions.CLEAN_SESSION.key(), false);
        cfg.put(MqttSourceOptions.CLIENT_ID.key(), "mqtt-source-client");
        Assertions.assertDoesNotThrow(() -> validateSource(cfg));
    }
}
