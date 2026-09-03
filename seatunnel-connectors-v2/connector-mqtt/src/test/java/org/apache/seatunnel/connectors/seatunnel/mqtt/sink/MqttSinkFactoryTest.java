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

package org.apache.seatunnel.connectors.seatunnel.mqtt.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MqttSinkFactoryTest {

    private OptionRule sinkRule;

    @BeforeEach
    void setUp() {
        sinkRule = new MqttSinkFactory().optionRule();
    }

    private static Map<String, Object> baseSinkConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(MqttSinkOptions.URL.key(), "tcp://localhost:1883");
        cfg.put(MqttSinkOptions.TOPIC.key(), "test");
        return cfg;
    }

    private void validateSink(Map<String, Object> cfg) {
        ConfigValidator.of(ReadonlyConfig.fromMap(cfg)).validate(sinkRule);
    }

    @Test
    void testOptionRule() {
        List<Option<?>> requiredOptions =
                sinkRule.getRequiredOptions().stream()
                        .flatMap(ro -> ro.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(requiredOptions.contains(MqttSinkOptions.URL));
        Assertions.assertTrue(requiredOptions.contains(MqttSinkOptions.TOPIC));

        List<Option<?>> optionalOptions = sinkRule.getOptionalOptions();
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.QOS));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.FORMAT));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.FIELD_DELIMITER));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.BATCH_SIZE));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.CLEAN_SESSION));
    }

    @Test
    void testValidBaseConfigPasses() {
        Assertions.assertDoesNotThrow(() -> validateSink(baseSinkConfig()));
    }

    @Test
    void testQosBoundariesPass() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.QOS.key(), 0);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));

        cfg.put(MqttSinkOptions.QOS.key(), 1);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }

    @Test
    void testQosAboveRangeFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.QOS.key(), 2);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testNegativeQosFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.QOS.key(), -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testNonPositiveBatchSizeFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.BATCH_SIZE.key(), 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));

        cfg.put(MqttSinkOptions.BATCH_SIZE.key(), -5);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testMinimumBatchSizePasses() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.BATCH_SIZE.key(), 1);
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }

    @Test
    void testUnsupportedFormatFails() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.FORMAT.key(), "xml");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(cfg));
    }

    @Test
    void testFormatIsCaseInsensitive() {
        Map<String, Object> cfg = baseSinkConfig();
        cfg.put(MqttSinkOptions.FORMAT.key(), "JSON");
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));

        cfg.put(MqttSinkOptions.FORMAT.key(), "Text");
        Assertions.assertDoesNotThrow(() -> validateSink(cfg));
    }
}
