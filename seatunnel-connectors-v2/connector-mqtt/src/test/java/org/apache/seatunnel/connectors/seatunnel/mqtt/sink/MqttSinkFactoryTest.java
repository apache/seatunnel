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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MqttSinkFactoryTest {

    @Test
    void testOptionRule() {
        MqttSinkFactory factory = new MqttSinkFactory();
        OptionRule rule = factory.optionRule();

        List<Option<?>> requiredOptions =
                rule.getRequiredOptions().stream()
                        .flatMap(ro -> ro.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(requiredOptions.contains(MqttSinkOptions.URL));
        Assertions.assertTrue(requiredOptions.contains(MqttSinkOptions.TOPIC));

        List<Option<?>> optionalOptions = rule.getOptionalOptions();
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.QOS));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.FIELD_DELIMITER));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.BATCH_SIZE));
        Assertions.assertTrue(optionalOptions.contains(MqttSinkOptions.CLEAN_SESSION));
    }

    @Test
    void testOptionRuleAcceptsDefaultsAndValidRanges() {
        validate(baseConfig());
        Map<String, Object> values = baseConfig();
        values.put("qos", 0);
        values.put("batch_size", 1);
        validate(values);
        values.put("qos", 1);
        values.put("batch_size", Integer.MAX_VALUE);
        validate(values);
    }

    @Test
    void testOptionRuleRejectsQosOutOfRange() {
        assertInvalidOption("qos", -1);
        assertInvalidOption("qos", 2);
    }

    @Test
    void testOptionRuleRejectsBatchSizeBelowOne() {
        assertInvalidOption("batch_size", 0);
        assertInvalidOption("batch_size", -1);
    }

    private static void assertInvalidOption(String key, Object value) {
        Map<String, Object> values = baseConfig();
        values.put(key, value);
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(values));
        Assertions.assertTrue(exception.getMessage().contains(key));
    }

    private static void validate(Map<String, Object> values) {
        ConfigValidator.of(ReadonlyConfig.fromMap(values))
                .validate(new MqttSinkFactory().optionRule());
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("url", "tcp://localhost:1883");
        values.put("topic", "test");
        return values;
    }
}
