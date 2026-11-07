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

package org.apache.seatunnel.connectors.sensorsdata.sdk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataOptions;
import org.apache.seatunnel.connectors.sensorsdata.sdk.config.SensorsDataSDKSinkOptions;
import org.apache.seatunnel.connectors.sensorsdata.sdk.sink.SensorsDataSDKSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SensorsDataSDKFactoryTest {

    private static final OptionRule OPTION_RULE = new SensorsDataSDKSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(OPTION_RULE);
    }

    @Test
    void testValidServerUrlAndConsumerConfigurations() {
        Map<String, Object> batchConfig = validConfig();
        batchConfig.put(SensorsDataSDKSinkOptions.CONSUMER.key(), "batch");
        Assertions.assertDoesNotThrow(() -> validate(batchConfig));

        Map<String, Object> consoleConfig = validConfig();
        consoleConfig.put(SensorsDataSDKSinkOptions.CONSUMER.key(), "console");
        Assertions.assertDoesNotThrow(() -> validate(consoleConfig));
    }

    @Test
    void testMissingServerUrlRejected() {
        Map<String, Object> config = validConfig();
        config.remove(SensorsDataSDKSinkOptions.SERVER_URL.key());

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testEmptyServerUrlRejected() {
        Map<String, Object> config = validConfig();
        config.put(SensorsDataSDKSinkOptions.SERVER_URL.key(), "");

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testWhitespaceOnlyServerUrlRejected() {
        Map<String, Object> config = validConfig();
        config.put(SensorsDataSDKSinkOptions.SERVER_URL.key(), "  \t");

        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testOmittedConsumerDefaultsToBatch() {
        ReadonlyConfig config = validate(validConfig());

        Assertions.assertEquals("batch", config.get(SensorsDataSDKSinkOptions.CONSUMER));
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.of(readonlyConfig).validate(OPTION_RULE);
        return readonlyConfig;
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                SensorsDataSDKSinkOptions.SERVER_URL.key(),
                "https://localhost:8106/sa?project=default");
        config.put(SensorsDataOptions.ENTITY_NAME.key(), "items");
        config.put(SensorsDataOptions.RECORD_TYPE.key(), "users");
        return config;
    }
}
