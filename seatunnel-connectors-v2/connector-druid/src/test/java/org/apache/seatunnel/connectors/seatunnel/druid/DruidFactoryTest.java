/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.druid;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.druid.config.DruidSinkOptions;
import org.apache.seatunnel.connectors.druid.sink.DruidSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DruidFactoryTest {

    private final OptionRule optionRule = new DruidSinkFactory().optionRule();

    @Test
    public void optionRuleTest() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(requiredConfig()));
    }

    @Test
    void testMissingRequiredOptionsRejected() {
        for (String key :
                new String[] {
                    DruidSinkOptions.COORDINATOR_URL.key(), DruidSinkOptions.DATASOURCE.key()
                }) {
            Map<String, Object> config = requiredConfig();
            config.remove(key);
            Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
        }
    }

    @Test
    void testBlankRequiredOptionsRejected() {
        for (String key :
                new String[] {
                    DruidSinkOptions.COORDINATOR_URL.key(), DruidSinkOptions.DATASOURCE.key()
                }) {
            for (String value : new String[] {"", " \t\r\n "}) {
                Map<String, Object> config = requiredConfig();
                config.put(key, value);
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
            }
        }
    }

    @Test
    void testDefaultBatchSize() {
        ReadonlyConfig config = validate(requiredConfig());
        Assertions.assertEquals(
                DruidSinkOptions.BATCH_SIZE_DEFAULT, config.get(DruidSinkOptions.BATCH_SIZE));
    }

    @Test
    void testPositiveBatchSize() {
        Assertions.assertDoesNotThrow(() -> validateBatchSize(1));
        Assertions.assertDoesNotThrow(() -> validateBatchSize(100));
    }

    @Test
    void testNonPositiveBatchSizeRejected() {
        assertInvalidBatchSize(0);
        assertInvalidBatchSize(-1);
    }

    private void assertInvalidBatchSize(int batchSize) {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateBatchSize(batchSize));
        Assertions.assertTrue(exception.getMessage().contains(DruidSinkOptions.BATCH_SIZE.key()));
    }

    private ReadonlyConfig validateBatchSize(int batchSize) {
        Map<String, Object> config = requiredConfig();
        config.put(DruidSinkOptions.BATCH_SIZE.key(), batchSize);
        return validate(config);
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "DruidSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }

    private Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(DruidSinkOptions.COORDINATOR_URL.key(), "localhost:8888");
        config.put(DruidSinkOptions.DATASOURCE.key(), "seatunnel");
        return config;
    }
}
