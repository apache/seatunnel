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

package org.apache.seatunnel.connectors.seatunnel;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.config.DingTalkSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.sink.DingTalkSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DingTalkFactoryTest {

    private final OptionRule optionRule = new DingTalkSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidRequiredOptions() {
        Assertions.assertDoesNotThrow(() -> validate(requiredConfig()));
    }

    @Test
    void testEmptyRequiredOptionsRejected() {
        assertInvalidValue(DingTalkSinkOptions.URL.key(), "");
        assertInvalidValue(DingTalkSinkOptions.SECRET.key(), "");
    }

    @Test
    void testWhitespaceOnlyRequiredOptionsRejected() {
        assertInvalidValue(DingTalkSinkOptions.URL.key(), " ");
        assertInvalidValue(DingTalkSinkOptions.SECRET.key(), "\t");
    }

    private void assertInvalidValue(String optionKey, String value) {
        Map<String, Object> config = requiredConfig();
        config.put(optionKey, value);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "DingTalkSink");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> requiredConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                DingTalkSinkOptions.URL.key(),
                "https://oapi.dingtalk.com/robot/send?access_token=test-token");
        config.put(DingTalkSinkOptions.SECRET.key(), "test-secret");
        return config;
    }
}
