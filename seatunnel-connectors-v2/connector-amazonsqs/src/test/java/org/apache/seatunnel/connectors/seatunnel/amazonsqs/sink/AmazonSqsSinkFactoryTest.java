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
package org.apache.seatunnel.connectors.seatunnel.amazonsqs.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AmazonSqsSinkFactoryTest {

    private final OptionRule optionRule = new AmazonSqsSinkFactory().optionRule();

    @Test
    void testOptionRule() {
        Assertions.assertNotNull(optionRule);
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(
                () ->
                        validate(
                                configWith(
                                        "https://sqs.us-east-1.amazonaws.com/123/q", "us-east-1")));
    }

    @Test
    void testMissingUrlRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put(AmazonSqsSinkOptions.REGION.key(), "us-east-1");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testMissingRegionRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put(AmazonSqsSinkOptions.URL.key(), "https://sqs.us-east-1.amazonaws.com/123/q");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(config));
    }

    @Test
    void testEmptyUrlRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(configWith("", "us-east-1")));
    }

    @Test
    void testWhitespaceOnlyUrlRejected() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(configWith("   \t", "us-east-1")));
    }

    @Test
    void testEmptyRegionRejected() {
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(configWith("https://sqs.us-east-1.amazonaws.com/123/q", "")));
    }

    @Test
    void testWhitespaceOnlyRegionRejected() {
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> validate(configWith("https://sqs.us-east-1.amazonaws.com/123/q", "  ")));
    }

    private void validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> configWith(String url, String region) {
        Map<String, Object> config = new HashMap<>();
        config.put(AmazonSqsSinkOptions.URL.key(), url);
        config.put(AmazonSqsSinkOptions.REGION.key(), region);
        return config;
    }
}
