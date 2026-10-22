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

package org.apache.seatunnel.connectors.seatunnel.maxcompute;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.sink.MaxcomputeSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.MaxcomputeSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MaxcomputeSourceFactoryTest {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://service.odps.aliyun.com/api",
                " \thttp://service.odps.aliyun.com/api\r\n "
            })
    void acceptsNonblankEndpointWithoutTrimming(String endpoint) {
        Map<String, Object> config = sourceConfig();
        config.put(MaxcomputeSourceOptions.ENDPOINT.key(), endpoint);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(readonlyConfig)
                                .validate(new MaxcomputeSourceFactory().optionRule()));
        Assertions.assertEquals(endpoint, readonlyConfig.get(MaxcomputeSourceOptions.ENDPOINT));
    }

    @Test
    void rejectsMissingEndpoint() {
        Map<String, Object> config = sourceConfig();
        config.remove(MaxcomputeSourceOptions.ENDPOINT.key());
        assertInvalidEndpoint(config);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n", "\r", " \t\r\n "})
    void rejectsBlankEndpoint(String endpoint) {
        Map<String, Object> config = sourceConfig();
        config.put(MaxcomputeSourceOptions.ENDPOINT.key(), endpoint);
        assertInvalidEndpoint(config);
    }

    private Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(MaxcomputeSourceOptions.ENDPOINT.key(), "http://service.odps.aliyun.com/api");
        config.put(MaxcomputeSourceOptions.TABLE_NAME.key(), "test_table");
        return config;
    }

    private void assertInvalidEndpoint(Map<String, Object> config) {
        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.of(ReadonlyConfig.fromMap(config))
                                        .validate(new MaxcomputeSourceFactory().optionRule()));
        Assertions.assertTrue(error.getMessage().contains(MaxcomputeSourceOptions.ENDPOINT.key()));
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new MaxcomputeSourceFactory()).optionRule());
        Assertions.assertNotNull((new MaxcomputeSinkFactory()).optionRule());
    }

    /**
     * The six client timeout / retry options (3 for the ODPS REST client, 3 for the Tunnel client)
     * must be declared as optional in both the source and sink factory option rules, otherwise
     * users cannot override them from job configs.
     */
    @Test
    void optionRuleRegistersTimeoutAndRetryOptions() {
        assertTimeoutAndRetryOptionsRegistered(new MaxcomputeSourceFactory().optionRule());
        assertTimeoutAndRetryOptionsRegistered(new MaxcomputeSinkFactory().optionRule());
    }

    private void assertTimeoutAndRetryOptionsRegistered(OptionRule rule) {
        Set<String> optionalKeys = new HashSet<>();
        for (Option<?> option : rule.getOptionalOptions()) {
            optionalKeys.add(option.key());
        }
        // REST client (control plane)
        Assertions.assertTrue(
                optionalKeys.contains("connect_timeout_ms"), "connect_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("read_timeout_ms"), "read_timeout_ms not registered");
        Assertions.assertTrue(optionalKeys.contains("retry_times"), "retry_times not registered");
        // Tunnel client (data plane)
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_connect_timeout_ms"),
                "tunnel_connect_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_read_timeout_ms"),
                "tunnel_read_timeout_ms not registered");
        Assertions.assertTrue(
                optionalKeys.contains("tunnel_retry_times"), "tunnel_retry_times not registered");
    }
}
