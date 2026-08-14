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

package org.apache.seatunnel.connectors.seatunnel.prometheus;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.prometheus.sink.PrometheusSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.prometheus.source.PrometheusSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PrometheusFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new PrometheusSourceFactory()).optionRule());
        Assertions.assertNotNull((new PrometheusSinkFactory()).optionRule());
    }

    @Test
    void testPositiveBatchSize() {
        Assertions.assertDoesNotThrow(() -> validateBatchSize(1));
    }

    @Test
    void testBatchSizeIsOptional() {
        Assertions.assertDoesNotThrow(() -> validate(sinkConfig()));
    }

    @Test
    void testNonPositiveBatchSizeFails() {
        assertInvalidBatchSize(0);
        assertInvalidBatchSize(-1);
    }

    private void assertInvalidBatchSize(int batchSize) {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateBatchSize(batchSize));
        Assertions.assertTrue(
                exception.getMessage().contains(PrometheusSinkOptions.BATCH_SIZE.key()));
    }

    private void validateBatchSize(int batchSize) {
        Map<String, Object> config = sinkConfig();
        config.put(PrometheusSinkOptions.BATCH_SIZE.key(), batchSize);
        validate(config);
    }

    private Map<String, Object> sinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(PrometheusSinkOptions.URL.key(), "http://localhost:9090");
        config.put(PrometheusSinkOptions.KEY_LABEL.key(), "label");
        config.put(PrometheusSinkOptions.KEY_VALUE.key(), "value");
        return config;
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new PrometheusSinkFactory().optionRule());
    }
}
