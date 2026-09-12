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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PaimonTableOptionsConditionExtensionTest {

    @Test
    void testValidTableOptionsPassViaOptionRule() {
        Map<String, Object> config = paimonSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", "4");
        tableOptions.put("file.format", "parquet");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testBlankKeyRejectedViaOptionRule() {
        Map<String, Object> config = paimonSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("  ", "4");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("blank property key"));
    }

    @Test
    void testNullValueRejectedViaOptionRule() {
        Map<String, Object> config = paimonSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("bucket", null);
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("null value"));
    }

    @Test
    void testAbsentTableOptionsSkipsExtension() {
        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(paimonSinkConfig()));
    }

    @Test
    void testEmptyTableOptionsSkipsExtension() {
        Map<String, Object> config = paimonSinkConfig();
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), new HashMap<>());

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    private static void validateSinkOptionRule(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new PaimonSinkFactory().optionRule());
    }

    private static Map<String, Object> paimonSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(PaimonSinkOptions.WAREHOUSE.key(), "file:///tmp/paimon");
        config.put(PaimonSinkOptions.DATABASE.key(), "db");
        config.put(PaimonSinkOptions.TABLE.key(), "t");
        return config;
    }
}
