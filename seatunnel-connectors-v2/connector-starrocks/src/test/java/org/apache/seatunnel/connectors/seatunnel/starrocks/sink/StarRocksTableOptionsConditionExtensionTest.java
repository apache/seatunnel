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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class StarRocksTableOptionsConditionExtensionTest {

    @Test
    void testDefaultTemplateWithTableOptionsPassViaOptionRule() {
        Map<String, Object> config = starRocksSinkConfig();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("replication_num", "3");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    @Test
    void testCustomTemplateRejectsTableOptionsViaOptionRule() {
        Map<String, Object> config = starRocksSinkConfig();
        config.put(
                StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE `${database}`.`${table}` (${rowtype_fields})");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("replication_num", "3");
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), tableOptions);

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSinkOptionRule(config));
        Assertions.assertTrue(exception.getMessage().contains("custom save_mode_create_template"));
    }

    @Test
    void testAbsentTableOptionsSkipsExtension() {
        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(starRocksSinkConfig()));
    }

    @Test
    void testEmptyTableOptionsSkipsExtension() {
        Map<String, Object> config = starRocksSinkConfig();
        config.put(SinkConnectorCommonOptions.TABLE_OPTIONS.key(), new HashMap<>());

        Assertions.assertDoesNotThrow(() -> validateSinkOptionRule(config));
    }

    private static void validateSinkOptionRule(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new StarRocksSinkFactory().optionRule());
    }

    private static Map<String, Object> starRocksSinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("base-url", "jdbc:mysql://127.0.0.1:9030");
        config.put("nodeUrls", Arrays.asList("127.0.0.1:8030"));
        config.put("username", "root");
        config.put("password", "");
        config.put("database", "test");
        return config;
    }
}
