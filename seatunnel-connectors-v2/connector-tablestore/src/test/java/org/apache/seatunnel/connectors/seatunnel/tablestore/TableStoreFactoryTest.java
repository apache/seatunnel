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

package org.apache.seatunnel.connectors.seatunnel.tablestore;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.sink.TableStoreSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.tablestore.source.TableStoreSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TableStoreFactoryTest {

    private static final List<String> REQUIRED_STRING_OPTIONS =
            Arrays.asList(
                    TableStoreSourceOptions.END_POINT.key(),
                    TableStoreSourceOptions.INSTANCE_NAME.key(),
                    TableStoreSourceOptions.ACCESS_KEY_ID.key(),
                    TableStoreSourceOptions.ACCESS_KEY_SECRET.key(),
                    TableStoreSourceOptions.TABLE.key());

    private final OptionRule sourceOptionRule = new TableStoreSourceFactory().optionRule();
    private final OptionRule sinkOptionRule = new TableStoreSinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceOptionRule);
        Assertions.assertNotNull(sinkOptionRule);
    }

    @Test
    void testValidSourceConfiguration() {
        Assertions.assertDoesNotThrow(() -> validate(sourceConfig(), sourceOptionRule));

        Map<String, Object> multiTable = sourceConfig();
        multiTable.put(TableStoreSourceOptions.TABLE.key(), "orders,users");
        multiTable.put(TableStoreSourceOptions.PRIMARY_KEYS.key(), Arrays.asList("id", "id"));
        Assertions.assertDoesNotThrow(() -> validate(multiTable, sourceOptionRule));
    }

    @Test
    void testSourceRejectsMissingRequiredOptions() {
        for (String key : REQUIRED_STRING_OPTIONS) {
            assertMissingRejected(sourceConfig(), key, sourceOptionRule);
        }
        assertMissingRejected(
                sourceConfig(), TableStoreSourceOptions.PRIMARY_KEYS.key(), sourceOptionRule);
        assertMissingRejected(
                sourceConfig(), ConnectorCommonOptions.SCHEMA.key(), sourceOptionRule);
    }

    @Test
    void testSourceRejectsBlankStringOptions() {
        for (String key : REQUIRED_STRING_OPTIONS) {
            assertValueRejected(sourceConfig(), key, "", sourceOptionRule);
            assertValueRejected(sourceConfig(), key, "   \t", sourceOptionRule);
        }
    }

    @Test
    void testSourceRejectsEmptyPrimaryKeys() {
        assertValueRejected(
                sourceConfig(),
                TableStoreSourceOptions.PRIMARY_KEYS.key(),
                Collections.emptyList(),
                sourceOptionRule);
    }

    @Test
    void testValidSinkConfiguration() {
        Assertions.assertDoesNotThrow(() -> validate(sinkConfig(), sinkOptionRule));

        Map<String, Object> withoutBatchSize = sinkConfig();
        withoutBatchSize.remove(TableStoreSinkOptions.BATCH_SIZE.key());
        Assertions.assertDoesNotThrow(() -> validate(withoutBatchSize, sinkOptionRule));
    }

    @Test
    void testSinkRejectsMissingRequiredOptions() {
        for (String key : REQUIRED_STRING_OPTIONS) {
            assertMissingRejected(sinkConfig(), key, sinkOptionRule);
        }
        assertMissingRejected(
                sinkConfig(), TableStoreSinkOptions.PRIMARY_KEYS.key(), sinkOptionRule);
        assertMissingRejected(sinkConfig(), ConnectorCommonOptions.SCHEMA.key(), sinkOptionRule);
    }

    @Test
    void testSinkRejectsBlankStringOptions() {
        for (String key : REQUIRED_STRING_OPTIONS) {
            assertValueRejected(sinkConfig(), key, "", sinkOptionRule);
            assertValueRejected(sinkConfig(), key, "   \t", sinkOptionRule);
        }
    }

    @Test
    void testSinkRejectsEmptyPrimaryKeys() {
        assertValueRejected(
                sinkConfig(),
                TableStoreSinkOptions.PRIMARY_KEYS.key(),
                Collections.emptyList(),
                sinkOptionRule);
    }

    private void assertMissingRejected(
            Map<String, Object> config, String key, OptionRule optionRule) {
        config.remove(key);
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(config, optionRule), key);
        Assertions.assertTrue(exception.getMessage().contains(key), exception.getMessage());
    }

    private void assertValueRejected(
            Map<String, Object> config, String key, Object value, OptionRule optionRule) {
        config.put(key, value);
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(config, optionRule), key);
        Assertions.assertTrue(exception.getMessage().contains(key), exception.getMessage());
    }

    private void validate(Map<String, Object> config, OptionRule optionRule) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "Tablestore");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
    }

    private Map<String, Object> commonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                TableStoreSourceOptions.END_POINT.key(),
                "https://instance.cn-hangzhou.ots.aliyuncs.com");
        config.put(TableStoreSourceOptions.INSTANCE_NAME.key(), "instance");
        config.put(TableStoreSourceOptions.ACCESS_KEY_ID.key(), "access-key-id");
        config.put(TableStoreSourceOptions.ACCESS_KEY_SECRET.key(), "access-key-secret");
        config.put(TableStoreSourceOptions.TABLE.key(), "orders");
        config.put(
                TableStoreSourceOptions.PRIMARY_KEYS.key(), Collections.singletonList("order_id"));
        Map<String, Object> fields = new HashMap<>();
        fields.put("order_id", "string");
        fields.put("amount", "double");
        config.put(ConnectorCommonOptions.SCHEMA.key(), Collections.singletonMap("fields", fields));
        return config;
    }

    private Map<String, Object> sourceConfig() {
        return commonConfig();
    }

    private Map<String, Object> sinkConfig() {
        Map<String, Object> config = commonConfig();
        config.put(TableStoreSinkOptions.BATCH_SIZE.key(), 100);
        return config;
    }
}
