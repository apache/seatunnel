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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PulsarMultiTableConfigTest {

    @Test
    void shouldParseTablesConfigsWithGlobalFallbacks() {
        Map<String, Object> config = createBaseConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders", null),
                        createTableConfig(
                                "db.users",
                                null,
                                "persistent://public/default/users-.*",
                                "CANAL_JSON")));

        PulsarMultiTableConfig multiTableConfig =
                PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertTrue(multiTableConfig.isMultiTable());
        Assertions.assertEquals(2, multiTableConfig.getTableConfigs().size());
        Assertions.assertEquals(
                "seatunnel-sub", multiTableConfig.getTableConfigs().get(0).getSubscriptionName());
        Assertions.assertEquals(
                PulsarSourceOptions.StartMode.EARLIEST,
                multiTableConfig.getTableConfigs().get(0).getStartMode());
        Assertions.assertEquals(
                "CANAL_JSON", multiTableConfig.getTableConfigs().get(1).getFormat());
    }

    @Test
    void shouldRejectOverlappingTopicDeclarations() {
        Map<String, Object> config = createBaseConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig(
                                "db.orders",
                                "persistent://public/default/a,persistent://public/default/b",
                                null),
                        createTableConfig("db.users", "persistent://public/default/b", null)));

        PulsarConnectorException exception =
                Assertions.assertThrows(
                        PulsarConnectorException.class,
                        () -> PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertEquals(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, exception.getSeaTunnelErrorCode());
    }

    @Test
    void shouldRejectSubscriptionStartModeWithoutResetMode() {
        Map<String, Object> config = createBaseConfig();
        config.put("cursor.startup.mode", "SUBSCRIPTION");
        config.remove("cursor.reset.mode");
        config.put(
                "tables_configs",
                Collections.singletonList(
                        createTableConfig(
                                "db.orders", "persistent://public/default/orders", null)));

        PulsarConnectorException exception =
                Assertions.assertThrows(
                        PulsarConnectorException.class,
                        () -> PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertEquals(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, exception.getSeaTunnelErrorCode());
    }

    @Test
    void shouldAllowOverlappingTopicPatternsForDeterministicRuntimeRouting() {
        Map<String, Object> config = createBaseConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig(
                                "db.orders_general", null, "persistent://public/default/orders-.*"),
                        createTableConfig(
                                "db.orders_vip",
                                null,
                                "persistent://public/default/orders-vip-.*")));

        PulsarMultiTableConfig multiTableConfig =
                PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(2, multiTableConfig.getTableConfigs().size());
    }

    @Test
    void shouldRejectDuplicateTopicPatterns() {
        Map<String, Object> config = createBaseConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig(
                                "db.orders_general", null, "persistent://public/default/orders-.*"),
                        createTableConfig(
                                "db.orders_duplicate",
                                null,
                                "persistent://public/default/orders-.*")));

        PulsarConnectorException exception =
                Assertions.assertThrows(
                        PulsarConnectorException.class,
                        () -> PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertEquals(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, exception.getSeaTunnelErrorCode());
    }

    @Test
    void shouldAllowPerTableSubscriptionNameWithoutGlobalSubscription() {
        Map<String, Object> config = createBaseConfig();
        config.remove("subscription.name");
        Map<String, Object> orders =
                createTableConfig("db.orders", "persistent://public/default/orders", null);
        orders.put("subscription.name", "orders-sub");
        config.put("tables_configs", Collections.singletonList(orders));

        PulsarMultiTableConfig multiTableConfig =
                PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(
                "orders-sub", multiTableConfig.getTableConfigs().get(0).getSubscriptionName());
    }

    @Test
    void shouldRejectSingleTableWithoutSubscriptionName() {
        Map<String, Object> config = createBaseConfig();
        config.remove("subscription.name");
        config.put("topic", "persistent://public/default/orders");

        PulsarConnectorException exception =
                Assertions.assertThrows(
                        PulsarConnectorException.class,
                        () -> PulsarMultiTableConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertEquals(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, exception.getSeaTunnelErrorCode());
    }

    private Map<String, Object> createBaseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("subscription.name", "seatunnel-sub");
        config.put("client.service-url", "pulsar://localhost:6650");
        config.put("admin.service-url", "http://localhost:8080");
        config.put("format", "JSON");
        config.put("cursor.startup.mode", "EARLIEST");
        config.put("cursor.stop.mode", "NEVER");
        config.put("cursor.reset.mode", "EARLIEST");
        return config;
    }

    private Map<String, Object> createTableConfig(
            String tablePath, String topic, String topicPattern) {
        return createTableConfig(tablePath, topic, topicPattern, null);
    }

    private Map<String, Object> createTableConfig(
            String tablePath, String topic, String topicPattern, String format) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_path", tablePath);
        if (topic != null) {
            config.put("topic", topic);
        }
        if (topicPattern != null) {
            config.put("topic-pattern", topicPattern);
        }
        if (format != null) {
            config.put("format", format);
        }
        Map<String, Object> schema = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        fields.put("id", "bigint");
        schema.put("fields", fields);
        config.put("schema", schema);
        return config;
    }
}
