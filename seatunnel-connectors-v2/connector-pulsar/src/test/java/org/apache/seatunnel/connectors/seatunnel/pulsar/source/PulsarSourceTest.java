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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class PulsarSourceTest {

    @Test
    void shouldExposeProducedCatalogTablesForTablesConfigs() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertEquals(2, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "db.orders",
                source.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
        Assertions.assertEquals(
                "db.users",
                source.getProducedCatalogTables().get(1).getTableId().toTablePath().toString());
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void shouldUseBoundednessOfAllTables() {
        Map<String, Object> config = createBaseConfig("LATEST");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void shouldRejectBatchModeForUnboundedMultiTableSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertThrows(
                PulsarConnectorException.class,
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldKeepSingleTableBatchCompatibilityForUnboundedSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put("topic", "persistent://public/default/orders");

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldKeepSingleTableTablesConfigsBatchCompatibilityForUnboundedSource() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders")));

        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(
                () -> source.setJobContext(new JobContext().setJobMode(JobMode.BATCH)));
    }

    @Test
    void shouldRejectSingleTableWithoutSubscriptionNameInFactory() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.remove("subscription.name");
        config.put("topic", "persistent://public/default/orders");

        PulsarSourceFactory factory = new PulsarSourceFactory();

        Assertions.assertThrows(
                PulsarConnectorException.class,
                () ->
                        factory.createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(config),
                                        Thread.currentThread().getContextClassLoader())));
    }

    @Test
    void shouldAllowPerTableSubscriptionNameInFactoryForTablesConfigs() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.remove("subscription.name");
        Map<String, Object> tableConfig =
                createTableConfig("db.orders", "persistent://public/default/orders");
        tableConfig.put("subscription.name", "orders-sub");
        config.put("tables_configs", Arrays.asList(tableConfig));

        PulsarSourceFactory factory = new PulsarSourceFactory();

        Assertions.assertDoesNotThrow(
                () ->
                        factory.createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(config),
                                                Thread.currentThread().getContextClassLoader()))
                                .createSource());
    }

    @Test
    void shouldNotTouchPulsarAdminDuringSourceConstructionForTopicPatternTables() {
        Map<String, Object> config = createBaseConfig("NEVER");
        config.put("admin.service-url", "invalid-admin-url");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createPatternTableConfig(
                                "db.orders_pattern", "persistent://public/default/orders.*")));

        Assertions.assertDoesNotThrow(
                () ->
                        new PulsarSource(
                                ReadonlyConfig.fromMap(config),
                                CatalogTableUtil.buildSimpleTextTable()));
    }

    @Test
    void shouldSerializeMultiTableSourceForSparkExecution() {
        Map<String, Object> config = createBaseConfig("LATEST");
        config.put(
                "tables_configs",
                Arrays.asList(
                        createTableConfig("db.orders", "persistent://public/default/orders"),
                        createTableConfig("db.users", "persistent://public/default/users")));
        PulsarSource source =
                new PulsarSource(
                        ReadonlyConfig.fromMap(config), CatalogTableUtil.buildSimpleTextTable());

        Assertions.assertDoesNotThrow(() -> SerializationUtils.serialize(source));
    }

    private Map<String, Object> createBaseConfig(String stopMode) {
        Map<String, Object> config = new HashMap<>();
        config.put("subscription.name", "seatunnel-sub");
        config.put("client.service-url", "pulsar://localhost:6650");
        config.put("admin.service-url", "http://localhost:8080");
        config.put("format", "JSON");
        config.put("cursor.startup.mode", "EARLIEST");
        config.put("cursor.stop.mode", stopMode);
        config.put("cursor.reset.mode", "EARLIEST");
        return config;
    }

    private Map<String, Object> createTableConfig(String tablePath, String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_path", tablePath);
        config.put("topic", topic);
        return config;
    }

    private Map<String, Object> createPatternTableConfig(String tablePath, String topicPattern) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_path", tablePath);
        config.put("topic-pattern", topicPattern);
        return config;
    }
}
