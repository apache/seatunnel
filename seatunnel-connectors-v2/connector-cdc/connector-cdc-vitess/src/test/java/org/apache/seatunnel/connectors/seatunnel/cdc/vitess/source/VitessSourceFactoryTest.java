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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Covers the connector-owned option rule contract for Vitess CDC. */
class VitessSourceFactoryTest {

    /** Specific startup must fail fast when the connector does not receive a reproducible VGTID. */
    @Test
    void testSpecificStartupRequiresVgtid() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                VitessSourceConfig.of(
                                        createConfig(StartupMode.SPECIFIC, null), testTable()));

        Assertions.assertTrue(
                exception.getMessage().contains("startup.specific-offset.vgtid is required"));
    }

    /** Specific startup must persist the configured VGTID into the initial SeaTunnel split. */
    @Test
    void testSpecificStartupPersistsInitialOffset() {
        String startupVgtid = "[{\"keyspace\":\"test\",\"shard\":\"-\",\"gtid\":\"MySQL56/1-10\"}]";
        VitessSourceConfig sourceConfig =
                VitessSourceConfig.of(
                        createConfig(StartupMode.SPECIFIC, startupVgtid), testTable());

        VitessSourceSplit split = sourceConfig.createInitialSplit();

        Assertions.assertEquals(startupVgtid, split.getOffset().get("vgtid"));
    }

    /**
     * Table resolution must stay inside one configured keyspace so runtime table identity is
     * stable.
     */
    @Test
    void testConfiguredKeyspaceRejectsForeignTable() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                VitessSourceConfig.of(
                                        createConfig(StartupMode.LATEST, null), foreignTable()));

        Assertions.assertTrue(
                exception.getMessage().contains("does not belong to keyspace 'test'"));
    }

    private static ReadonlyConfig createConfig(StartupMode startupMode, String specificVgtid) {
        Map<String, Object> options = new HashMap<>();
        options.put(VitessSourceOptions.HOSTNAME.key(), "127.0.0.1");
        options.put(VitessSourceOptions.KEYSPACE.key(), "test");
        options.put(VitessSourceOptions.STARTUP_MODE.key(), startupMode.name());
        options.put(
                ConnectorCommonOptions.TABLE_NAMES.key(),
                Collections.singletonList("test.products"));
        if (specificVgtid != null) {
            options.put(VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID.key(), specificVgtid);
        }
        return ReadonlyConfig.fromMap(options);
    }

    private static List<CatalogTable> testTable() {
        return Collections.singletonList(createTable("test", "products"));
    }

    private static List<CatalogTable> foreignTable() {
        return Collections.singletonList(createTable("other", "products"));
    }

    private static CatalogTable createTable(String keyspace, String tableName) {
        return CatalogTable.of(
                TableIdentifier.of(keyspace, TablePath.of(keyspace, tableName)),
                TableSchema.builder()
                        .primaryKey(
                                PrimaryKey.of("pk_" + tableName, Collections.singletonList("id")))
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }
}
