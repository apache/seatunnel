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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Covers factory-path validation, schema selection, and restore semantics for Vitess CDC. */
class VitessSourceFactoryTest {

    private final VitessSourceFactory factory = new VitessSourceFactory();

    /** Specific startup must fail fast when the connector does not receive a reproducible VGTID. */
    @Test
    void testSpecificStartupRequiresVgtid() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                VitessSourceConfig.of(
                                        createConfig(
                                                StartupMode.SPECIFIC,
                                                null,
                                                null,
                                                singleTableConfigs("test.products")),
                                        testTable()));

        Assertions.assertTrue(
                exception.getMessage().contains("startup.specific-offset.vgtid is required"));
    }

    /** Specific startup must persist both the offset and the checkpoint table schema snapshot. */
    @Test
    void testSpecificStartupPersistsInitialCheckpointState() {
        String startupVgtid = "[{\"keyspace\":\"test\",\"shard\":\"-\",\"gtid\":\"MySQL56/1-10\"}]";
        VitessSourceConfig sourceConfig =
                VitessSourceConfig.of(
                        createConfig(
                                StartupMode.SPECIFIC,
                                startupVgtid,
                                null,
                                singleTableConfigs("test.products")),
                        testTable());

        VitessSourceSplit split = sourceConfig.createInitialSplit();

        Assertions.assertEquals(startupVgtid, split.getOffset().get("vgtid"));
        Assertions.assertNotNull(split.getTableSchemas());
        Assertions.assertNotNull(split.getCheckpointTables());
        Assertions.assertEquals(1, split.getCheckpointTables().size());
    }

    /**
     * The factory must reject configs that declare tables without deterministic schema metadata.
     */
    @Test
    void testFactoryRequiresExplicitSchemaMetadata() {
        Map<String, Object> options = baseOptions(StartupMode.LATEST, null);
        options.put("table-names", Collections.singletonList("test.products"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(options), getClass().getClassLoader());

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> factory.createSource(context).createSource());

        Assertions.assertTrue(exception.getMessage().contains("requires explicit schema metadata"));
    }

    /** The factory must create sources from declared table schemas instead of catalog discovery. */
    @Test
    void testFactoryCreatesSourceFromDeclaredTableConfigs() {
        TableSourceFactoryContext context =
                createContext(
                        StartupMode.LATEST,
                        null,
                        null,
                        multiTableConfigs("test.products", "test.customers"));

        VitessSource source = createVitessSource(factory.createSource(context));

        Assertions.assertEquals(2, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                Arrays.asList("test.products", "test.customers"),
                Arrays.asList(
                        source.getProducedCatalogTables().get(0).getTablePath().toString(),
                        source.getProducedCatalogTables().get(1).getTablePath().toString()));
    }

    /**
     * Root-level schema must work for the single-table path instead of requiring tables_configs.
     */
    @Test
    void testFactoryCreatesSourceFromRootSchema() {
        TableSourceFactoryContext context =
                createContext(
                        StartupMode.LATEST,
                        Collections.singletonList("test.products"),
                        null,
                        singleTableConfigs("test.products"));

        VitessSource source = createVitessSource(factory.createSource(context));

        Assertions.assertEquals(1, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "test.products",
                source.getProducedCatalogTables().get(0).getTablePath().toString());
        Assertions.assertEquals(
                3, source.getProducedCatalogTables().get(0).getTableSchema().getColumns().size());
    }

    /** table-pattern must filter the declared schema list deterministically before startup. */
    @Test
    void testFactoryFiltersDeclaredTablesByPattern() {
        Map<String, Object> options = baseOptions(StartupMode.LATEST, null);
        options.put("table-pattern", "test\\.prod.*");
        options.put("tables_configs", multiTableConfigs("test.products", "test.customers"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(options), getClass().getClassLoader());

        VitessSource source = createVitessSource(factory.createSource(context));

        Assertions.assertEquals(1, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "test.products",
                source.getProducedCatalogTables().get(0).getTablePath().toString());
    }

    /**
     * Restore must reuse the checkpoint table snapshot so the SeaTunnel row converter stays aligned
     * with the split state instead of silently downgrading to the latest config.
     */
    @Test
    void testRestoreSourceUsesCheckpointTables() {
        List<CatalogTable> checkpointTables =
                Collections.singletonList(
                        createTable(
                                "test",
                                "products",
                                Arrays.asList(
                                        physicalColumn("id", BasicType.INT_TYPE),
                                        physicalColumn("name", BasicType.STRING_TYPE),
                                        physicalColumn("description", BasicType.STRING_TYPE))));
        VitessSourceSplit split =
                VitessSourceConfig.of(
                                createConfig(
                                        StartupMode.SPECIFIC,
                                        "[{\"keyspace\":\"test\",\"shard\":\"-\",\"gtid\":\"MySQL56/1-10\"}]",
                                        Collections.singletonList("test.products"),
                                        singleTableConfigs("test.products")),
                                checkpointTables)
                        .createInitialSplit();
        ChangeStreamTableSourceState<Serializable, VitessSourceSplit> state =
                new ChangeStreamTableSourceState<>(
                        null, Collections.singletonList(Collections.singletonList(split)));

        TableSourceFactoryContext context =
                createContext(
                        StartupMode.LATEST,
                        Collections.singletonList("test.products"),
                        null,
                        singleTableConfigsWithoutDescription("test.products"));

        VitessSource restoredSource = createVitessSource(factory.restoreSource(context, state));

        Assertions.assertEquals(
                3,
                restoredSource
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .size());
        Assertions.assertEquals(
                "description",
                restoredSource
                        .getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(2)
                        .getName());
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
                                        createConfig(
                                                StartupMode.LATEST,
                                                null,
                                                Collections.singletonList("test.products"),
                                                singleTableConfigs("test.products")),
                                        foreignTable()));

        Assertions.assertTrue(
                exception.getMessage().contains("does not belong to keyspace 'test'"));
    }

    private VitessSource createVitessSource(TableSource<?, VitessSourceSplit, ?> tableSource) {
        return (VitessSource) tableSource.createSource();
    }

    private TableSourceFactoryContext createContext(
            StartupMode startupMode,
            List<String> tableNames,
            String tablePattern,
            List<Map<String, Object>> tableConfigs) {
        return new TableSourceFactoryContext(
                createConfig(startupMode, null, tableNames, tableConfigs, tablePattern),
                getClass().getClassLoader());
    }

    private ReadonlyConfig createConfig(
            StartupMode startupMode,
            String specificVgtid,
            List<String> tableNames,
            List<Map<String, Object>> tableConfigs) {
        return createConfig(startupMode, specificVgtid, tableNames, tableConfigs, null);
    }

    private ReadonlyConfig createConfig(
            StartupMode startupMode,
            String specificVgtid,
            List<String> tableNames,
            List<Map<String, Object>> tableConfigs,
            String tablePattern) {
        Map<String, Object> options = baseOptions(startupMode, specificVgtid);
        if (tableNames != null) {
            options.put("table-names", tableNames);
        }
        if (tablePattern != null) {
            options.put("table-pattern", tablePattern);
        }
        if (tableConfigs.size() == 1) {
            options.put("schema", tableConfigs.get(0).get("schema"));
        } else {
            options.put("tables_configs", tableConfigs);
        }
        return ReadonlyConfig.fromMap(options);
    }

    private Map<String, Object> baseOptions(StartupMode startupMode, String specificVgtid) {
        Map<String, Object> options = new HashMap<>();
        options.put(VitessSourceOptions.HOSTNAME.key(), "127.0.0.1");
        options.put(VitessSourceOptions.KEYSPACE.key(), "test");
        options.put(VitessSourceOptions.STARTUP_MODE.key(), startupMode.name());
        if (specificVgtid != null) {
            options.put(VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID.key(), specificVgtid);
        }
        return options;
    }

    private List<Map<String, Object>> singleTableConfigs(String tableName) {
        return Collections.singletonList(tableConfig(tableName, true));
    }

    private List<Map<String, Object>> singleTableConfigsWithoutDescription(String tableName) {
        return Collections.singletonList(tableConfig(tableName, false));
    }

    private List<Map<String, Object>> multiTableConfigs(String... tableNames) {
        List<Map<String, Object>> configs = new ArrayList<>(tableNames.length);
        for (String tableName : tableNames) {
            configs.add(tableConfig(tableName, tableName.endsWith("products")));
        }
        return configs;
    }

    private Map<String, Object> tableConfig(String tableName, boolean withDescription) {
        List<Map<String, Object>> columns = new ArrayList<>();
        columns.add(column("id", "int"));
        columns.add(column("name", "string"));
        if (withDescription) {
            columns.add(column("description", "string"));
        }

        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put("name", "pk_" + tableName.substring(tableName.indexOf('.') + 1));
        primaryKey.put("columnNames", Collections.singletonList("id"));

        Map<String, Object> schema = new HashMap<>();
        schema.put("table", tableName);
        schema.put("columns", columns);
        schema.put("primaryKey", primaryKey);

        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("schema", schema);
        return tableConfig;
    }

    private Map<String, Object> column(String name, String type) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        return column;
    }

    private static List<CatalogTable> testTable() {
        return Collections.singletonList(
                createTable(
                        "test",
                        "products",
                        Arrays.asList(
                                physicalColumn("id", BasicType.INT_TYPE),
                                physicalColumn("name", BasicType.STRING_TYPE),
                                physicalColumn("description", BasicType.STRING_TYPE))));
    }

    private static List<CatalogTable> foreignTable() {
        return Collections.singletonList(
                createTable(
                        "other",
                        "products",
                        Collections.singletonList(physicalColumn("id", BasicType.INT_TYPE))));
    }

    private static CatalogTable createTable(
            String keyspace, String tableName, List<PhysicalColumn> columns) {
        TableSchema.Builder schemaBuilder =
                TableSchema.builder()
                        .primaryKey(
                                PrimaryKey.of("pk_" + tableName, Collections.singletonList("id")));
        columns.forEach(schemaBuilder::column);
        return CatalogTable.of(
                TableIdentifier.of(keyspace, TablePath.of(keyspace, tableName)),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static PhysicalColumn physicalColumn(String name, BasicType<?> dataType) {
        return PhysicalColumn.builder().name(name).dataType(dataType).build();
    }
}
