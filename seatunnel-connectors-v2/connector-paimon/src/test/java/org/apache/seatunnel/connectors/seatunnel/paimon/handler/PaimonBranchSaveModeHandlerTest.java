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

package org.apache.seatunnel.connectors.seatunnel.paimon.handler;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.SupportLoadTable;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssignerFactory;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PaimonBranchSaveModeHandlerTest {

    private static final String CATALOG_NAME = "paimon_catalog";
    private static final String DATABASE_NAME = "default";
    private static final String TABLE_NAME = "branch_table";
    private static final String BRANCH_NAME = "test_branch";

    @TempDir private Path temporaryFolder;

    private PaimonCatalog paimonCatalog;
    private ReadonlyConfig readonlyConfig;
    private TablePath tablePath;
    private CatalogTable catalogTable;

    @BeforeEach
    public void before() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", temporaryFolder.toString());
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        properties.put("branch", BRANCH_NAME);
        properties.put("paimon.table.write-props", new HashMap<String, String>());
        readonlyConfig = ReadonlyConfig.fromMap(properties);

        tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        catalogTable = createCatalogTable(TABLE_NAME);

        paimonCatalog = new PaimonCatalog(CATALOG_NAME, readonlyConfig);
        paimonCatalog.open();
        paimonCatalog.createDatabase(tablePath, true);
        paimonCatalog.createTable(tablePath, catalogTable, false);

        FileStoreTable table = (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
        if (!table.branchManager().branchExists(BRANCH_NAME)) {
            table.createBranch(BRANCH_NAME);
        }
    }

    @Test
    public void createSchemaWithBranchShouldFailWithoutCreatingMainTableWhenTableMissing() {
        String missingTableName = "missing_branch_table";
        TablePath missingTablePath = TablePath.of(DATABASE_NAME, missingTableName);
        CatalogTable missingCatalogTable = createCatalogTable(missingTableName);
        PaimonSaveModeHandler handler =
                new PaimonSaveModeHandler(
                        new TestSupportLoadTable(),
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        paimonCatalog,
                        missingCatalogTable,
                        null,
                        BRANCH_NAME);

        PaimonConnectorException exception =
                Assertions.assertThrows(
                        PaimonConnectorException.class, handler::handleSchemaSaveMode);
        Assertions.assertTrue(
                exception.getMessage().contains("main table must exist"),
                "The error message should explain that branch writes require an existing table.");
        Assertions.assertFalse(
                paimonCatalog.tableExists(missingTablePath),
                "Branch save mode must not auto-create a main table.");
    }

    @Test
    public void createSchemaWithBranchShouldFailWhenBranchMissing() {
        String tableWithoutBranch = "table_without_branch";
        TablePath tableWithoutBranchPath = TablePath.of(DATABASE_NAME, tableWithoutBranch);
        CatalogTable tableWithoutBranchCatalogTable = createCatalogTable(tableWithoutBranch);
        paimonCatalog.createTable(tableWithoutBranchPath, tableWithoutBranchCatalogTable, false);
        PaimonSaveModeHandler handler =
                new PaimonSaveModeHandler(
                        new TestSupportLoadTable(),
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        paimonCatalog,
                        tableWithoutBranchCatalogTable,
                        null,
                        BRANCH_NAME);

        PaimonConnectorException exception =
                Assertions.assertThrows(
                        PaimonConnectorException.class, handler::handleSchemaSaveMode);
        Assertions.assertTrue(
                exception.getMessage().contains("does not exist"),
                "The error message should explain that the branch does not exist.");
        Assertions.assertTrue(paimonCatalog.tableExists(tableWithoutBranchPath));
        Assertions.assertFalse(
                ((FileStoreTable) paimonCatalog.getPaimonTable(tableWithoutBranchPath))
                        .branchManager()
                        .branchExists(BRANCH_NAME),
                "Branch save mode must not auto-create the branch.");
    }

    @Test
    public void dropDataWithBranchShouldFailWithoutDroppingMainTableOrBranch() {
        PaimonSaveModeHandler handler =
                new PaimonSaveModeHandler(
                        new TestSupportLoadTable(),
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        paimonCatalog,
                        catalogTable,
                        null,
                        BRANCH_NAME);

        PaimonConnectorException exception =
                Assertions.assertThrows(
                        PaimonConnectorException.class, handler::handleSchemaSaveMode);
        Assertions.assertTrue(
                exception.getMessage().contains("data_save_mode=DROP_DATA"),
                "The error message should explain the unsupported save mode.");

        FileStoreTable mainTable = (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
        Assertions.assertTrue(
                mainTable.branchManager().branchExists(BRANCH_NAME),
                "DROP_DATA on a branch must not delete branch metadata.");
    }

    @Test
    public void recreateSchemaWithBranchShouldFailWithoutDroppingMainTableOrBranch() {
        PaimonSaveModeHandler handler =
                new PaimonSaveModeHandler(
                        new TestSupportLoadTable(),
                        SchemaSaveMode.RECREATE_SCHEMA,
                        DataSaveMode.APPEND_DATA,
                        paimonCatalog,
                        catalogTable,
                        null,
                        BRANCH_NAME);

        PaimonConnectorException exception =
                Assertions.assertThrows(
                        PaimonConnectorException.class, handler::handleSchemaSaveMode);
        Assertions.assertTrue(
                exception.getMessage().contains("schema_save_mode=RECREATE_SCHEMA"),
                "The error message should explain the unsupported save mode.");

        FileStoreTable mainTable = (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
        Assertions.assertTrue(
                mainTable.branchManager().branchExists(BRANCH_NAME),
                "RECREATE_SCHEMA on a branch must not delete branch metadata.");
    }

    @Test
    public void restoreSchemaSaveModeWithBranchShouldFailForDestructiveSaveMode() {
        PaimonSaveModeHandler handler =
                new PaimonSaveModeHandler(
                        new TestSupportLoadTable(),
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        paimonCatalog,
                        catalogTable,
                        null,
                        BRANCH_NAME);

        PaimonConnectorException exception =
                Assertions.assertThrows(
                        PaimonConnectorException.class, handler::handleSchemaSaveModeWithRestore);
        Assertions.assertTrue(
                exception.getMessage().contains("data_save_mode=DROP_DATA"),
                "The error message should explain the unsupported save mode.");
    }

    @Test
    public void schemaEvolutionWithBranchShouldAlterBranchSchemaOnly() throws Exception {
        PaimonSinkConfig sinkConfig = new PaimonSinkConfig(readonlyConfig);
        PaimonSinkWriter writer =
                new PaimonSinkWriter(
                        new TestSinkWriterContext(),
                        readonlyConfig,
                        catalogTable,
                        ((FileStoreTable) paimonCatalog.getPaimonTable(tablePath))
                                .switchToBranch(BRANCH_NAME),
                        UUID.randomUUID().toString(),
                        null,
                        sinkConfig,
                        new PaimonHadoopConfiguration(),
                        new PaimonBucketAssignerFactory());
        try {
            writer.applySchemaChange(
                    AlterTableAddColumnEvent.add(
                            catalogTable.getTableId(),
                            PhysicalColumn.of(
                                    "branch_only_column",
                                    BasicType.STRING_TYPE,
                                    (Long) null,
                                    true,
                                    null,
                                    null)));
        } finally {
            writer.close();
        }

        FileStoreTable mainTable = (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
        FileStoreTable branchTable = mainTable.switchToBranch(BRANCH_NAME);
        Assertions.assertFalse(
                mainTable.schema().fieldNames().contains("branch_only_column"),
                "Schema evolution configured with a branch must not alter the main schema.");
        Assertions.assertTrue(
                branchTable.schema().fieldNames().contains("branch_only_column"),
                "Schema evolution configured with a branch must alter the branch schema.");
    }

    @AfterEach
    public void after() {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    private static TableSchema baseSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, false, null, null))
                .primaryKey(
                        org.apache.seatunnel.api.table.catalog.PrimaryKey.of(
                                "pk", Collections.singletonList("id")))
                .build();
    }

    private static CatalogTable createCatalogTable(String tableName) {
        return CatalogTable.of(
                TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, tableName),
                baseSchema(),
                new HashMap<>(),
                Collections.emptyList(),
                "branch test table");
    }

    private static class TestSupportLoadTable implements SupportLoadTable<Table> {
        private Table table;

        @Override
        public void setLoadTable(Table table) {
            this.table = table;
        }

        @Override
        public Table getLoadTable() {
            return table;
        }
    }

    private static class TestSinkWriterContext
            implements org.apache.seatunnel.api.sink.SinkWriter.Context {
        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public org.apache.seatunnel.api.common.metrics.MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public org.apache.seatunnel.api.event.EventListener getEventListener() {
            return null;
        }
    }
}
