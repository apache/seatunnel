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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.bigquery.catalog.BigQueryCatalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQueryCatalogAndSaveModeTest {

    @Mock private BigQuery mockBigQuery;
    private BigQueryCatalog catalog;
    private ReadonlyConfig mockConfig;

    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("project_id", "test-project");
        configMap.put("dataset_id", "test-dataset");
        configMap.put("write_mode", "batch");
        mockConfig = ReadonlyConfig.fromMap(configMap);

        catalog = new BigQueryCatalog("test-catalog", mockConfig);

        // Inject the mocked BigQuery client into the catalog using reflection
        java.lang.reflect.Field clientField = BigQueryCatalog.class.getDeclaredField("bigquery");
        clientField.setAccessible(true);
        clientField.set(catalog, mockBigQuery);
    }

    @Test
    public void testDatabaseExists() {
        when(mockBigQuery.getDataset("existing-db")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getDataset("non-existing-db")).thenReturn(null);

        assertTrue(catalog.databaseExists("existing-db"));
        assertTrue(!catalog.databaseExists("non-existing-db"));
    }

    @Test
    public void testTableExists() {
        TablePath tablePath = TablePath.of("test-dataset", "test-table");
        TableId tableId = TableId.of("test-dataset", "test-table");
        when(mockBigQuery.getTable(tableId)).thenReturn(mock(Table.class));

        assertTrue(catalog.tableExists(tablePath));
    }

    @Test
    public void testCreateTableCase1_NewTable() throws Exception {
        TablePath tablePath = TablePath.of("test-dataset", "new-table");
        TableId tableId = TableId.of("test-dataset", "new-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getTable(tableId)).thenReturn(null);

        catalog.createTable(tablePath, catalogTable, false);

        verify(mockBigQuery, times(1)).create(any(TableInfo.class));
    }

    @Test
    public void testCreateTableCase2_ExistingTableThrowsError() {
        TablePath tablePath = TablePath.of("test-dataset", "existing-table");
        TableId tableId = TableId.of("test-dataset", "existing-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getTable(tableId)).thenReturn(mock(Table.class));

        assertThrows(
                TableAlreadyExistException.class,
                () -> catalog.createTable(tablePath, catalogTable, false));
    }

    @Test
    public void testDropTable() throws Exception {
        TablePath tablePath = TablePath.of("test-dataset", "drop-table");
        TableId tableId = TableId.of("test-dataset", "drop-table");

        when(mockBigQuery.delete(tableId)).thenReturn(true);

        catalog.dropTable(tablePath, false);

        verify(mockBigQuery, times(1)).delete(tableId);
    }

    @Test
    public void testSaveModeHandler_RecreateSchema() {
        TablePath tablePath = TablePath.of("test-dataset", "recreate-table");
        TableId tableId = TableId.of("test-dataset", "recreate-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getTable(tableId)).thenReturn(mock(Table.class)).thenReturn(null);
        when(mockBigQuery.delete(tableId)).thenReturn(true);

        BigQuerySaveModeHandler handler =
                new BigQuerySaveModeHandler(
                        SchemaSaveMode.RECREATE_SCHEMA,
                        DataSaveMode.APPEND_DATA,
                        catalog,
                        tablePath,
                        catalogTable,
                        null);

        handler.handleSchemaSaveMode();

        // Should drop the existing table and recreate it
        verify(mockBigQuery, times(1)).delete(tableId);
        verify(mockBigQuery, times(1)).create(any(TableInfo.class));
    }

    @Test
    public void testSaveModeHandler_CreateSchemaWhenNotExist_ExistsSkip() {
        TablePath tablePath = TablePath.of("test-dataset", "exists-table");
        TableId tableId = TableId.of("test-dataset", "exists-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getTable(tableId)).thenReturn(mock(Table.class));

        BigQuerySaveModeHandler handler =
                new BigQuerySaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        catalog,
                        tablePath,
                        catalogTable,
                        null);

        handler.handleSchemaSaveMode();

        // Should skip both deletion and creation
        verify(mockBigQuery, never()).delete(tableId);
        verify(mockBigQuery, never()).create(any(TableInfo.class));
    }

    @Test
    public void testSaveModeHandler_CoherenceCheckFailsForTypeMismatch() {
        TablePath tablePath = TablePath.of("test-dataset", "mismatch-table");
        TableId tableId = TableId.of("test-dataset", "mismatch-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        Table mockTable = mock(Table.class);
        TableDefinition mockDef = mock(TableDefinition.class);
        when(mockTable.getDefinition()).thenReturn(mockDef);

        // Remote table has STRING, while source is INT64
        com.google.cloud.bigquery.Schema remoteSchema =
                com.google.cloud.bigquery.Schema.of(Field.of("id", StandardSQLTypeName.STRING));
        when(mockDef.getSchema()).thenReturn(remoteSchema);

        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));
        when(mockBigQuery.getTable(tableId)).thenReturn(mockTable);

        BigQuerySaveModeHandler handler =
                new BigQuerySaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        catalog,
                        tablePath,
                        catalogTable,
                        null);

        // Executing the schema save mode should throw a CatalogException for type incompatibility
        assertThrows(CatalogException.class, () -> handler.handleSchemaSaveMode());
    }

    // ────────────────────────────────────────────────────────────────────────
    // 3D COMBINATORIAL PARAMETERIZED MATRIX TESTING
    // ────────────────────────────────────────────────────────────────────────

    public enum TableState {
        NOT_EXIST,
        EXIST_CORRECT_SCHEMA,
        EXIST_WRONG_SCHEMA
    }

    public enum ExpectedOutcome {
        SUCCESS,
        THROWS_TABLE_NOT_EXIST,
        THROWS_COHERENCE_MISMATCH,
        THROWS_DATA_ALREADY_EXISTS
    }

    private static Stream<Arguments> saveModeMatrixProvider() {
        List<Arguments> cases = new ArrayList<>();
        for (SchemaSaveMode schemaSaveMode : SchemaSaveMode.values()) {
            for (DataSaveMode dataSaveMode : DataSaveMode.values()) {
                for (TableState tableState : TableState.values()) {
                    for (boolean isBatch : new boolean[] {true, false}) {
                        ExpectedOutcome outcome =
                                determineExpectedOutcome(schemaSaveMode, dataSaveMode, tableState);
                        cases.add(
                                Arguments.of(
                                        schemaSaveMode,
                                        dataSaveMode,
                                        tableState,
                                        isBatch,
                                        outcome));
                    }
                }
            }
        }
        return cases.stream();
    }

    private static ExpectedOutcome determineExpectedOutcome(
            SchemaSaveMode schemaSaveMode, DataSaveMode dataSaveMode, TableState tableState) {

        // Phase 1: Determine Schema Stage behavior
        boolean isNewTableCreated = false;

        if (tableState == TableState.NOT_EXIST) {
            if (schemaSaveMode == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
                return ExpectedOutcome.THROWS_TABLE_NOT_EXIST; // Fatal Schema Error
            }
            if (schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA
                    || schemaSaveMode == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
                isNewTableCreated = true;
            }
        } else {
            // Table exists
            if (schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA) {
                isNewTableCreated = true;
            }
        }

        // Schema Coherence Validation: Only applies if the table already existed (not newly
        // created)
        // and we aren't ignoring schema changes or recreating schema.
        if (!isNewTableCreated && tableState == TableState.EXIST_WRONG_SCHEMA) {
            if (schemaSaveMode == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST
                    || schemaSaveMode == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
                return ExpectedOutcome.THROWS_COHERENCE_MISMATCH; // Schema Coherence mismatch error
            }
        }

        // Phase 2: Data Stage behavior
        if (!isNewTableCreated && tableState != TableState.NOT_EXIST) {
            if (dataSaveMode == DataSaveMode.ERROR_WHEN_DATA_EXISTS) {
                return ExpectedOutcome.THROWS_DATA_ALREADY_EXISTS; // Fatal Data Error
            }
        }

        return ExpectedOutcome.SUCCESS;
    }

    @ParameterizedTest(
            name = "Case: Schema={0}, Data={1}, TableState={2}, Batch={3} -> Expected={4}")
    @MethodSource("saveModeMatrixProvider")
    public void testCombinatorialSaveModeMatrix(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            TableState tableState,
            boolean isBatch,
            ExpectedOutcome expected)
            throws Exception {

        TablePath tablePath = TablePath.of("test-dataset", "matrix-table");
        TableId tableId = TableId.of("test-dataset", "matrix-table");
        CatalogTable catalogTable = createMockCatalogTable(tablePath);

        // Stub Dataset exist checks
        when(mockBigQuery.getDataset("test-dataset")).thenReturn(mock(Dataset.class));

        // Mock target table behavior based on the testing state
        if (tableState == TableState.NOT_EXIST) {
            if (schemaSaveMode == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
                Table mockTable = mock(Table.class);
                TableDefinition mockDef = mock(TableDefinition.class);
                when(mockTable.getDefinition()).thenReturn(mockDef);
                when(mockTable.getNumRows()).thenReturn(java.math.BigInteger.ZERO);
                com.google.cloud.bigquery.Schema correctSchema =
                        com.google.cloud.bigquery.Schema.of(
                                Field.of("id", StandardSQLTypeName.INT64));
                when(mockDef.getSchema()).thenReturn(correctSchema);

                when(mockBigQuery.getTable(tableId)).thenReturn(null).thenReturn(mockTable);
            } else {
                when(mockBigQuery.getTable(tableId)).thenReturn(null);
            }
        } else {
            Table mockTable = mock(Table.class);
            TableDefinition mockDef = mock(TableDefinition.class);
            when(mockTable.getDefinition()).thenReturn(mockDef);
            when(mockTable.getNumRows())
                    .thenReturn(java.math.BigInteger.valueOf(100)); // Has existing rows

            if (tableState == TableState.EXIST_CORRECT_SCHEMA) {
                com.google.cloud.bigquery.Schema correctSchema =
                        com.google.cloud.bigquery.Schema.of(
                                Field.of("id", StandardSQLTypeName.INT64));
                when(mockDef.getSchema()).thenReturn(correctSchema);
            } else {
                com.google.cloud.bigquery.Schema wrongSchema =
                        com.google.cloud.bigquery.Schema.of(
                                Field.of("id", StandardSQLTypeName.STRING));
                when(mockDef.getSchema()).thenReturn(wrongSchema);
            }

            // Sequential stubbing: first getTable returns existing table, second returns null
            // (after delete/recreate)
            if (schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA) {
                when(mockBigQuery.getTable(tableId)).thenReturn(mockTable).thenReturn(null);
            } else {
                when(mockBigQuery.getTable(tableId)).thenReturn(mockTable);
            }
        }

        // Mock deletion success
        when(mockBigQuery.delete(tableId)).thenReturn(true);

        // Dynamically construct a new BigQueryCatalog for each test run using isBatch
        Map<String, Object> localConfigMap = new HashMap<>();
        localConfigMap.put("project_id", "test-project");
        localConfigMap.put("dataset_id", "test-dataset");
        localConfigMap.put("write_mode", isBatch ? "batch" : "streaming");
        ReadonlyConfig localConfig = ReadonlyConfig.fromMap(localConfigMap);

        BigQueryCatalog localCatalog = new BigQueryCatalog("test-catalog", localConfig);

        // Inject the mocked BigQuery client into this localized catalog using reflection
        java.lang.reflect.Field clientField = BigQueryCatalog.class.getDeclaredField("bigquery");
        clientField.setAccessible(true);
        clientField.set(localCatalog, mockBigQuery);

        BigQuerySaveModeHandler handler =
                new BigQuerySaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        localCatalog,
                        tablePath,
                        catalogTable,
                        "SELECT 1");

        // Run assertions according to expected outcomes
        if (expected == ExpectedOutcome.THROWS_TABLE_NOT_EXIST) {
            assertThrows(
                    SeaTunnelRuntimeException.class,
                    () -> {
                        handler.handleSchemaSaveMode();
                    });
        } else if (expected == ExpectedOutcome.THROWS_COHERENCE_MISMATCH) {
            assertThrows(
                    CatalogException.class,
                    () -> {
                        handler.handleSchemaSaveMode();
                    });
        } else if (expected == ExpectedOutcome.THROWS_DATA_ALREADY_EXISTS) {
            assertThrows(
                    SeaTunnelRuntimeException.class,
                    () -> {
                        handler.handleSchemaSaveMode();
                        handler.handleDataSaveMode();
                    });
        } else {
            // SUCCESS
            assertDoesNotThrow(
                    () -> {
                        handler.handleSchemaSaveMode();
                        handler.handleDataSaveMode();
                    });
        }
    }

    private CatalogTable createMockCatalogTable(TablePath tablePath) {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, "Primary key"));
        builder.primaryKey(
                org.apache.seatunnel.api.table.catalog.PrimaryKey.of(
                        "id_pk", Collections.singletonList("id")));
        return CatalogTable.of(
                TableIdentifier.of("test-catalog", tablePath),
                builder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "BigQuery Catalog Test Table");
    }
}
