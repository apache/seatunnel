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

package org.apache.seatunnel.connectors.bigquery.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQuerySchemaChangeManagerTest {
    private static final TableIdentifier SOURCE_TABLE =
            TableIdentifier.of("mysql", "shop", "products");
    private static final TableId TARGET_TABLE =
            TableId.of("test-project", "test_dataset", "test_table");

    private Map<String, Object> options;
    private BigQuery bigQuery;
    private BigQuerySchemaChangeManager manager;

    @BeforeEach
    void setUp() throws InterruptedException {
        options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), TARGET_TABLE.getProject());
        options.put(BigQuerySinkOptions.DATASET_ID.key(), TARGET_TABLE.getDataset());
        options.put(BigQuerySinkOptions.TABLE_ID.key(), TARGET_TABLE.getTable());

        bigQuery = mock(BigQuery.class);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mock(TableResult.class));
        manager = new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery);
    }

    @Test
    void testApplyNullableAddColumn() throws Exception {
        mockTargetSchema(Field.of("email", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `email` STRING",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testApplyRepeatedAddColumn() throws Exception {
        mockTargetSchema(
                Field.newBuilder("tags", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("tags", ArrayType.STRING_ARRAY_TYPE, false));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `tags` ARRAY<STRING>",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectNullableArrayColumn() throws Exception {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("tags", ArrayType.STRING_ARRAY_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("cannot preserve nullable array semantics"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testSkipDdlWhenCompatibleColumnAlreadyExists() throws Exception {
        mockExistingTargetSchema(Field.of("email", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testApplyMultipleAddColumnsAsOneStatement() throws Exception {
        mockTargetSchema(
                Field.of("score", StandardSQLTypeName.INT64),
                Field.of("amount", StandardSQLTypeName.NUMERIC));
        AlterTableColumnsEvent event =
                new AlterTableColumnsEvent(SOURCE_TABLE)
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        SOURCE_TABLE, column("score", BasicType.INT_TYPE, true)))
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        SOURCE_TABLE,
                                        column("amount", new DecimalType(20, 2), true)));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `score` INT64, "
                        + "ADD COLUMN IF NOT EXISTS `amount` NUMERIC(20, 2)",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectRequiredColumn() throws Exception {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("required_value", BasicType.STRING_TYPE, false));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertEquals(
                BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED, exception.getSeaTunnelErrorCode());
        assertTrue(exception.getMessage().contains("REQUIRED"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRelaxRequiredColumnToNullable() throws Exception {
        options.put(BigQuerySinkOptions.SCHEMA_EVOLUTION_RELAX_NOT_NULL.key(), true);
        manager = new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery);
        mockTargetSchema(Field.of("required_value", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("required_value", BasicType.STRING_TYPE, false));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `required_value` STRING",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectDropColumn() throws Exception {
        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(SOURCE_TABLE, "obsolete");

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("only supports ADD COLUMN"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingColumnWithDifferentType() throws Exception {
        mockExistingTargetSchema(Field.of("email", StandardSQLTypeName.INT64));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingRequiredColumn() throws Exception {
        mockExistingTargetSchema(
                Field.newBuilder("email", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    private void mockTargetSchema(Field... fields) {
        Table beforeSchemaChange = mockTable(Schema.of());
        Table afterSchemaChange = mockTable(Schema.of(fields));
        when(bigQuery.getTable(TARGET_TABLE)).thenReturn(beforeSchemaChange, afterSchemaChange);
    }

    private void mockExistingTargetSchema(Field... fields) {
        Table existingTable = mockTable(Schema.of(fields));
        when(bigQuery.getTable(TARGET_TABLE)).thenReturn(existingTable);
    }

    private Table mockTable(Schema schema) {
        Table table = mock(Table.class);
        TableDefinition definition = mock(TableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getSchema()).thenReturn(schema);
        return table;
    }

    private static Column column(
            String name,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> type,
            boolean nullable) {
        return PhysicalColumn.of(name, type, (Long) null, nullable, null, null);
    }
}
