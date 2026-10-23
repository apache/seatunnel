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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.snapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostgresSnapshotSplitReadTaskTest {

    @Test
    public void testResolveTableFallsBackToCatalogQualifiedTableId() throws Exception {
        TableId tableIdWithoutCatalog = new TableId(null, "public", "users");
        TableId tableIdWithCatalog = new TableId("traffic", "public", "users");
        Table table = table(tableIdWithCatalog);

        PostgresSchema databaseSchema = mock(PostgresSchema.class);
        when(databaseSchema.tableFor(tableIdWithoutCatalog)).thenReturn(null);
        when(databaseSchema.tableFor(tableIdWithCatalog)).thenReturn(table);

        Table resolvedTable =
                resolveTable(
                        newSnapshotSplitReadTask(
                                mock(PostgresConnectorConfig.class), databaseSchema),
                        tableIdWithCatalog);

        Assertions.assertSame(table, resolvedTable);
    }

    @Test
    public void testResolveTableFallsBackToConfiguredDatabaseName() throws Exception {
        TableId tableIdWithoutCatalog = new TableId(null, "public", "users");
        TableId tableIdWithCatalog = new TableId("traffic", "public", "users");
        Table table = table(tableIdWithCatalog);

        PostgresConnectorConfig connectorConfig = mock(PostgresConnectorConfig.class);
        when(connectorConfig.databaseName()).thenReturn("traffic");

        PostgresSchema databaseSchema = mock(PostgresSchema.class);
        when(databaseSchema.tableFor(tableIdWithoutCatalog)).thenReturn(null);
        when(databaseSchema.tableFor(tableIdWithCatalog)).thenReturn(table);

        Table resolvedTable =
                resolveTable(
                        newSnapshotSplitReadTask(connectorConfig, databaseSchema),
                        tableIdWithoutCatalog);

        Assertions.assertSame(table, resolvedTable);
    }

    private static PostgresSnapshotSplitReadTask newSnapshotSplitReadTask(
            PostgresConnectorConfig connectorConfig, PostgresSchema databaseSchema) {
        return new PostgresSnapshotSplitReadTask(
                connectorConfig,
                null,
                mock(SnapshotProgressListener.class),
                databaseSchema,
                null,
                null,
                null);
    }

    private static Table resolveTable(PostgresSnapshotSplitReadTask task, TableId tableId)
            throws Exception {
        Method method =
                PostgresSnapshotSplitReadTask.class.getDeclaredMethod(
                        "resolveTable", TableId.class);
        method.setAccessible(true);
        return (Table) method.invoke(task, tableId);
    }

    private static Table table(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor().name("id").type("int4").create())
                .create();
    }
}
