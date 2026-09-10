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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRestorePlanGeneratorTest {

    private static final TableIdentifier TABLE_ID =
            TableIdentifier.of("catalog", "database", "table");

    @Test
    void testCompactPlanReconstructsTargetAfterMultipleChanges() {
        CatalogTable initial =
                table(
                        column("id", BasicType.INT_TYPE, 11L),
                        column("old_name", BasicType.STRING_TYPE, 64L),
                        column("kept", BasicType.STRING_TYPE, 64L));
        CatalogTable target =
                table(
                        column("kept", BasicType.STRING_TYPE, 128L),
                        column("id", BasicType.LONG_TYPE, 20L),
                        column("new_name", BasicType.STRING_TYPE, 256L));
        AlterTableAddColumnEvent latestEvent =
                AlterTableAddColumnEvent.add(
                        TABLE_ID, column("new_name", BasicType.STRING_TYPE, 256L));
        latestEvent.setJobId("job-1");
        latestEvent.setStatement("ALTER TABLE ...");
        latestEvent.setSourceDialectName("MySQL");
        latestEvent.setChangeAfter(target);

        AlterTableColumnsEvent restorePlan =
                SchemaRestorePlanGenerator.generate(initial, latestEvent);

        List<AlterTableColumnEvent> operations = restorePlan.getEvents();
        assertEquals(4, operations.size());
        assertTrue(operations.get(0) instanceof AlterTableDropColumnEvent);
        assertEquals("old_name", ((AlterTableDropColumnEvent) operations.get(0)).getColumn());
        assertTrue(operations.get(1) instanceof AlterTableAddColumnEvent);
        assertTrue(((AlterTableAddColumnEvent) operations.get(1)).isFirst());
        assertEquals("kept", ((AlterTableAddColumnEvent) operations.get(1)).getColumn().getName());
        assertEquals("kept", ((AlterTableAddColumnEvent) operations.get(2)).getAfterColumn());
        assertEquals("id", ((AlterTableAddColumnEvent) operations.get(3)).getAfterColumn());
        for (AlterTableColumnEvent operation : operations) {
            assertEquals("job-1", operation.getJobId());
            assertEquals("ALTER TABLE ...", operation.getStatement());
            assertEquals("MySQL", operation.getSourceDialectName());
        }
        assertEquals("job-1", restorePlan.getJobId());
        assertEquals("ALTER TABLE ...", restorePlan.getStatement());
        assertEquals("MySQL", restorePlan.getSourceDialectName());
        assertEquals(target.getTableId(), restorePlan.getChangeAfter().getTableId());
        assertEquals(target.getTableSchema(), restorePlan.getChangeAfter().getTableSchema());

        TableSchema rebuilt =
                new TableSchemaChangeEventDispatcher()
                        .reset(initial.getTableSchema())
                        .apply(restorePlan);
        assertEquals(target.getTableSchema(), rebuilt);
    }

    @Test
    void testDuplicateTargetColumnFailsClosed() {
        CatalogTable initial = table(column("id", BasicType.INT_TYPE, 11L));
        CatalogTable target =
                table(
                        column("id", BasicType.INT_TYPE, 11L),
                        column("id", BasicType.LONG_TYPE, 20L));
        AlterTableAddColumnEvent latestEvent =
                AlterTableAddColumnEvent.add(TABLE_ID, column("id", BasicType.LONG_TYPE, 20L));
        latestEvent.setChangeAfter(target);

        SchemaEvolutionException exception =
                assertThrows(
                        SchemaEvolutionException.class,
                        () -> SchemaRestorePlanGenerator.generate(initial, latestEvent));

        assertTrue(exception.getMessage().contains("duplicate column id"));
    }

    @Test
    void testCatalogNormalizationDoesNotChangeTableIdentity() {
        TableIdentifier initialTableId = TableIdentifier.of("MySQL", "database", "table");
        TableIdentifier eventTableId = TableIdentifier.of("", "database", "table");
        CatalogTable initial = table(initialTableId, column("id", BasicType.INT_TYPE, 11L));
        CatalogTable target =
                table(
                        initialTableId,
                        column("id", BasicType.INT_TYPE, 11L),
                        column("new_name", BasicType.STRING_TYPE, 256L));
        AlterTableAddColumnEvent latestEvent =
                AlterTableAddColumnEvent.add(
                        eventTableId, column("new_name", BasicType.STRING_TYPE, 256L));
        latestEvent.setChangeAfter(target);

        AlterTableColumnsEvent restorePlan =
                SchemaRestorePlanGenerator.generate(initial, latestEvent);

        assertEquals(eventTableId, restorePlan.tableIdentifier());
        assertEquals(target.getTableSchema(), restorePlan.getChangeAfter().getTableSchema());
    }

    private static CatalogTable table(Column... columns) {
        return table(TABLE_ID, columns);
    }

    private static CatalogTable table(TableIdentifier tableIdentifier, Column... columns) {
        return CatalogTable.of(
                tableIdentifier,
                TableSchema.builder().columns(Arrays.asList(columns)).build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static Column column(
            String name,
            org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> type,
            long length) {
        return PhysicalColumn.of(name, type, length, true, null, null);
    }
}
