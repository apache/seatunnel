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

package org.apache.seatunnel.api.table.schema.handler;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class RestoreTableSchemaEventTest {

    @Test
    void dispatchersReplaceRuntimeSchemaWithCheckpointSchema() {
        CatalogTable initialTable = table(false);
        CatalogTable restoredTable = table(true);
        RestoreTableSchemaEvent event = new RestoreTableSchemaEvent(restoredTable);

        SeaTunnelRowType restoredRowType =
                new DataTypeChangeEventDispatcher()
                        .reset(initialTable.getSeaTunnelRowType())
                        .apply(event);
        TableSchema restoredSchema =
                new TableSchemaChangeEventDispatcher()
                        .reset(initialTable.getTableSchema())
                        .apply(event);
        TableSchema restoredAlterSchema =
                new AlterTableSchemaEventHandler()
                        .reset(initialTable.getTableSchema())
                        .apply(event);

        Assertions.assertEquals(restoredTable.getSeaTunnelRowType(), restoredRowType);
        Assertions.assertEquals(
                restoredTable.getTableSchema().getColumns(), restoredSchema.getColumns());
        Assertions.assertEquals(
                restoredTable.getTableSchema().getColumns(), restoredAlterSchema.getColumns());
        Assertions.assertSame(restoredTable, event.getChangeAfter());
    }

    @Test
    void restoreEventRejectsNullCatalogTable() {
        Assertions.assertThrows(
                NullPointerException.class, () -> new RestoreTableSchemaEvent(null));
    }

    @Test
    void dispatchersFailFastWhenRestoreEventLosesChangeAfter() {
        CatalogTable initialTable = table(false);
        RestoreTableSchemaEvent event = new RestoreTableSchemaEvent(table(true));
        event.setChangeAfter(null);

        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        new DataTypeChangeEventDispatcher()
                                .reset(initialTable.getSeaTunnelRowType())
                                .apply(event));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        new TableSchemaChangeEventDispatcher()
                                .reset(initialTable.getTableSchema())
                                .apply(event));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        new AlterTableSchemaEventHandler()
                                .reset(initialTable.getTableSchema())
                                .apply(event));
    }

    private static CatalogTable table(boolean includeEmail) {
        TableSchema.Builder schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""));
        if (includeEmail) {
            schema.column(PhysicalColumn.of("email", BasicType.STRING_TYPE, 128L, true, null, ""));
        }
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "customers"),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }
}
