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

package org.apache.seatunnel.connectors.doris.sink.savemode;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisSaveModeHandlerTest {

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("doris", "test_db", "test_table");
    private static final TablePath TABLE_PATH = TABLE_IDENTIFIER.toTablePath();

    @Test
    void dropDataTruncatesConfiguredPartitions() {
        DorisCatalog catalog = mock(DorisCatalog.class);
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTableId()).thenReturn(TABLE_IDENTIFIER);
        when(catalog.tableExists(TABLE_PATH)).thenReturn(true);
        DorisSaveModeHandler handler =
                new DorisSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable,
                        null,
                        Arrays.asList("p1", "p2"));

        handler.handleDataSaveMode();

        verify(catalog).truncateTable(TABLE_PATH, true, Arrays.asList("p1", "p2"));
        verify(catalog, never()).truncateTable(TABLE_PATH, true);
    }

    @Test
    void dropDataWithoutPartitionsPreservesWholeTableCleanup() {
        DorisCatalog catalog = mock(DorisCatalog.class);
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTableId()).thenReturn(TABLE_IDENTIFIER);
        when(catalog.tableExists(TABLE_PATH)).thenReturn(true);
        DorisSaveModeHandler handler =
                new DorisSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable,
                        null,
                        Collections.emptyList());

        handler.handleDataSaveMode();

        verify(catalog).truncateTable(TABLE_PATH, true);
        verify(catalog, never()).truncateTable(TABLE_PATH, true, Collections.emptyList());
    }

    @Test
    void appendDataDoesNotTruncatePartitions() {
        DorisCatalog catalog = mock(DorisCatalog.class);
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTableId()).thenReturn(TABLE_IDENTIFIER);
        DorisSaveModeHandler handler =
                new DorisSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.APPEND_DATA,
                        catalog,
                        catalogTable,
                        null,
                        Collections.singletonList("p1"));

        handler.handleDataSaveMode();

        verify(catalog, never()).truncateTable(TABLE_PATH, true);
        verify(catalog, never()).truncateTable(TABLE_PATH, true, Collections.singletonList("p1"));
    }

    @Test
    void dropDataDoesNotTruncateNewlyCreatedTable() {
        DorisCatalog catalog = mock(DorisCatalog.class);
        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTableId()).thenReturn(TABLE_IDENTIFIER);
        when(catalog.databaseExists(TABLE_PATH.getDatabaseName())).thenReturn(true);
        when(catalog.tableExists(TABLE_PATH)).thenReturn(false, true);
        DorisSaveModeHandler handler =
                new DorisSaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        catalogTable,
                        null,
                        Collections.singletonList("p1"));

        handler.handleSchemaSaveMode();
        handler.handleDataSaveMode();

        verify(catalog).createTable(TABLE_PATH, catalogTable, true);
        verify(catalog, never()).truncateTable(TABLE_PATH, true);
        verify(catalog, never()).truncateTable(TABLE_PATH, true, Collections.singletonList("p1"));
    }
}
