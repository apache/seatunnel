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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergDropDataStrategy;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IcebergSaveModeHandlerTest {

    private static final TablePath TABLE_PATH = TablePath.of("database.table");

    private static final CatalogTable SOURCE_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "source_col",
                                            BasicType.STRING_TYPE,
                                            (Long) null,
                                            true,
                                            null,
                                            ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "source");

    @Test
    void shouldUseDeleteCommitStrategyByDefault() {
        IcebergCatalog catalog = mock(IcebergCatalog.class);
        when(catalog.tableExists(TABLE_PATH)).thenReturn(true);
        IcebergSaveModeHandler handler =
                new IcebergSaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        SOURCE_TABLE,
                        null,
                        IcebergDropDataStrategy.DELETE_COMMIT,
                        null);

        handler.handleDataSaveMode();

        verify(catalog).tableExists(TABLE_PATH);
        verify(catalog)
                .truncateTable(TABLE_PATH, true, IcebergDropDataStrategy.DELETE_COMMIT, null);
    }

    @Test
    void shouldUseHardMetadataResetStrategyWhenConfigured() {
        IcebergCatalog catalog = mock(IcebergCatalog.class);
        when(catalog.tableExists(TABLE_PATH)).thenReturn(true);

        IcebergSaveModeHandler handler =
                new IcebergSaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.DROP_DATA,
                        catalog,
                        SOURCE_TABLE,
                        null,
                        IcebergDropDataStrategy.HARD_METADATA_RESET,
                        "st_branch");

        handler.handleDataSaveMode();

        verify(catalog).tableExists(TABLE_PATH);
        verify(catalog)
                .truncateTable(
                        TABLE_PATH, true, IcebergDropDataStrategy.HARD_METADATA_RESET, "st_branch");
        verify(catalog, never()).getTable(TABLE_PATH);
        verify(catalog, never()).createTable(any(), any(), anyBoolean());
    }
}
