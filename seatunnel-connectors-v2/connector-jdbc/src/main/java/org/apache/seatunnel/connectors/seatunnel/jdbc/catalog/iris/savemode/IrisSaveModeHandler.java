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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.savemode;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

@Slf4j
public class IrisSaveModeHandler extends DefaultSaveModeHandler {
    public boolean createIndex;

    public IrisSaveModeHandler(
            @Nonnull SchemaSaveMode schemaSaveMode,
            @Nonnull DataSaveMode dataSaveMode,
            @Nonnull Catalog catalog,
            @Nonnull TablePath tablePath,
            @Nullable CatalogTable catalogTable,
            @Nullable String customSql,
            boolean createIndex) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql);
        this.createIndex = createIndex;
    }

    @Override
    protected void createTable() {
        try {
            log.info(
                    "Creating table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.CREATE_TABLE,
                            tablePath,
                            Optional.ofNullable(catalogTable)));
            catalog.createTable(tablePath, catalogTable, true, createIndex);
        } catch (UnsupportedOperationException ignore) {
            log.info("Creating table {}", tablePath);
        }
    }
}
