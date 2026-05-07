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
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergDropDataStrategy;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@Slf4j
public class IcebergSaveModeHandler extends DefaultSaveModeHandler {

    private final IcebergCatalog icebergCatalog;
    private final IcebergDropDataStrategy dropDataStrategy;
    @Nullable private final String commitBranch;

    public IcebergSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            IcebergCatalog catalog,
            CatalogTable catalogTable,
            String customSql,
            IcebergDropDataStrategy dropDataStrategy,
            @Nullable String commitBranch) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.icebergCatalog = catalog;
        this.dropDataStrategy = dropDataStrategy;
        this.commitBranch = commitBranch;
    }

    @Override
    protected void keepSchemaDropData() {
        if (!tableExists()) {
            return;
        }
        if (dropDataStrategy == IcebergDropDataStrategy.HARD_METADATA_RESET) {
            log.info(
                    "Clearing Iceberg table {} using HARD_METADATA_RESET. This removes all snapshot refs and requires orphan cleanup outside the task startup path.",
                    tablePath);
        } else {
            log.info(
                    "Clearing Iceberg table {} using DELETE_COMMIT on branch {}.",
                    tablePath,
                    commitBranch == null ? "main" : commitBranch);
        }
        icebergCatalog.truncateTable(tablePath, true, dropDataStrategy, commitBranch);
    }
}
