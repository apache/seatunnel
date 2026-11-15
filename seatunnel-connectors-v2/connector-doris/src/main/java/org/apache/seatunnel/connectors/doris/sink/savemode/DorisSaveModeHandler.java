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
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/** Applies Doris-specific partition cleanup for the DROP_DATA save mode. */
@Slf4j
public class DorisSaveModeHandler extends DefaultSaveModeHandler {

    private final List<String> partitions;

    private final DorisCatalog dorisCatalog;

    public DorisSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            DorisCatalog catalog,
            CatalogTable catalogTable,
            String customSql,
            List<String> partitions) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.dorisCatalog = catalog;
        this.partitions = partitions;
    }

    @Override
    protected void truncateTable() {
        if (partitions.isEmpty()) {
            super.truncateTable();
            return;
        }
        log.info("Truncating partitions {} in table {}", partitions, tablePath);
        dorisCatalog.truncateTable(tablePath, true, partitions);
    }
}
