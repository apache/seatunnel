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

package org.apache.seatunnel.connectors.seatunnel.paimon.handler;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.SupportLoadTable;

import org.apache.paimon.table.Table;

public class PaimonSaveModeHandler extends DefaultSaveModeHandler {

    private SupportLoadTable<Table> supportLoadTable;
    private Catalog catalog;
    private CatalogTable catalogTable;

    public PaimonSaveModeHandler(
            SupportLoadTable supportLoadTable,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.supportLoadTable = supportLoadTable;
        this.catalog = catalog;
        this.catalogTable = catalogTable;
    }

    @Override
    public void handleSchemaSaveMode() {
        super.handleSchemaSaveMode();
        TablePath tablePath = catalogTable.getTablePath();
        Table paimonTable = ((PaimonCatalog) catalog).getPaimonTable(tablePath);
        // load paimon table and set it into paimon sink
        this.supportLoadTable.setLoadTable(paimonTable);
    }
}
