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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.SupportLoadTable;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;

public class PaimonSaveModeHandler extends DefaultSaveModeHandler {

    private SupportLoadTable<Table> supportLoadTable;
    private Catalog catalog;
    private CatalogTable catalogTable;
    private String branch;

    public PaimonSaveModeHandler(
            SupportLoadTable supportLoadTable,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql,
            String branch) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.supportLoadTable = supportLoadTable;
        this.catalog = catalog;
        this.catalogTable = catalogTable;
        this.branch = branch;
    }

    @Override
    public void handleSchemaSaveMode() {
        checkBranchSaveMode();
        super.handleSchemaSaveMode();
        TablePath tablePath = catalogTable.getTablePath();
        Table paimonTable = ((PaimonCatalog) catalog).getPaimonTable(tablePath);
        Table loadTable = this.supportLoadTable.getLoadTable();
        if (loadTable == null || this.schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA) {
            if (isNonMainBranch()) {
                paimonTable = ((FileStoreTable) paimonTable).switchToBranch(branch);
            }
            this.supportLoadTable.setLoadTable(paimonTable);
        }
    }

    @Override
    public void handleDataSaveMode() {
        checkBranchSaveMode();
        super.handleDataSaveMode();
    }

    @Override
    public void handleSchemaSaveModeWithRestore() {
        checkBranchSaveMode();
        super.handleSchemaSaveModeWithRestore();
    }

    private boolean isNonMainBranch() {
        return StringUtils.isNotEmpty(branch)
                && !BranchManager.DEFAULT_MAIN_BRANCH.equalsIgnoreCase(branch);
    }

    private void checkBranchSaveMode() {
        if (!isNonMainBranch()) {
            return;
        }
        if (!catalog.tableExists(tablePath)) {
            throw unsupportedBranchSaveMode(
                    "The main table must exist before writing to a non-main branch.");
        }
        Table paimonTable = ((PaimonCatalog) catalog).getPaimonTable(tablePath);
        if (!((FileStoreTable) paimonTable).branchManager().branchExists(branch)) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.BRANCH_NOT_EXISTS,
                    String.format(
                            "Specified branch '%s' of table '%s' does not exist.",
                            branch, tablePath));
        }
        if (this.schemaSaveMode == SchemaSaveMode.RECREATE_SCHEMA) {
            throw unsupportedBranchSaveMode(
                    "schema_save_mode=RECREATE_SCHEMA would drop and recreate the main table.");
        }
        if (this.dataSaveMode == DataSaveMode.DROP_DATA) {
            throw unsupportedBranchSaveMode(
                    "data_save_mode=DROP_DATA would truncate the main table.");
        }
    }

    private PaimonConnectorException unsupportedBranchSaveMode(String reason) {
        return new PaimonConnectorException(
                PaimonConnectorErrorCode.UNSUPPORTED_BRANCH_SAVE_MODE,
                String.format(
                        "Paimon branch '%s' does not support this save mode for table '%s'. %s",
                        branch, tablePath, reason));
    }
}
