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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.AllArgsConstructor;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;

@AllArgsConstructor
public class DefaultSaveModeHandler implements SaveModeHandler {

    public SchemaSaveMode schemaSaveMode;
    public DataSaveMode dataSaveMode;
    public Catalog catalog;
    public TablePath tablePath;
    public CatalogTable catalogTable;
    public String customSql;

    public DefaultSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql) {
        this(
                schemaSaveMode,
                dataSaveMode,
                catalog,
                catalogTable.getTableId().toTablePath(),
                catalogTable,
                customSql);
    }

    @Override
    public void handleSchemaSaveMode() {
        switch (schemaSaveMode) {
            case RECREATE_SCHEMA:
                recreateSchema();
                break;
            case CREATE_SCHEMA_WHEN_NOT_EXIST:
                createSchemaWhenNotExist();
                break;
            case ERROR_WHEN_SCHEMA_NOT_EXIST:
                errorWhenSchemaNotExist();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + schemaSaveMode);
        }
    }

    @Override
    public void handleDataSaveMode() {
        switch (dataSaveMode) {
            case DROP_DATA:
                keepSchemaDropData();
                break;
            case APPEND_DATA:
                keepSchemaAndData();
                break;
            case CUSTOM_PROCESSING:
                customProcessing();
                break;
            case ERROR_WHEN_DATA_EXISTS:
                errorWhenDataExists();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + dataSaveMode);
        }
    }

    protected void recreateSchema() {
        if (tableExists()) {
            dropTable();
        }
        createTable();
    }

    protected void createSchemaWhenNotExist() {
        if (!tableExists()) {
            createTable();
        }
    }

    protected void errorWhenSchemaNotExist() {
        if (!tableExists()) {
            throw new SeaTunnelRuntimeException(SINK_TABLE_NOT_EXIST, "The sink table not exist");
        }
    }

    protected void keepSchemaDropData() {
        if (tableExists()) {
            truncateTable();
        }
    }

    protected void keepSchemaAndData() {}

    protected void customProcessing() {
        executeCustomSql();
    }

    protected void errorWhenDataExists() {
        if (dataExists()) {
            throw new SeaTunnelRuntimeException(
                    SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
        }
    }

    protected boolean tableExists() {
        return catalog.tableExists(tablePath);
    }

    protected void dropTable() {
        catalog.dropTable(tablePath, true);
    }

    protected void createTable() {
        catalog.createTable(tablePath, catalogTable, true);
    }

    protected void truncateTable() {
        catalog.truncateTable(tablePath, true);
    }

    protected boolean dataExists() {
        return catalog.isExistsData(tablePath);
    }

    protected void executeCustomSql() {
        catalog.executeSql(tablePath, customSql);
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
