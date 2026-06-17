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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** A component used to get schema by table path. */
public class OracleSchema {

    private final OracleConnectorConfig connectorConfig;
    private final Map<TableId, TableChange> schemasByTableId;
    private final Map<TableId, CatalogTable> tableMap;

    public OracleSchema(
            OracleConnectorConfig connectorConfig, Map<TableId, CatalogTable> tableMap) {
        this.connectorConfig = connectorConfig;
        this.schemasByTableId = new HashMap<>();
        this.tableMap = tableMap;
    }

    /**
     * Gets table schema for the given table path. It will request to MySQL server by running `SHOW
     * CREATE TABLE` if cache missed.
     */
    public TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(jdbc, tableId);
        }
        return schema;
    }

    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) {
        OracleConnection oracleConnection = (OracleConnection) jdbc;
        Tables tables = new Tables();

        try {
            setSessionToPdbIfNeeded(oracleConnection);
            oracleConnection.readSchema(
                    tables,
                    tableId.catalog(),
                    tableId.schema(),
                    getSchemaReadTableFilter(tableId),
                    null,
                    false);
            for (TableId id : tables.tableIds()) {
                TableId tableMapId = getTableMapId(tableId, id);
                if (tableMapId != null) {
                    Table table =
                            CatalogTableUtils.mergeCatalogTableConfig(
                                    tables.forTable(id), tableMap.get(tableMapId));
                    TableChanges.TableChange tableChange =
                            new TableChanges.TableChange(
                                    TableChanges.TableChangeType.CREATE, table);
                    schemasByTableId.put(tableMapId, tableChange);
                }
            }
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    String.format("Failed to read schema for table %s ", tableId), e);
        }

        if (!schemasByTableId.containsKey(tableId)) {
            throw new SeaTunnelException(
                    String.format(
                            "Can't obtain schema for table %s. Read schema table ids: %s. Configured table ids: %s",
                            tableId, tables.tableIds(), tableMap.keySet()));
        }

        return schemasByTableId.get(tableId);
    }

    private void setSessionToPdbIfNeeded(OracleConnection oracleConnection) throws SQLException {
        String pdbName = connectorConfig.getPdbName();
        if (pdbName != null) {
            oracleConnection.setSessionToPdb(pdbName);
        }
    }

    private Tables.TableFilter getSchemaReadTableFilter(TableId requestedTableId) {
        Tables.TableFilter dataCollectionFilter =
                connectorConfig.getTableFilters().dataCollectionFilter();
        if (!tableMap.containsKey(requestedTableId)) {
            return dataCollectionFilter;
        }

        return tableId ->
                dataCollectionFilter.isIncluded(tableId)
                        || hasSameSchemaAndTable(requestedTableId, tableId);
    }

    private TableId getTableMapId(TableId requestedTableId, TableId readTableId) {
        if (tableMap.containsKey(readTableId)) {
            return readTableId;
        }

        if (tableMap.containsKey(requestedTableId)
                && hasSameSchemaAndTable(requestedTableId, readTableId)) {
            return requestedTableId;
        }

        TableId readTableIdWithRequestedCatalog =
                new TableId(requestedTableId.catalog(), readTableId.schema(), readTableId.table());
        if (tableMap.containsKey(readTableIdWithRequestedCatalog)) {
            return readTableIdWithRequestedCatalog;
        }

        return null;
    }

    private boolean hasSameSchemaAndTable(TableId left, TableId right) {
        return equalsIgnoreCase(left.schema(), right.schema())
                && equalsIgnoreCase(left.table(), right.table());
    }

    private boolean equalsIgnoreCase(String left, String right) {
        if (left == null || right == null) {
            return left == right;
        }
        return left.equalsIgnoreCase(right);
    }
}
