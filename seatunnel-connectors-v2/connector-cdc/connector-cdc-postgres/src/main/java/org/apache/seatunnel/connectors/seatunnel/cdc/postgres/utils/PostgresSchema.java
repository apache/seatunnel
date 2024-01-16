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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresSchema {

    private final PostgresConnectorConfig connectorConfig;
    private final Map<TableId, TableChanges.TableChange> schemasByTableId;
    private final Map<TableId, CatalogTable> tableMap;

    public PostgresSchema(
            final PostgresConnectorConfig connectorConfig, Map<TableId, CatalogTable> tableMap) {
        this.schemasByTableId = new ConcurrentHashMap<>();
        this.connectorConfig = connectorConfig;
        this.tableMap = tableMap;
    }

    public TableChanges.TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChanges.TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    private TableChanges.TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) {

        CatalogTable catalogTable = tableMap.get(tableId);
        // Because the catalog is null in the postgresConnection.readSchema method
        tableId = new TableId(null, tableId.schema(), tableId.table());

        PostgresConnection postgresConnection = (PostgresConnection) jdbc;
        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
        Tables tables = new Tables();
        try {
            postgresConnection.readSchema(
                    tables,
                    tableId.catalog(),
                    tableId.schema(),
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
            Table table =
                    CatalogTableUtils.mergeCatalogTableConfig(
                            tables.forTable(tableId), catalogTable);
            TableChanges.TableChange tableChange =
                    new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
            tableChangeMap.put(tableId, tableChange);
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    String.format("Failed to read schema for table %s ", tableId), e);
        }

        if (!tableChangeMap.containsKey(tableId)) {
            throw new SeaTunnelException(
                    String.format("Can't obtain schema for table %s ", tableId));
        }

        return tableChangeMap.get(tableId);
    }
}
