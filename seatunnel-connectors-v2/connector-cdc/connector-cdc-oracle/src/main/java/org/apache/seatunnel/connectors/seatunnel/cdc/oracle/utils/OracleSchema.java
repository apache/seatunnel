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
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
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
            oracleConnection.readSchema(
                    tables,
                    tableId.catalog(),
                    tableId.schema(),
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
            for (TableId id : tables.tableIds()) {
                if (tableMap.containsKey(id)) {
                    Table table =
                            CatalogTableUtils.mergeCatalogTableConfig(
                                    tables.forTable(id), tableMap.get(id));
                    TableChanges.TableChange tableChange =
                            new TableChanges.TableChange(
                                    TableChanges.TableChangeType.CREATE, table);
                    schemasByTableId.put(id, tableChange);
                }
            }
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    String.format("Failed to read schema for table %s ", tableId), e);
        } catch (Exception e) {
            log.error(
                    "mergeCatalogTableConfig for table [{}] from tableMap [{}]",
                    tableId,
                    tableMap,
                    e);
            throw e;
        }

        if (!schemasByTableId.containsKey(tableId)) {
            throw new SeaTunnelException(
                    String.format("Can't obtain schema for table %s ", tableId));
        }

        return schemasByTableId.get(tableId);
    }
}
