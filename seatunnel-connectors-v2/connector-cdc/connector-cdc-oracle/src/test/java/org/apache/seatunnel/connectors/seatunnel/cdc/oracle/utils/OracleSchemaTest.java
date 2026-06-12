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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OracleSchemaTest {

    @Test
    public void getTableSchemaShouldUseRequestedCatalogWhenReadSchemaReturnsCataloglessTableId()
            throws SQLException {
        TableId requestedTableId = new TableId("ORCLPDB", "HCDRS_LIB_B", "T_B1");
        TableId readTableId = new TableId(null, "HCDRS_LIB_B", "T_B1");
        CatalogTable catalogTable = catalogTable(requestedTableId);
        Map<TableId, CatalogTable> tableMap =
                Collections.singletonMap(requestedTableId, catalogTable);

        OracleSchema schema = new OracleSchema(oracleConnectorConfig(), tableMap);

        TableChanges.TableChange tableChange =
                schema.getTableSchema(
                        oracleConnection(debeziumTable(readTableId)), requestedTableId);

        Assertions.assertEquals(TableChanges.TableChangeType.CREATE, tableChange.getType());
        Assertions.assertEquals(
                Collections.singletonList("ID"), tableChange.getTable().primaryKeyColumnNames());
    }

    @Test
    public void getTableSchemaShouldUseRequestedTableWhenReadSchemaReturnsDifferentCatalogAndCase()
            throws SQLException {
        TableId requestedTableId = new TableId("ORCLPDB", "LZ", "T_1");
        TableId readTableId = new TableId("ORCL", "lz", "t_1");
        CatalogTable catalogTable = catalogTable(requestedTableId);
        Map<TableId, CatalogTable> tableMap =
                Collections.singletonMap(requestedTableId, catalogTable);

        OracleSchema schema = new OracleSchema(oracleConnectorConfig(), tableMap);

        TableChanges.TableChange tableChange =
                schema.getTableSchema(
                        oracleConnection(debeziumTable(readTableId)), requestedTableId);

        Assertions.assertEquals(TableChanges.TableChangeType.CREATE, tableChange.getType());
        Assertions.assertEquals(
                Collections.singletonList("ID"), tableChange.getTable().primaryKeyColumnNames());
    }

    @Test
    public void getTableSchemaShouldIncludeEquivalentReadIdWhenConnectorFilterRejectsIt()
            throws SQLException {
        TableId requestedTableId = new TableId("ORCLPDB", "HCDRS_LIB_B", "T_B_SKIP");
        TableId readTableId = new TableId("ORCL", "hcdrs_lib_b", "t_b_skip");
        CatalogTable catalogTable = catalogTable(requestedTableId);
        Map<TableId, CatalogTable> tableMap =
                Collections.singletonMap(requestedTableId, catalogTable);

        OracleSchema schema =
                new OracleSchema(
                        oracleConnectorConfig(tableId -> requestedTableId.equals(tableId)),
                        tableMap);

        TableChanges.TableChange tableChange =
                schema.getTableSchema(
                        oracleConnectionApplyingFilter(debeziumTable(readTableId)),
                        requestedTableId);

        Assertions.assertEquals(TableChanges.TableChangeType.CREATE, tableChange.getType());
        Assertions.assertEquals(
                Collections.singletonList("ID"), tableChange.getTable().primaryKeyColumnNames());
    }

    @Test
    public void getTableSchemaShouldSetPdbSessionBeforeReadingSchema() throws SQLException {
        TableId requestedTableId = new TableId("ORCLPDB", "HCDRS_LIB_A", "T_A2");
        CatalogTable catalogTable = catalogTable(requestedTableId);
        Map<TableId, CatalogTable> tableMap =
                Collections.singletonMap(requestedTableId, catalogTable);

        OracleSchema schema =
                new OracleSchema(oracleConnectorConfig("ORCLPDB", tableId -> true), tableMap);
        OracleConnection oracleConnection = oracleConnection(debeziumTable(requestedTableId));

        schema.getTableSchema(oracleConnection, requestedTableId);

        InOrder inOrder = inOrder(oracleConnection);
        inOrder.verify(oracleConnection).setSessionToPdb("ORCLPDB");
        inOrder.verify(oracleConnection)
                .readSchema(
                        any(Tables.class),
                        eq("ORCLPDB"),
                        eq("HCDRS_LIB_A"),
                        any(Tables.TableFilter.class),
                        isNull(),
                        eq(false));
    }

    private static CatalogTable catalogTable(TableId tableId) {
        return CatalogTable.of(
                TableIdentifier.of(null, tableId.catalog(), tableId.schema(), tableId.table()),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("ID")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .build())
                        .primaryKey(PrimaryKey.of("PK_T_B1", Collections.singletonList("ID")))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    private static Table debeziumTable(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
                .addColumn(
                        Column.editor()
                                .name("ID")
                                .jdbcType(Types.INTEGER)
                                .type("NUMBER")
                                .position(1)
                                .optional(false)
                                .create())
                .setPrimaryKeyNames("ID")
                .create();
    }

    private static OracleConnectorConfig oracleConnectorConfig() {
        return oracleConnectorConfig(tableId -> true);
    }

    private static OracleConnectorConfig oracleConnectorConfig(
            Tables.TableFilter dataCollectionFilter) {
        return oracleConnectorConfig(null, dataCollectionFilter);
    }

    private static OracleConnectorConfig oracleConnectorConfig(
            String pdbName, Tables.TableFilter dataCollectionFilter) {
        OracleConnectorConfig connectorConfig = mock(OracleConnectorConfig.class);
        RelationalTableFilters tableFilters = mock(RelationalTableFilters.class);
        when(connectorConfig.getPdbName()).thenReturn(pdbName);
        when(connectorConfig.getTableFilters()).thenReturn(tableFilters);
        when(tableFilters.dataCollectionFilter()).thenReturn(dataCollectionFilter);
        return connectorConfig;
    }

    private static OracleConnection oracleConnection(Table table) throws SQLException {
        OracleConnection oracleConnection = mock(OracleConnection.class);
        doAnswer(
                        invocation -> {
                            Tables tables = invocation.getArgument(0);
                            tables.overwriteTable(table);
                            return null;
                        })
                .when(oracleConnection)
                .readSchema(
                        any(Tables.class),
                        nullable(String.class),
                        nullable(String.class),
                        any(Tables.TableFilter.class),
                        isNull(),
                        eq(false));
        return oracleConnection;
    }

    private static OracleConnection oracleConnectionApplyingFilter(Table table)
            throws SQLException {
        OracleConnection oracleConnection = mock(OracleConnection.class);
        doAnswer(
                        invocation -> {
                            Tables tables = invocation.getArgument(0);
                            Tables.TableFilter tableFilter = invocation.getArgument(3);
                            if (tableFilter.isIncluded(table.id())) {
                                tables.overwriteTable(table);
                            }
                            return null;
                        })
                .when(oracleConnection)
                .readSchema(
                        any(Tables.class),
                        nullable(String.class),
                        nullable(String.class),
                        any(Tables.TableFilter.class),
                        isNull(),
                        eq(false));
        return oracleConnection;
    }
}
