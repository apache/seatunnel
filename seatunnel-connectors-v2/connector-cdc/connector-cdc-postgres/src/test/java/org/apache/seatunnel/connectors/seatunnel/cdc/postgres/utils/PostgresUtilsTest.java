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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresUtilsTest {
    @Test
    public void testSplitScanQuery() {
        Table table =
                Table.editor()
                        .tableId(TableId.parse("db1.schema1.table1"))
                        .addColumn(Column.editor().name("id").type("int8").create())
                        .create();
        String splitScanSQL =
                PostgresUtils.buildSplitScanQuery(
                        table,
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitScanSQL);

        splitScanSQL =
                PostgresUtils.buildSplitScanQuery(
                        table,
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM \"schema1\".\"table1\"", splitScanSQL);

        splitScanSQL =
                PostgresUtils.buildSplitScanQuery(
                        table,
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" <= ? AND NOT (\"id\" = ?)",
                splitScanSQL);

        table =
                Table.editor()
                        .tableId(TableId.parse("db1.schema1.table1"))
                        .addColumn(Column.editor().name("id").type("uuid").create())
                        .create();
        splitScanSQL =
                PostgresUtils.buildSplitScanQuery(
                        table,
                        new SeaTunnelRowType(
                                new String[] {"id"},
                                new SeaTunnelDataType[] {BasicType.STRING_TYPE}),
                        false,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\"::text >= ?", splitScanSQL);
    }

    @Test
    public void testInvalidatedSlotReasonOnPostgres17AndLater() throws SQLException {
        Map<String, Object> row = slotRow("reserved");
        row.put("conflicting", false);
        row.put("invalidation_reason", null);
        Assertions.assertEquals(
                Optional.empty(), PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));

        row.put("wal_status", "lost");
        row.put("invalidation_reason", "idle_timeout");
        Assertions.assertEquals(
                Optional.of("idle_timeout"),
                PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));

        row.put("invalidation_reason", "wal_removed");
        Assertions.assertEquals(
                Optional.of("wal_removed"),
                PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));
    }

    @Test
    public void testInvalidatedSlotReasonBeforePostgres17() throws SQLException {
        // PostgreSQL 16 has wal_status and conflicting but no invalidation_reason
        Map<String, Object> row = slotRow("reserved");
        row.put("conflicting", false);
        Assertions.assertEquals(
                Optional.empty(), PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));

        row.put("conflicting", true);
        Assertions.assertEquals(
                Optional.of("conflict with recovery"),
                PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));

        // PostgreSQL 13 to 15 only have wal_status
        row = slotRow("unreserved");
        Assertions.assertEquals(
                Optional.empty(), PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));

        row.put("wal_status", "lost");
        Assertions.assertEquals(
                Optional.of("required WAL was removed, wal_status is lost"),
                PostgresUtils.readSlotInvalidationReason(slotResultSet(row)));
    }

    @Test
    public void testSlotWithoutInvalidationColumnsIsNeverInvalidated() throws SQLException {
        // PostgreSQL 12 and earlier have none of the invalidation columns
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("slot_name", "seatunnel");
        row.put("active", false);
        ResultSet resultSet = slotResultSet(row);

        Assertions.assertEquals(
                Optional.empty(), PostgresUtils.readSlotInvalidationReason(resultSet));
        verify(resultSet, never()).getString(anyString());
        verify(resultSet, never()).getBoolean(anyString());

        ResultSet noSlot = mock(ResultSet.class);
        when(noSlot.next()).thenReturn(false);
        Assertions.assertEquals(Optional.empty(), PostgresUtils.readSlotInvalidationReason(noSlot));
    }

    @Test
    public void testGetReplicationSlotInvalidationReason() throws SQLException {
        Map<String, Object> row = slotRow("lost");
        row.put("invalidation_reason", "idle_timeout");
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet invalidated = slotResultSet(row);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(invalidated);

        Assertions.assertEquals(
                Optional.of("idle_timeout"),
                PostgresUtils.getReplicationSlotInvalidationReason(connection, "st_idle"));
        verify(statement).setString(1, "st_idle");

        row = slotRow("reserved");
        row.put("invalidation_reason", null);
        ResultSet healthy = slotResultSet(row);
        when(statement.executeQuery()).thenReturn(healthy);
        Assertions.assertEquals(
                Optional.empty(),
                PostgresUtils.getReplicationSlotInvalidationReason(connection, "st_idle"));
    }

    @Test
    public void testReplicationSlotInvalidatedMessage() {
        SeaTunnelRuntimeException exception =
                PostgresUtils.replicationSlotInvalidated("st_idle", "idle_timeout");

        Assertions.assertTrue(exception.getMessage().contains("POSTGRES-04"));
        Assertions.assertTrue(exception.getMessage().contains("'st_idle' has been invalidated"));
        Assertions.assertTrue(exception.getMessage().contains("reason: idle_timeout"));
        Assertions.assertTrue(
                exception.getMessage().contains("pg_drop_replication_slot('st_idle')"));
    }

    private static Map<String, Object> slotRow(String walStatus) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("slot_name", "seatunnel");
        row.put("active", false);
        row.put("wal_status", walStatus);
        return row;
    }

    private static ResultSet slotResultSet(Map<String, Object> row) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(row.size());
        int index = 1;
        for (Map.Entry<String, Object> column : row.entrySet()) {
            when(metaData.getColumnName(index++)).thenReturn(column.getKey());
            Object value = column.getValue();
            when(resultSet.getString(column.getKey()))
                    .thenReturn(value == null ? null : value.toString());
            when(resultSet.getBoolean(column.getKey())).thenReturn(Boolean.TRUE.equals(value));
        }
        return resultSet;
    }
}
