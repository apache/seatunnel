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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.sqlserver.SourceInfo;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlServerUtilsTest {

    @Test
    public void testSampleDataFromColumnOrdersByDatabaseCollation() throws Exception {
        Connection connection = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        JdbcConnection jdbc = mock(JdbcConnection.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        SqlServerUtils.sampleDataFromColumn(jdbc, TableId.parse("db1.schema1.table1"), "id", 1000);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        // The sample must be ordered by the database collation, not re-sorted on the Java
        // side: chunk boundaries are later compared by the database in its own collation.
        Assertions.assertEquals(
                "SELECT [id] FROM [db1].[schema1].[table1] ORDER BY [id]", sqlCaptor.getValue());
    }

    @Test
    public void testSplitScanQuery() {
        String splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM [db1].[schema1].[table1] WHERE [id] >= ? AND NOT ([id] = ?) AND [id] <= ?",
                splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM [db1].[schema1].[table1]", splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM [db1].[schema1].[table1] WHERE [id] <= ? AND NOT ([id] = ?)",
                splitScanSQL);

        splitScanSQL =
                SqlServerUtils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM [db1].[schema1].[table1] WHERE [id] >= ?", splitScanSQL);
    }

    @Test
    public void testLsnStringToOffset() {
        String lsnString = "00000027:00000a80:0003";
        LsnOffset offset = SqlServerUtils.lsnStringToOffset(lsnString);
        Assertions.assertEquals(lsnString, offset.getCommitLsn().toString());

        String invalidLsn = "invalid_lsn";
        Assertions.assertThrows(
                RuntimeException.class, () -> SqlServerUtils.lsnStringToOffset(invalidLsn));
    }

    @Test
    public void testGetLsnPositionPreservesCompleteSqlServerOffset() {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put(SourceInfo.COMMIT_LSN_KEY, "00000027:00000a80:0003");
        sourceOffset.put(SourceInfo.CHANGE_LSN_KEY, "00000027:00000a80:0004");
        sourceOffset.put(SourceInfo.EVENT_SERIAL_NO_KEY, 2L);

        LsnOffset offset = SqlServerUtils.getLsnPosition(sourceOffset);

        Assertions.assertEquals("00000027:00000a80:0003", offset.getCommitLsn().toString());
        Assertions.assertEquals("00000027:00000a80:0004", offset.getChangeLsn().toString());
        Assertions.assertEquals("2", offset.getEventSerialNo());

        sourceOffset.put(SourceInfo.EVENT_SERIAL_NO_KEY, 3L);
        Assertions.assertTrue(SqlServerUtils.getLsnPosition(sourceOffset).isAfter(offset));
    }
}
