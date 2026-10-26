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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Db2UtilsTest {
    @Test
    public void testSplitScanQuery() {
        String splitScanSQL =
                Db2Utils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" >= ? AND NOT (\"id\" = ?) AND \"id\" <= ?",
                splitScanSQL);

        splitScanSQL =
                Db2Utils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        true);
        Assertions.assertEquals("SELECT * FROM \"schema1\".\"table1\"", splitScanSQL);

        splitScanSQL =
                Db2Utils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        true,
                        false);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" <= ? AND NOT (\"id\" = ?)",
                splitScanSQL);

        splitScanSQL =
                Db2Utils.buildSplitScanQuery(
                        TableId.parse("db1.schema1.table1"),
                        new SeaTunnelRowType(
                                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}),
                        false,
                        true);
        Assertions.assertEquals(
                "SELECT * FROM \"schema1\".\"table1\" WHERE \"id\" >= ?", splitScanSQL);
    }

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

        Db2Utils.sampleDataFromColumn(jdbc, TableId.parse("db1.schema1.table1"), "id", 1000);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(stmt).executeQuery(sqlCaptor.capture());
        // The sample must be ordered by the database collation, not re-sorted on the Java
        // side: chunk boundaries are later compared by the database in its own collation.
        Assertions.assertEquals(
                "SELECT \"id\" FROM \"schema1\".\"table1\" ORDER BY \"id\"", sqlCaptor.getValue());
    }
}
