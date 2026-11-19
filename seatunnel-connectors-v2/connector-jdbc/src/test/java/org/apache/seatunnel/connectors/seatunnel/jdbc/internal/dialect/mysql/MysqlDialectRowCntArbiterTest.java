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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the zero row estimate arbitration in {@link MysqlDialect}: SHOW TABLE STATUS returns an
 * InnoDB statistics estimate that can stay at 0 after a bulk load while the table actually holds
 * millions of rows; a stale zero collapses concurrent read to a single split. The arbiter re-checks
 * a zero estimate with COUNT(*), which must keep genuinely empty tables at zero and add no extra
 * query for tables with a healthy estimate.
 */
public class MysqlDialectRowCntArbiterTest {

    private MysqlDialect dialect;
    private Connection connection;
    private Statement showStmt;
    private Statement countStmt;
    private ResultSet showRs;
    private ResultSet countRs;

    @BeforeEach
    public void setUp() throws SQLException {
        dialect = new MysqlDialect();
        connection = mock(Connection.class);
        showStmt = mock(Statement.class);
        countStmt = mock(Statement.class);
        showRs = mock(ResultSet.class);
        countRs = mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(showStmt, countStmt);
        when(showStmt.execute(anyString())).thenReturn(true);
        when(showStmt.executeQuery(contains("SHOW TABLE STATUS"))).thenReturn(showRs);
        when(countStmt.executeQuery(contains("COUNT"))).thenReturn(countRs);

        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(18);
        when(showRs.getMetaData()).thenReturn(metaData);
        when(showRs.next()).thenReturn(true);
        when(countRs.next()).thenReturn(true);
    }

    private JdbcSourceTable buildTable() {
        return JdbcSourceTable.builder().tablePath(TablePath.of("testdb", "test_table")).build();
    }

    /** A stale zero estimate on a loaded table must be corrected to the COUNT(*) result. */
    @Test
    public void testZeroEstimateOnLoadedTableCorrectedByCount() throws SQLException {
        when(showRs.getLong(5)).thenReturn(0L);
        when(countRs.getLong(1)).thenReturn(8_992_024L);
        Long rowCnt = dialect.approximateRowCntStatement(connection, buildTable());
        assertEquals(8_992_024L, rowCnt);
        verify(countStmt).executeQuery(contains("COUNT"));
    }

    /** A zero estimate on a genuinely empty table must stay zero without any behavioral change. */
    @Test
    public void testZeroEstimateOnEmptyTableStaysZero() throws SQLException {
        when(showRs.getLong(5)).thenReturn(0L);
        when(countRs.getLong(1)).thenReturn(0L);
        Long rowCnt = dialect.approximateRowCntStatement(connection, buildTable());
        assertEquals(0L, rowCnt);
    }

    /** A healthy non-zero estimate must be returned as-is without issuing COUNT(*). */
    @Test
    public void testNonZeroEstimateSkipsCountArbiter() throws SQLException {
        when(showRs.getLong(5)).thenReturn(8_573_070L);
        Long rowCnt = dialect.approximateRowCntStatement(connection, buildTable());
        assertEquals(8_573_070L, rowCnt);
        verify(countStmt, never()).executeQuery(anyString());
    }
}
