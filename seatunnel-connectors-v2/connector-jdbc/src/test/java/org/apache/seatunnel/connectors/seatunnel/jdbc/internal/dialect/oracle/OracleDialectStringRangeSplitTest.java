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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.StringRangeSplitDecision;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies Oracle range splitting requires binary session ordering and fixed ASCII keys. */
public class OracleDialectStringRangeSplitTest {

    @Test
    public void testAcceptsBinaryNlsAndFixedAsciiSamples() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        Statement sampleStatement = mock(Statement.class);
        ResultSet nlsResultSet = binaryNlsResultSet();
        ResultSet sampleResultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(nlsStatement, sampleStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(nlsResultSet);
        when(sampleStatement.executeQuery(anyString())).thenReturn(sampleResultSet);
        when(sampleResultSet.next()).thenReturn(true, true, false);
        when(sampleResultSet.getString(1)).thenReturn("A000", "A001");

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 2);

        assertTrue(decision.isSafe());
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(sampleStatement).executeQuery(sqlCaptor.capture());
        assertTrue(sqlCaptor.getValue().contains("ROWNUM <= 2"));
        assertTrue(sqlCaptor.getValue().contains("ORDER BY \"ORDER_ID\" ASC"));
    }

    @Test
    public void testRejectsNonBinaryComparisonSession() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        ResultSet nlsResultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(nlsStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(nlsResultSet);
        when(nlsResultSet.next()).thenReturn(true, true, false);
        when(nlsResultSet.getString(1)).thenReturn("NLS_SORT", "NLS_COMP");
        when(nlsResultSet.getString(2)).thenReturn("BINARY", "LINGUISTIC");

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 2);

        assertFalse(decision.isSafe());
    }

    @Test
    public void testRejectsVariableLengthSamples() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        Statement sampleStatement = mock(Statement.class);
        ResultSet sampleResultSet = mock(ResultSet.class);
        ResultSet nlsResultSet = binaryNlsResultSet();
        when(connection.createStatement()).thenReturn(nlsStatement, sampleStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(nlsResultSet);
        when(sampleStatement.executeQuery(anyString())).thenReturn(sampleResultSet);
        when(sampleResultSet.next()).thenReturn(true, true, false);
        when(sampleResultSet.getString(1)).thenReturn("A000", "A0010");

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 2);

        assertFalse(decision.isSafe());
    }

    private ResultSet binaryNlsResultSet() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString(1)).thenReturn("NLS_SORT", "NLS_COMP");
        when(resultSet.getString(2)).thenReturn("BINARY", "BINARY");
        return resultSet;
    }

    private JdbcSourceTable physicalTable() {
        return JdbcSourceTable.builder().tablePath(TablePath.of("APP", "ORDERS")).build();
    }
}
