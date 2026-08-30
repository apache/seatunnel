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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
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
        stubSafeOracleRangeMetadata(connection);

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
        stubSafeOracleRangeMetadata(connection);

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 2);

        assertFalse(decision.isSafe());
    }

    @Test
    public void testRejectsDatabaseEncodingThatDoesNotPreserveAsciiBinaryOrdering()
            throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        PreparedStatement encodingStatement = mock(PreparedStatement.class);
        ResultSet encodingResultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(nlsStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(binaryNlsResultSet());
        when(connection.prepareStatement(anyString())).thenReturn(encodingStatement);
        when(encodingStatement.executeQuery()).thenReturn(encodingResultSet);
        when(encodingResultSet.next()).thenReturn(true);
        when(encodingResultSet.getInt(anyInt())).thenReturn(193);

        StringRangeSplitDecision decision =
                new OracleDialect().validateStringRangeSplitSession(connection);

        assertFalse(decision.isSafe());
        assertTrue(decision.getReason().contains("does not preserve printable ASCII"));
    }

    @Test
    public void testRedactsNonAsciiSampleValue() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        Statement sampleStatement = mock(Statement.class);
        ResultSet sampleResultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(nlsStatement, sampleStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(binaryNlsResultSet());
        when(sampleStatement.executeQuery(anyString())).thenReturn(sampleResultSet);
        when(sampleResultSet.next()).thenReturn(true, false);
        when(sampleResultSet.getString(1)).thenReturn("AB\u4e2d");
        stubSafeOracleRangeMetadata(connection);

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 1);

        assertFalse(decision.isSafe());
        assertFalse(decision.getReason().contains("AB\u4e2d"));
        assertTrue(decision.getReason().contains("length 3"));
    }

    @Test
    public void testRejectsMissingPhysicalTablePath() throws SQLException {
        JdbcSourceTable table = JdbcSourceTable.builder().tablePath(TablePath.DEFAULT).build();

        StringRangeSplitDecision decision =
                new OracleDialect().validateStringRangeSplit(null, table, "ORDER_ID", 1);

        assertFalse(decision.isSafe());
    }

    @Test
    public void testRejectsNonBinaryDataBoundColumnCollation() throws SQLException {
        Connection connection = mock(Connection.class);
        Statement nlsStatement = mock(Statement.class);
        PreparedStatement collationStatement = mock(PreparedStatement.class);
        ResultSet collationResultSet = mock(ResultSet.class);
        PreparedStatement encodingStatement = mock(PreparedStatement.class);
        ResultSet encodingResultSet = mock(ResultSet.class);
        when(connection.createStatement()).thenReturn(nlsStatement);
        when(nlsStatement.executeQuery(anyString())).thenReturn(binaryNlsResultSet());
        when(connection.prepareStatement(anyString()))
                .thenReturn(encodingStatement, collationStatement);
        stubAsciiCompatibleEncoding(encodingStatement, encodingResultSet);
        when(collationStatement.executeQuery()).thenReturn(collationResultSet);
        when(collationResultSet.next()).thenReturn(true);
        when(collationResultSet.getString(1)).thenReturn("BINARY_CI");

        StringRangeSplitDecision decision =
                new OracleDialect()
                        .validateStringRangeSplit(connection, physicalTable(), "ORDER_ID", 1);

        assertFalse(decision.isSafe());
        assertTrue(decision.getReason().contains("column collation BINARY_CI is not binary"));
    }

    @Test
    public void testDerivedOracleDialectDoesNotAdvertiseRangeSplitSupport() throws SQLException {
        OracleDialect dialect = new DerivedOracleDialect();

        assertFalse(dialect.supportStringRangeSplit());
        assertFalse(dialect.validateStringRangeSplitSession(null).isSafe());
    }

    private void stubSafeOracleRangeMetadata(Connection connection) throws SQLException {
        PreparedStatement collationStatement = mock(PreparedStatement.class);
        ResultSet collationResultSet = mock(ResultSet.class);
        PreparedStatement encodingStatement = mock(PreparedStatement.class);
        ResultSet encodingResultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString()))
                .thenReturn(encodingStatement, collationStatement);
        stubAsciiCompatibleEncoding(encodingStatement, encodingResultSet);
        when(collationStatement.executeQuery()).thenReturn(collationResultSet);
        when(collationResultSet.next()).thenReturn(true);
        when(collationResultSet.getString(1)).thenReturn("BINARY");
    }

    private void stubAsciiCompatibleEncoding(
            PreparedStatement encodingStatement, ResultSet encodingResultSet) throws SQLException {
        when(encodingStatement.executeQuery()).thenReturn(encodingResultSet);
        when(encodingResultSet.next()).thenReturn(true);
        when(encodingResultSet.getInt(anyInt()))
                .thenAnswer(invocation -> ((Integer) invocation.getArgument(0)) + 31);
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

    private static final class DerivedOracleDialect extends OracleDialect {}
}
