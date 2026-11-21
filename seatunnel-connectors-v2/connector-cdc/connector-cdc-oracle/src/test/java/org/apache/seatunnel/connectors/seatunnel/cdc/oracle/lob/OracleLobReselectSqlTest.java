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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleLobReselectSqlTest {

    @Test
    public void buildsFlashbackQueryAndQuotesIdentifiers() {
        Assertions.assertEquals(
                "SELECT \"VAL_CLOB\", \"A\"\"B\" FROM (SELECT * FROM \"DEBEZIUM\".\"LOB_TYPES\""
                        + " AS OF SCN ?) WHERE \"ID\"=? AND \"CODE\"=?",
                OracleLobReselectSql.buildQuery(
                        "DEBEZIUM",
                        "LOB_TYPES",
                        Arrays.asList("VAL_CLOB", "A\"B"),
                        Arrays.asList("ID", "CODE"),
                        "12345"));
    }

    @Test
    public void quotesXmlTypeColumns() {
        Assertions.assertEquals(
                "SELECT \"VAL_XML\" FROM \"DEBEZIUM\".\"XML_TYPES\" WHERE \"ID\"=?",
                OracleLobReselectSql.buildQuery(
                        "DEBEZIUM",
                        "XML_TYPES",
                        Collections.singletonList("VAL_XML"),
                        Collections.singletonList("ID"),
                        null));
    }

    @Test
    public void buildsCurrentRowQueryWithoutScn() {
        Assertions.assertEquals(
                "SELECT \"VAL_BLOB\" FROM \"LOB_TYPES\" WHERE \"ID\"=?",
                OracleLobReselectSql.buildQuery(
                        null,
                        "LOB_TYPES",
                        Collections.singletonList("VAL_BLOB"),
                        Collections.singletonList("ID"),
                        "0"));
        Assertions.assertNull(OracleLobReselectSql.usableCommitScn("0"));
        Assertions.assertNull(OracleLobReselectSql.usableCommitScn("12a"));
        Assertions.assertNull(OracleLobReselectSql.usableCommitScn(null));
        Assertions.assertEquals("99", OracleLobReselectSql.usableCommitScn(" 99 "));
    }

    @Test
    public void fallsBackToCurrentRowOnlyForAgedFlashbackErrors() {
        Assertions.assertTrue(
                OracleLobReselectSql.shouldFallbackToCurrentRow(
                        new SQLException("ORA-01555: snapshot too old", "72000", 1555)));
        Assertions.assertTrue(
                OracleLobReselectSql.shouldFallbackToCurrentRow(
                        new SQLException("wrapped", new SQLException("ORA-01466: changed"))));
        SQLException next = new SQLException("ORA-01466: table definition has changed");
        SQLException head = new SQLException("query failed");
        head.setNextException(next);
        Assertions.assertTrue(OracleLobReselectSql.shouldFallbackToCurrentRow(head));
        Assertions.assertFalse(
                OracleLobReselectSql.shouldFallbackToCurrentRow(
                        new SQLException("ORA-08181: invalid SCN", "72000", 8181)));
        Assertions.assertFalse(
                OracleLobReselectSql.shouldFallbackToCurrentRow(
                        new SQLException("ORA-01031: insufficient privileges", "42000", 1031)));
        Assertions.assertFalse(
                OracleLobReselectSql.shouldFallbackToCurrentRow(
                        new SQLException("ORA-00942: table or view does not exist", "42000", 942)));
    }

    @Test
    public void jdbcQueryFallsBackToTheCurrentRowAfterOra01555() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement flashback = mock(PreparedStatement.class);
        PreparedStatement current = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString()))
                .thenAnswer(
                        invocation -> {
                            String sql = invocation.getArgument(0);
                            return sql.contains("AS OF SCN") ? flashback : current;
                        });
        when(flashback.executeQuery())
                .thenThrow(new SQLException("ORA-01555: snapshot too old", "72000", 1555));
        when(current.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1)).thenReturn("real-clob");

        OracleLobColumnReselector.ReselectResult result =
                JdbcOracleLobColumnReselector.query(
                        connection,
                        "DEBEZIUM",
                        "LOB_TYPES",
                        Collections.singletonList("VAL_CLOB"),
                        Collections.singletonList("ID"),
                        Collections.singletonList(11),
                        "12345");

        Assertions.assertTrue(result.isFound());
        Assertions.assertEquals("real-clob", result.getValues().get("VAL_CLOB"));
        verify(flashback).setLong(1, 12345L);
        verify(flashback).setObject(2, 11);
        verify(current).setObject(1, 11);
    }

    @Test
    public void jdbcQueryDoesNotFallBackToTheCurrentRowAfterOra01031() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement flashback = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(flashback);
        SQLException missingPrivilege =
                new SQLException("ORA-01031: insufficient privileges", "42000", 1031);
        when(flashback.executeQuery()).thenThrow(missingPrivilege);

        SQLException thrown =
                Assertions.assertThrows(
                        SQLException.class,
                        () ->
                                JdbcOracleLobColumnReselector.query(
                                        connection,
                                        "DEBEZIUM",
                                        "LOB_TYPES",
                                        Collections.singletonList("VAL_CLOB"),
                                        Collections.singletonList("ID"),
                                        Collections.singletonList(11),
                                        "12345"));
        Assertions.assertEquals(missingPrivilege, thrown);
        verify(connection, times(1)).prepareStatement(anyString());
    }

    @Test
    public void retriesFlashbackOnTheNextEventAfterOra01555() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement flashback = mock(PreparedStatement.class);
        PreparedStatement current = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString()))
                .thenAnswer(
                        invocation -> {
                            String sql = invocation.getArgument(0);
                            return sql.contains("AS OF SCN") ? flashback : current;
                        });
        when(flashback.executeQuery())
                .thenThrow(new SQLException("ORA-01555: snapshot too old", "72000", 1555));
        when(current.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1)).thenReturn("current-clob");

        for (String commitScn : Arrays.asList("12345", "67890")) {
            OracleLobColumnReselector.ReselectResult result =
                    JdbcOracleLobColumnReselector.query(
                            connection,
                            "DEBEZIUM",
                            "LOB_TYPES",
                            Collections.singletonList("VAL_CLOB"),
                            Collections.singletonList("ID"),
                            Collections.singletonList(11),
                            commitScn);
            Assertions.assertTrue(result.isFound());
            Assertions.assertEquals("current-clob", result.getValues().get("VAL_CLOB"));
        }

        verify(flashback, times(2)).executeQuery();
        verify(current, times(2)).executeQuery();
        verify(flashback).setLong(1, 12345L);
        verify(flashback).setLong(1, 67890L);
    }

    @Test
    public void reusesPreparedStatementForTheSameColumnSet() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.isClosed()).thenReturn(false);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1)).thenReturn("cached");
        Map<String, PreparedStatement> cache = new HashMap<String, PreparedStatement>();

        JdbcOracleLobColumnReselector.query(
                connection,
                cache,
                "DEBEZIUM",
                "LOB_TYPES",
                Collections.singletonList("VAL_CLOB"),
                Collections.singletonList("ID"),
                Collections.singletonList(11),
                "12345");
        JdbcOracleLobColumnReselector.query(
                connection,
                cache,
                "DEBEZIUM",
                "LOB_TYPES",
                Collections.singletonList("VAL_CLOB"),
                Collections.singletonList("ID"),
                Collections.singletonList(12),
                "67890");

        Assertions.assertEquals(1, cache.size());
        verify(connection, times(1)).prepareStatement(anyString());
        verify(statement, times(1)).clearParameters();
        verify(statement).setLong(1, 12345L);
        verify(statement).setLong(1, 67890L);
        verify(statement).setObject(2, 11);
        verify(statement).setObject(2, 12);
    }
}
