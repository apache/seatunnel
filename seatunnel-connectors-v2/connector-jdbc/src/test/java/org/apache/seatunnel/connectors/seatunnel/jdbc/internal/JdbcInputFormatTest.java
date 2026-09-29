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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcInputFormatTest {

    private static final TablePath TABLE_PATH = TablePath.of("test", "public", "source_table");
    private static final TableSchema TABLE_SCHEMA = TableSchema.builder().build();
    private static final JdbcSourceSplit SPLIT =
            new JdbcSourceSplit(TABLE_PATH, "split-0", null, null, null, null, null);

    @Test
    void shouldRollbackAndRestoreConfiguredAutoCommitAfterClosingSplit() throws Exception {
        TestContext context = createContext(true);
        context.openEmptySplit();
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);

        context.inputFormat.close();

        InOrder inOrder = inOrder(context.statement, context.resultSet, context.connection);
        inOrder.verify(context.statement).getConnection();
        inOrder.verify(context.resultSet).close();
        inOrder.verify(context.statement).close();
        inOrder.verify(context.connection).rollback();
        inOrder.verify(context.connection).setAutoCommit(true);

        context.inputFormat.close();
        verify(context.connection, times(1)).rollback();
    }

    @Test
    void shouldRollbackAndKeepConfiguredManualCommit() throws Exception {
        TestContext context = createContext(false);
        context.openEmptySplit();
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);

        context.inputFormat.close();

        verify(context.connection).rollback();
        verify(context.connection, never()).setAutoCommit(true);
    }

    @Test
    void shouldNotRollbackAutoCommitConnection() throws Exception {
        TestContext context = createContext(true);
        context.openEmptySplit();
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(true);

        context.inputFormat.close();

        verify(context.connection, never()).rollback();
        verify(context.connection, never()).setAutoCommit(true);
    }

    @Test
    void shouldFinishTransactionWhenResourceCloseFails() throws Exception {
        TestContext context = createContext(true);
        context.openEmptySplit();
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLException("result set close failed")).when(context.resultSet).close();
        doThrow(new SQLException("statement close failed")).when(context.statement).close();

        context.inputFormat.close();

        verify(context.connection).rollback();
        verify(context.connection).setAutoCommit(true);
    }

    @Test
    void shouldKeepConnectionReusableAcrossSuccessfulSplits() throws Exception {
        TestContext context = createContext(true);
        PreparedStatement secondStatement = mock(PreparedStatement.class);
        ResultSet secondResultSet = mock(ResultSet.class);

        when(context.chunkSplitter.generateSplitStatement(SPLIT, TABLE_SCHEMA))
                .thenReturn(context.statement, secondStatement);
        when(context.statement.executeQuery()).thenReturn(context.resultSet);
        when(secondStatement.executeQuery()).thenReturn(secondResultSet);
        when(context.resultSet.next()).thenReturn(false);
        when(secondResultSet.next()).thenReturn(false);
        when(context.statement.getConnection()).thenReturn(context.connection);
        when(secondStatement.getConnection()).thenReturn(context.connection);
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);

        context.inputFormat.open(SPLIT);
        context.inputFormat.close();
        context.inputFormat.open(SPLIT);
        context.inputFormat.close();

        verify(context.chunkSplitter, times(2)).generateSplitStatement(SPLIT, TABLE_SCHEMA);
        verify(context.connection, times(2)).rollback();
        verify(context.connection, times(2)).setAutoCommit(true);
        verify(context.connection, never()).close();
        verify(context.chunkSplitter, never()).close();
    }

    @Test
    void shouldDiscardConnectionWhenTransactionCleanupFails() throws Exception {
        TestContext context = createContext(true);
        context.openEmptySplit();
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLException("rollback failed")).when(context.connection).rollback();

        context.inputFormat.close();

        verify(context.connection).close();
        verify(context.chunkSplitter).close();
        verify(context.connection, never()).setAutoCommit(true);
    }

    @Test
    void shouldCloseCachedConnectionWhenStatementCannotExposeConnection() throws Exception {
        TestContext context = createContext(true);
        context.openEmptySplit();
        when(context.statement.getConnection())
                .thenThrow(new SQLException("get connection failed"));

        context.inputFormat.close();

        verify(context.chunkSplitter).close();
        verify(context.resultSet).close();
        verify(context.statement).close();
    }

    @Test
    void shouldDiscardCachedConnectionWhenStatementCreationFails() throws Exception {
        TestContext context = createContext(true);
        when(context.chunkSplitter.generateSplitStatement(SPLIT, TABLE_SCHEMA))
                .thenThrow(new SQLException("prepare failed"));

        assertThrows(JdbcConnectorException.class, () -> context.inputFormat.open(SPLIT));

        verify(context.chunkSplitter).close();
        verify(context.statement, never()).executeQuery();
    }

    @Test
    void shouldRollbackAndKeepConnectionWhenExecuteQueryFails() throws Exception {
        TestContext context = createContext(true);
        when(context.chunkSplitter.generateSplitStatement(SPLIT, TABLE_SCHEMA))
                .thenReturn(context.statement);
        when(context.statement.getConnection()).thenReturn(context.connection);
        when(context.statement.executeQuery()).thenThrow(new SQLException("execute failed"));
        when(context.connection.isClosed()).thenReturn(false);
        when(context.connection.getAutoCommit()).thenReturn(false);

        assertThrows(JdbcConnectorException.class, () -> context.inputFormat.open(SPLIT));

        verify(context.statement).close();
        verify(context.connection).rollback();
        verify(context.connection).setAutoCommit(true);
        verify(context.connection, never()).close();
        verify(context.chunkSplitter, never()).close();
    }

    @Test
    void shouldAlwaysCloseCachedConnectionWhenInputFormatCloses() throws Exception {
        TestContext context = createContext(true);

        context.inputFormat.closeInputFormat();

        verify(context.chunkSplitter).close();
    }

    @Test
    void shouldIgnoreClosedConnection() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(true);

        JdbcInputFormat.finishReadTransaction(connection, true);

        verify(connection, never()).getAutoCommit();
        verify(connection, never()).rollback();
    }

    private static TestContext createContext(boolean configuredAutoCommit) throws SQLException {
        JdbcDialect dialect = mock(JdbcDialect.class);
        JdbcRowConverter rowConverter = mock(JdbcRowConverter.class);
        ChunkSplitter chunkSplitter = mock(ChunkSplitter.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        Connection connection = mock(Connection.class);
        CatalogTable catalogTable = mock(CatalogTable.class);
        Map<TablePath, CatalogTable> tables = Collections.singletonMap(TABLE_PATH, catalogTable);

        when(dialect.getRowConverter()).thenReturn(rowConverter);
        when(catalogTable.getTableSchema()).thenReturn(TABLE_SCHEMA);

        JdbcInputFormat inputFormat =
                new JdbcInputFormat(dialect, chunkSplitter, tables, configuredAutoCommit);
        return new TestContext(inputFormat, chunkSplitter, statement, resultSet, connection);
    }

    private static final class TestContext {
        private final JdbcInputFormat inputFormat;
        private final ChunkSplitter chunkSplitter;
        private final PreparedStatement statement;
        private final ResultSet resultSet;
        private final Connection connection;

        private TestContext(
                JdbcInputFormat inputFormat,
                ChunkSplitter chunkSplitter,
                PreparedStatement statement,
                ResultSet resultSet,
                Connection connection) {
            this.inputFormat = inputFormat;
            this.chunkSplitter = chunkSplitter;
            this.statement = statement;
            this.resultSet = resultSet;
            this.connection = connection;
        }

        private void openEmptySplit() throws IOException, SQLException {
            when(chunkSplitter.generateSplitStatement(SPLIT, TABLE_SCHEMA)).thenReturn(statement);
            when(statement.executeQuery()).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(false);
            when(statement.getConnection()).thenReturn(connection);
            inputFormat.open(SPLIT);
        }
    }
}
