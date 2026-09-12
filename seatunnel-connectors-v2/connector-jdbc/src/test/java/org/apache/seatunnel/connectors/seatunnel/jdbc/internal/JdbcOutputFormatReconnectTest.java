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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;

/** Tests JDBC output retry decisions for nested SQLExceptions thrown by batch execution. */
public class JdbcOutputFormatReconnectTest {

    @Test
    public void testFlushRetryShouldReconnectWhenBatchNextExceptionHasConnectionSqlState()
            throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.reestablishConnection()).thenReturn(connection);

        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(
                        batchException(
                                "batch failed",
                                "HY000",
                                new SQLException("connection dropped", "08006")));

        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        outputFormat.flush();

        Assertions.assertEquals(2, executor.prepareStatementsCalls);
        Assertions.assertEquals(2, executor.executeBatchCalls);
        Assertions.assertEquals(1, executor.closeStatementsCalls);
        Mockito.verify(provider).reestablishConnection();
        Mockito.verify(provider, Mockito.never()).isConnectionValid();
    }

    @Test
    public void testFlushRetryShouldReconnectWhenBatchNextExceptionIsStatementClosed()
            throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.reestablishConnection()).thenReturn(connection);

        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(
                        batchException(
                                "batch failed",
                                "HY000",
                                new SQLException("No operations allowed after statement closed.")));

        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        outputFormat.flush();

        Assertions.assertEquals(2, executor.prepareStatementsCalls);
        Assertions.assertEquals(2, executor.executeBatchCalls);
        Assertions.assertEquals(1, executor.closeStatementsCalls);
        Mockito.verify(provider).reestablishConnection();
        Mockito.verify(provider, Mockito.never()).isConnectionValid();
    }

    @Test
    public void testFlushRetryShouldReconnectWhenSqlServerStatementHandleIsNotExecuting()
            throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.reestablishConnection()).thenReturn(connection);

        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(
                        batchException(
                                "batch failed",
                                "HY000",
                                new SQLException("Statement handle is not executing.")));

        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        outputFormat.flush();

        Assertions.assertEquals(2, executor.prepareStatementsCalls);
        Assertions.assertEquals(2, executor.executeBatchCalls);
        Assertions.assertEquals(1, executor.closeStatementsCalls);
        Mockito.verify(provider).reestablishConnection();
        Mockito.verify(provider, Mockito.never()).isConnectionValid();
    }

    private JdbcConnectionConfig buildConnectionConfig() {
        return JdbcConnectionConfig.builder()
                .url("jdbc:postgresql://localhost:5432/test")
                .maxRetries(1)
                .batchSize(1024)
                .build();
    }

    private static BatchUpdateException batchException(
            String message, String sqlState, SQLException nextException) {
        BatchUpdateException exception = new BatchUpdateException(message, sqlState, new int[0]);
        exception.setNextException(nextException);
        return exception;
    }

    private static class TrackingJdbcBatchExecutor
            implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final SQLException firstFailure;
        private boolean failedOnce;
        private int prepareStatementsCalls;
        private int executeBatchCalls;
        private int closeStatementsCalls;

        private TrackingJdbcBatchExecutor(SQLException firstFailure) {
            this.firstFailure = firstFailure;
        }

        @Override
        public void prepareStatements(Connection connection) {
            prepareStatementsCalls++;
        }

        @Override
        public void addToBatch(SeaTunnelRow record) {}

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCalls++;
            if (!failedOnce) {
                failedOnce = true;
                throw firstFailure;
            }
        }

        @Override
        public void closeStatements() {
            closeStatementsCalls++;
        }
    }
}
