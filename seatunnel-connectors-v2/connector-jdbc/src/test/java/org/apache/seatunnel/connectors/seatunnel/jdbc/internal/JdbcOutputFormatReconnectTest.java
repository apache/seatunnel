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
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
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

    @Test
    public void testFlushShouldFailInsteadOfReconnectingWhenEarlierBatchIsUncommitted()
            throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("connection dropped", "08006"), 2);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        // The first batch is flushed into the open transaction; the writer commits it later.
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        // Reconnecting would replay only "BB"; "AA" died with the old connection's transaction.
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(2, executor.executeBatchCalls);
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    @Test
    public void testFlushShouldFailInsteadOfRetryingWhenTransactionIsRolledBack() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Mockito.when(provider.isConnectionValid()).thenReturn(true);
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("deadlock detected", "40001"), 2);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        // A deadlock rolls back the whole transaction, including the earlier batch.
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(2, executor.executeBatchCalls);
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    @Test
    public void testFlushRetryShouldReconnectAfterWriterEndedTransaction() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("connection dropped", "08006"), 2);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();
        // The writer committed at the checkpoint, so nothing earlier is pending any more.
        outputFormat.markCommitted(provider.getConnection());
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        outputFormat.flush();

        Assertions.assertEquals(3, executor.executeBatchCalls);
        Mockito.verify(provider).reestablishConnection();
    }

    @Test
    public void testFlushRetryShouldReconnectWithEarlierBatchOnAutoCommitConnection()
            throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.reestablishConnection()).thenReturn(connection);
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("connection dropped", "08006"), 2);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        // With auto-commit every earlier batch is already durable, so a reconnect loses nothing.
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));
        outputFormat.flush();

        Assertions.assertEquals(3, executor.executeBatchCalls);
        Mockito.verify(provider).reestablishConnection();
    }

    @Test
    public void testCommitShouldFailWhenConnectionWithUncommittedBatchWasReplaced()
            throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Connection flushConnection = provider.getConnection();
        Connection replacement = Mockito.mock(Connection.class);
        TrackingJdbcBatchExecutor executor = new TrackingJdbcBatchExecutor(null, Integer.MAX_VALUE);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();

        // A pooled provider swaps a dead connection for a new one; committing it would lose "AA".
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(replacement));
        Assertions.assertDoesNotThrow(
                () -> outputFormat.checkUncommittedBatchesOn(flushConnection));

        // Rolling back the replacement does not bring "AA" back, so nothing may commit any more.
        outputFormat.markRolledBack(replacement, false);
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(replacement));
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(flushConnection));
    }

    @Test
    public void testCommitOfFlushConnectionClearsPendingBatches() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Connection flushConnection = provider.getConnection();
        Connection replacement = Mockito.mock(Connection.class);
        TrackingJdbcBatchExecutor executor = new TrackingJdbcBatchExecutor(null, Integer.MAX_VALUE);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();

        // Committing a replacement is not a commit of "AA".
        outputFormat.markCommitted(replacement);
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(replacement));

        // Committing the connection that held "AA" makes it durable; nothing is pending.
        outputFormat.markCommitted(flushConnection);
        Assertions.assertDoesNotThrow(() -> outputFormat.checkUncommittedBatchesOn(replacement));
    }

    @Test
    public void testFullRollbackOfOwnReportedBatchesDoesNotBlockLaterCommits() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Connection connection = provider.getConnection();
        TrackingJdbcBatchExecutor executor = new TrackingJdbcBatchExecutor(null, Integer.MAX_VALUE);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();

        // Row-level error handling reports the rows it rolls back, so nothing is silently lost.
        outputFormat.markRolledBack(connection, true);
        Assertions.assertDoesNotThrow(() -> outputFormat.checkUncommittedBatchesOn(connection));
    }

    @Test
    public void testFullRollbackOfUnreportedBatchesBlocksLaterCommits() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Connection connection = provider.getConnection();
        TrackingJdbcBatchExecutor executor = new TrackingJdbcBatchExecutor(null, Integer.MAX_VALUE);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();

        // A failed checkpoint rolled "AA" back without reporting it; a later commit on the same
        // connection must not report this interval as complete.
        outputFormat.markRolledBack(connection, false);
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(connection));
    }

    @Test
    public void testLostConnectionBlocksCommitEvenWithoutRetries() throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Connection connection = provider.getConnection();
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("connection dropped", "08006"), 2);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(
                        provider,
                        JdbcConnectionConfig.builder()
                                .url("jdbc:postgresql://localhost:5432/test")
                                .maxRetries(0)
                                .batchSize(1024)
                                .build(),
                        () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        // "AA" died with the connection; even the same connection object must not commit.
        Assertions.assertThrows(
                JdbcConnectorException.class,
                () -> outputFormat.checkUncommittedBatchesOn(connection));
    }

    @Test
    public void testFlushShouldNotRetryRolledBackTransactionSharedWithOtherWriters()
            throws Exception {
        JdbcConnectionProvider provider = manualCommitProvider();
        Mockito.when(provider.isConnectionValid()).thenReturn(true);
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("deadlock detected", "40001"), 1);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        // No earlier batch of this writer, but in a multi-table sink other writers can share the
        // connection, and the rollback discarded their uncommitted batches too.
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(1, executor.executeBatchCalls);
    }

    @Test
    public void testFlushShouldRetryRolledBackTransactionOnAutoCommitConnection() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.isConnectionValid()).thenReturn(true);
        TrackingJdbcBatchExecutor executor =
                new TrackingJdbcBatchExecutor(new SQLException("deadlock detected", "40001"), 1);
        JdbcOutputFormat<SeaTunnelRow, TrackingJdbcBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();

        // With auto-commit only the failed batch was rolled back, so retrying it is safe.
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.flush();

        Assertions.assertEquals(2, executor.executeBatchCalls);
    }

    private static JdbcConnectionProvider manualCommitProvider() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.reestablishConnection()).thenReturn(connection);
        return provider;
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
        private final int failOnCall;
        private boolean failedOnce;
        private int prepareStatementsCalls;
        private int executeBatchCalls;
        private int closeStatementsCalls;

        private TrackingJdbcBatchExecutor(SQLException firstFailure) {
            this(firstFailure, 1);
        }

        private TrackingJdbcBatchExecutor(SQLException firstFailure, int failOnCall) {
            this.firstFailure = firstFailure;
            this.failOnCall = failOnCall;
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
            if (!failedOnce && executeBatchCalls >= failOnCall) {
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
