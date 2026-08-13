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

import java.sql.Connection;
import java.sql.SQLException;

/** Tests the commit boundary of batch-size-triggered flushes in {@link JdbcOutputFormat}. */
class JdbcOutputFormatBatchCommitTest {

    @Test
    void testInternalBatchFlushCommitsWhenCommitOnFlushEnabled() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        CountingExecutor executor = new CountingExecutor();
        JdbcOutputFormat<SeaTunnelRow, CountingExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor, true);
        outputFormat.open();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        Mockito.verify(connection, Mockito.never()).commit();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.times(1)).commit();
    }

    @Test
    void testInternalBatchFlushSkipsCommitWhenCommitOnFlushDisabled() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        CountingExecutor executor = new CountingExecutor();
        JdbcOutputFormat<SeaTunnelRow, CountingExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor, false);
        outputFormat.open();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.never()).commit();
    }

    @Test
    void testInternalBatchFlushSkipsCommitForAutoCommitConnection() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(true);

        CountingExecutor executor = new CountingExecutor();
        JdbcOutputFormat<SeaTunnelRow, CountingExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor, true);
        outputFormat.open();

        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"BB"}));

        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.never()).commit();
    }

    @Test
    void testCommitFailureAfterBatchFlushShouldNotReconnectAndRetryEmptyBatch() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.doThrow(new SQLException("commit failed", "08006")).when(connection).commit();

        CountingExecutor executor = new CountingExecutor();
        JdbcOutputFormat<SeaTunnelRow, CountingExecutor> outputFormat =
                new JdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor, true);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);

        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.times(1)).commit();
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    private static JdbcConnectionConfig buildConnectionConfig() {
        return JdbcConnectionConfig.builder()
                .url("jdbc:test")
                .batchSize(2)
                .batchIntervalMs(0)
                .build();
    }

    private static class CountingExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private int executeBatchCalls;

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {}

        @Override
        public void executeBatch() {
            executeBatchCalls++;
        }

        @Override
        public void closeStatements() {}
    }
}
