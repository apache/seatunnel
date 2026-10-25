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

class JdbcOutputFormatReconnectSafetyTest {

    @Test
    void testFlushRetryShouldFailFastWhenIntegrityConstraintViolation() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.isConnectionValid()).thenReturn(true);

        TestNonRetryableDataErrorExecutor executor =
                new TestNonRetryableDataErrorExecutor(
                        new SQLException("ORA-00001: unique constraint violated", "23000", 1));

        NoSleepJdbcOutputFormat<SeaTunnelRow, TestNonRetryableDataErrorExecutor> outputFormat =
                new NoSleepJdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);

        Assertions.assertEquals(1, executor.prepareStatementsCalls);
        Assertions.assertEquals(1, executor.executeBatchCalls);
        Assertions.assertEquals(0, executor.closeStatementsCalls);
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    @Test
    void testFlushRetryShouldFailFastWhenOracleValueTooLarge() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.isConnectionValid()).thenReturn(true);

        TestNonRetryableDataErrorExecutor executor =
                new TestNonRetryableDataErrorExecutor(
                        new SQLException("ORA-12899: value too large for column", "72000", 12899));

        NoSleepJdbcOutputFormat<SeaTunnelRow, TestNonRetryableDataErrorExecutor> outputFormat =
                new NoSleepJdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);

        Assertions.assertEquals(1, executor.prepareStatementsCalls);
        Assertions.assertEquals(1, executor.executeBatchCalls);
        Assertions.assertEquals(0, executor.closeStatementsCalls);
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    @Test
    void testCloseShouldNotFlushAgainAfterFlushFailure() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.isConnectionValid()).thenReturn(true);

        TestNonRetryableDataErrorExecutor executor =
                new TestNonRetryableDataErrorExecutor(
                        new SQLException("ORA-12899: value too large for column", "72000", 12899));

        NoSleepJdbcOutputFormat<SeaTunnelRow, TestNonRetryableDataErrorExecutor> outputFormat =
                new NoSleepJdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::close);

        Assertions.assertEquals(1, executor.prepareStatementsCalls);
        Assertions.assertEquals(1, executor.executeBatchCalls);
        Assertions.assertEquals(1, executor.closeStatementsCalls);
    }

    @Test
    void testFlushRetryShouldNotTreatGenericVendorCodeOneAsOracleDataError() throws Exception {
        JdbcConnectionProvider provider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(connection);
        Mockito.when(provider.getConnection()).thenReturn(connection);
        Mockito.when(provider.isConnectionValid()).thenReturn(true);

        TestNonRetryableDataErrorExecutor executor =
                new TestNonRetryableDataErrorExecutor(
                        new SQLException("generic driver failure", "HY000", 1));

        NoSleepJdbcOutputFormat<SeaTunnelRow, TestNonRetryableDataErrorExecutor> outputFormat =
                new NoSleepJdbcOutputFormat<>(provider, buildConnectionConfig(), () -> executor);
        outputFormat.open();
        outputFormat.writeRecord(new SeaTunnelRow(new Object[] {"AA"}));

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);

        Assertions.assertEquals(1, executor.prepareStatementsCalls);
        Assertions.assertEquals(3, executor.executeBatchCalls);
        Assertions.assertEquals(0, executor.closeStatementsCalls);
        Mockito.verify(provider, Mockito.never()).reestablishConnection();
    }

    private static JdbcConnectionConfig buildConnectionConfig() {
        return JdbcConnectionConfig.builder()
                .url("jdbc:test")
                .batchSize(100)
                .batchIntervalMs(0)
                .maxRetries(2)
                .build();
    }

    private static class NoSleepJdbcOutputFormat<I, E extends JdbcBatchStatementExecutor<I>>
            extends JdbcOutputFormat<I, E> {
        private NoSleepJdbcOutputFormat(
                JdbcConnectionProvider connectionProvider,
                JdbcConnectionConfig jdbcConnectionConfig,
                StatementExecutorFactory<E> statementExecutorFactory) {
            super(connectionProvider, jdbcConnectionConfig, statementExecutorFactory);
        }

        @Override
        protected void sleepBeforeFlushRetry(long sleepMs) throws InterruptedException {}
    }

    private static class TestNonRetryableDataErrorExecutor
            implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final SQLException flushFailure;
        private int prepareStatementsCalls;
        private int executeBatchCalls;
        private int closeStatementsCalls;

        private TestNonRetryableDataErrorExecutor(SQLException flushFailure) {
            this.flushFailure = flushFailure;
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
            throw flushFailure;
        }

        @Override
        public void closeStatements() {
            closeStatementsCalls++;
        }
    }
}
