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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcOutputFormatTest {

    @Test
    public void testWriteRecordKeepsVoidSignature() throws Exception {
        Assertions.assertEquals(
                Void.TYPE,
                JdbcOutputFormat.class.getMethod("writeRecord", Object.class).getReturnType());
    }

    @Test
    public void testWriteRecordWithAutoFlushReturnsTrueWhenBatchIntervalFlushes() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        CountingExecutor executor = new CountingExecutor();
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .batchSize(100)
                        .batchIntervalMs(1)
                        .maxRetries(0)
                        .build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);

        JdbcOutputFormat<String, CountingExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.open();
        setLastFlushTimeMs(outputFormat, 0L);

        boolean autoFlushed = outputFormat.writeRecordWithAutoFlush("row-1");

        Assertions.assertTrue(autoFlushed);
        Assertions.assertEquals(1, executor.executeBatchCount);
    }

    @Test
    public void testClearBatchFailureIsPropagatedAndKeepsBatchCount() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        FailingClearBatchExecutor executor = new FailingClearBatchExecutor();
        JdbcConnectionConfig config = JdbcConnectionConfig.builder().batchSize(100).build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);

        JdbcOutputFormat<String, FailingClearBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.open();
        outputFormat.writeRecordWithAutoFlush("row-1");

        JdbcConnectorException ex =
                Assertions.assertThrows(
                        JdbcConnectorException.class, outputFormat::clearBatchSilently);

        Assertions.assertTrue(ex.getMessage().contains("Failed to clear JDBC batch"));
        Assertions.assertEquals(1, getBatchCount(outputFormat));
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(0, executor.executeBatchCount);
        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::close);
        Assertions.assertEquals(0, executor.executeBatchCount);
    }

    @Test
    public void testRowLevelSqlStateStillRetriesByDefault() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        RowLevelSQLExceptionExecutor executor = new RowLevelSQLExceptionExecutor();
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder().batchSize(1).maxRetries(1).build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);
        when(connectionProvider.isConnectionValid()).thenReturn(true);

        JdbcOutputFormat<String, RowLevelSQLExceptionExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.open();

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> outputFormat.writeRecord("row-1"));
        Assertions.assertEquals(2, executor.executeBatchCount);
    }

    @Test
    public void testRowLevelSqlStateFailsFastWhenEnabled() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        RowLevelSQLExceptionExecutor executor = new RowLevelSQLExceptionExecutor();
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder().batchSize(1).maxRetries(1).build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);

        JdbcOutputFormat<String, RowLevelSQLExceptionExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.setFailFastOnRowLevelSqlState(true);
        outputFormat.open();

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> outputFormat.writeRecord("row-1"));
        Assertions.assertEquals(1, executor.executeBatchCount);
    }

    @Test
    public void testCloseDoesNotFlushAgainAfterFlushFailure() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        FailingBatchExecutor executor = new FailingBatchExecutor();
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder().batchSize(1).maxRetries(0).build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);

        JdbcOutputFormat<String, FailingBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.open();

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> outputFormat.writeRecord("row-1"));
        Assertions.assertEquals(1, executor.executeBatchCount);

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(1, executor.executeBatchCount);

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::close);
        Assertions.assertEquals(1, executor.executeBatchCount);
    }

    @Test
    public void testCloseDoesNotFlushAgainAfterRuntimeFlushFailure() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        RuntimeFailingBatchExecutor executor = new RuntimeFailingBatchExecutor();
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder().batchSize(1).maxRetries(0).build();

        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connectionProvider.getConnection()).thenReturn(connection);

        JdbcOutputFormat<String, RuntimeFailingBatchExecutor> outputFormat =
                new JdbcOutputFormat<>(connectionProvider, config, () -> executor);
        outputFormat.open();

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> outputFormat.writeRecord("row-1"));
        Assertions.assertEquals(1, executor.executeBatchCount);

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::flush);
        Assertions.assertEquals(1, executor.executeBatchCount);

        Assertions.assertThrows(JdbcConnectorException.class, outputFormat::close);
        Assertions.assertEquals(1, executor.executeBatchCount);
    }

    private static void setLastFlushTimeMs(JdbcOutputFormat<?, ?> outputFormat, long value)
            throws Exception {
        Field field = JdbcOutputFormat.class.getDeclaredField("lastFlushTimeMs");
        field.setAccessible(true);
        field.set(outputFormat, value);
    }

    private static int getBatchCount(JdbcOutputFormat<?, ?> outputFormat) throws Exception {
        Field field = JdbcOutputFormat.class.getDeclaredField("batchCount");
        field.setAccessible(true);
        return (int) field.get(outputFormat);
    }

    private static class CountingExecutor implements JdbcBatchStatementExecutor<String> {

        protected int executeBatchCount;

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(String record) {}

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCount++;
        }

        @Override
        public void closeStatements() throws SQLException {}
    }

    private static class FailingClearBatchExecutor extends CountingExecutor {

        @Override
        public void clearBatch() throws SQLException {
            throw new SQLException("clear failed");
        }
    }

    private static class RowLevelSQLExceptionExecutor extends CountingExecutor {

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCount++;
            throw new SQLException("data too long", "22001");
        }
    }

    private static class FailingBatchExecutor extends CountingExecutor {

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCount++;
            throw new SQLException("flush failed");
        }
    }

    private static class RuntimeFailingBatchExecutor extends CountingExecutor {

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCount++;
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED, "flush failed");
        }
    }
}
