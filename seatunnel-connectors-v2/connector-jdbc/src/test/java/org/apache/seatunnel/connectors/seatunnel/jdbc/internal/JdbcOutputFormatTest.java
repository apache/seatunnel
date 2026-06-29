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

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
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
    public void testWriteRecordReturnsTrueWhenBatchIntervalFlushes() throws Exception {
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

        boolean autoFlushed = outputFormat.writeRecord("row-1");

        Assertions.assertTrue(autoFlushed);
        Assertions.assertEquals(1, executor.executeBatchCount);
    }

    private static void setLastFlushTimeMs(JdbcOutputFormat<?, ?> outputFormat, long value)
            throws Exception {
        Field field = JdbcOutputFormat.class.getDeclaredField("lastFlushTimeMs");
        field.setAccessible(true);
        field.set(outputFormat, value);
    }

    private static class CountingExecutor implements JdbcBatchStatementExecutor<String> {

        private int executeBatchCount;

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(String record) {}

        @Override
        public void executeBatch() {
            executeBatchCount++;
        }

        @Override
        public void closeStatements() throws SQLException {}
    }
}
