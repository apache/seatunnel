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

package io.debezium.connector.postgresql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Clock;

import java.sql.SQLException;
import java.time.Duration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the in-place JDBC data connection reconnect performed by the WAL streaming probe. */
public class PostgresStreamingChangeEventSourceProbeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReconnectDataConnectionOnceWhenProbeFails() throws Exception {
        PostgresConnection connection = mock(PostgresConnection.class);
        when(connection.prepareQuery("SELECT 1"))
                .thenThrow(new SQLException("connection reset"))
                .thenReturn(connection);
        when(connection.connect()).thenReturn(connection);
        when(connection.commit()).thenReturn(connection);

        newSource(connection).probeConnectionIfNeeded(streamingOffsetContext());

        verify(connection).close();
        verify(connection).connect();
        verify(connection, times(2)).prepareQuery("SELECT 1");
        verify(connection).commit();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPropagateWhenReconnectFailsSoEngineCanRestart() throws Exception {
        PostgresConnection connection = mock(PostgresConnection.class);
        when(connection.prepareQuery("SELECT 1")).thenThrow(new SQLException("connection reset"));
        when(connection.connect()).thenThrow(new SQLException("database is shutting down"));

        Assertions.assertThrows(
                SQLException.class,
                () -> newSource(connection).probeConnectionIfNeeded(streamingOffsetContext()));

        verify(connection).close();
        verify(connection, times(1)).prepareQuery("SELECT 1");
        verify(connection, never()).commit();
    }

    @Test
    public void shouldRethrowWithoutReconnectDuringPreSnapshotCatchUpStreaming() throws Exception {
        PostgresConnection connection = mock(PostgresConnection.class);
        SQLException reset = new SQLException("connection reset");
        when(connection.prepareQuery("SELECT 1")).thenThrow(reset);
        PostgresOffsetContext catchUpOffsetContext = mock(PostgresOffsetContext.class);
        when(catchUpOffsetContext.getStreamingStoppingLsn()).thenReturn(Lsn.valueOf(100L));

        SQLException thrown =
                Assertions.assertThrows(
                        SQLException.class,
                        () -> newSource(connection).probeConnectionIfNeeded(catchUpOffsetContext));

        Assertions.assertSame(reset, thrown);
        verify(connection, never()).close();
        verify(connection, never()).connect();
        verify(connection, times(1)).prepareQuery("SELECT 1");
    }

    @SuppressWarnings("unchecked")
    private static PostgresStreamingChangeEventSource newSource(PostgresConnection connection) {
        PostgresConnectorConfig connectorConfig = mock(PostgresConnectorConfig.class);
        when(connectorConfig.statusUpdateInterval()).thenReturn(Duration.ofSeconds(10));
        when(connectorConfig.getPollInterval()).thenReturn(Duration.ofMillis(500));

        PostgresTaskContext taskContext = mock(PostgresTaskContext.class);
        when(taskContext.getConfig()).thenReturn(connectorConfig);

        return new PostgresStreamingChangeEventSource(
                connectorConfig,
                mock(Snapshotter.class),
                connection,
                mock(PostgresEventDispatcher.class),
                mock(ErrorHandler.class),
                Clock.system(),
                mock(PostgresSchema.class),
                taskContext,
                mock(ReplicationConnection.class));
    }

    private static PostgresOffsetContext streamingOffsetContext() {
        PostgresOffsetContext offsetContext = mock(PostgresOffsetContext.class);
        when(offsetContext.getStreamingStoppingLsn()).thenReturn(null);
        return offsetContext;
    }
}
