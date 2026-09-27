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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectionPoolManagerTest {

    @Test
    void reusesAConnectionThatIsStillUsable() throws SQLException {
        Connection live = mock(Connection.class);
        when(live.isClosed()).thenReturn(false);
        when(live.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(live);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(live, manager.getConnection(0));
        Assertions.assertSame(live, manager.getConnection(0));

        // Borrowed once, then served from the cache.
        verify(pool, times(1)).getConnection();
        verify(live, never()).close();
    }

    @Test
    void replacesAConnectionThatHasGoneStale() throws SQLException {
        // A streaming sink can sit idle for hours between writes. The server closes the socket in
        // the meantime, and the cached connection is dead by the time the next record arrives.
        Connection stale = mock(Connection.class);
        when(stale.isClosed()).thenReturn(false);
        when(stale.isValid(anyInt())).thenReturn(false);

        Connection fresh = mock(Connection.class);
        when(fresh.isClosed()).thenReturn(false);
        when(fresh.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(stale, fresh);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(stale, manager.getConnection(0));
        Assertions.assertSame(fresh, manager.getConnection(0));

        verify(pool, times(2)).getConnection();
        // The dead one is handed back rather than leaked.
        verify(stale, times(1)).close();
    }

    @Test
    void replacesAConnectionThatIsAlreadyClosed() throws SQLException {
        Connection closed = mock(Connection.class);
        when(closed.isClosed()).thenReturn(true);

        Connection fresh = mock(Connection.class);
        when(fresh.isClosed()).thenReturn(false);
        when(fresh.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(closed, fresh);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(closed, manager.getConnection(0));
        Assertions.assertSame(fresh, manager.getConnection(0));

        // isValid is never consulted once isClosed has answered.
        verify(closed, never()).isValid(anyInt());
    }

    @Test
    void usesTheConfiguredTestQueryInsteadOfIsValid() throws SQLException {
        // connectionTestQuery is set for drivers whose isValid cannot be trusted, so the check has
        // to honour it rather than form a second opinion.
        Statement statement = mock(Statement.class);
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        when(connection.createStatement()).thenReturn(statement);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(connection);
        when(pool.getConnectionTestQuery()).thenReturn("SELECT 1");

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(connection, manager.getConnection(0));
        Assertions.assertSame(connection, manager.getConnection(0));

        verify(statement, times(1)).execute("SELECT 1");
        verify(connection, never()).isValid(anyInt());
    }

    @Test
    void replacesTheConnectionWhenTheTestQueryFails() throws SQLException {
        Statement failing = mock(Statement.class);
        when(failing.execute(anyString())).thenThrow(new SQLException("connection is closed"));

        Connection stale = mock(Connection.class);
        when(stale.isClosed()).thenReturn(false);
        when(stale.createStatement()).thenReturn(failing);

        Connection fresh = mock(Connection.class);
        when(fresh.isClosed()).thenReturn(false);
        when(fresh.createStatement()).thenReturn(mock(Statement.class));

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(stale, fresh);
        when(pool.getConnectionTestQuery()).thenReturn("SELECT 1");

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(stale, manager.getConnection(0));
        Assertions.assertSame(fresh, manager.getConnection(0));

        verify(pool, times(2)).getConnection();
    }

    @Test
    void passesThePoolsValidationTimeoutToIsValid() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        when(connection.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(connection);
        when(pool.getValidationTimeout()).thenReturn(8000L);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        manager.getConnection(0);
        manager.getConnection(0);

        // Seconds, rounded down from the pool's own millisecond setting.
        verify(connection, times(1)).isValid(8);
    }

    @Test
    void fallsBackToFiveSecondsWhenThePoolHasNoValidationTimeout() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        when(connection.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(connection);
        // Hikari reports 0 when the validation timeout has not been configured.
        when(pool.getValidationTimeout()).thenReturn(0L);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        manager.getConnection(0);
        manager.getConnection(0);

        verify(connection, times(1)).isValid(5);
    }

    @Test
    void neverPassesATimeoutBelowOneSecond() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        when(connection.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(connection);
        // Sub-second timeouts truncate to zero seconds, which isValid treats as no timeout at all.
        when(pool.getValidationTimeout()).thenReturn(250L);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        manager.getConnection(0);
        manager.getConnection(0);

        verify(connection, times(1)).isValid(1);
    }

    @Test
    void keepsConnectionsSeparatePerQueueIndex() throws SQLException {
        Connection first = mock(Connection.class);
        when(first.isClosed()).thenReturn(false);
        when(first.isValid(anyInt())).thenReturn(true);

        Connection second = mock(Connection.class);
        when(second.isClosed()).thenReturn(false);
        when(second.isValid(anyInt())).thenReturn(true);

        HikariDataSource pool = mock(HikariDataSource.class);
        when(pool.getConnection()).thenReturn(first, second);

        ConnectionPoolManager manager = new ConnectionPoolManager(pool);

        Assertions.assertSame(first, manager.getConnection(0));
        Assertions.assertSame(second, manager.getConnection(1));
        Assertions.assertSame(first, manager.getConnection(0));
        Assertions.assertSame(second, manager.getConnection(1));

        verify(pool, times(2)).getConnection();
    }
}
