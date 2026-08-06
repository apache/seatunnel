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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
public class ConnectionPoolManager {

    private static final long DEFAULT_VALIDATION_TIMEOUT_MILLIS = 5000L;

    private final HikariDataSource connectionPool;

    private final Map<Integer, Connection> connectionMap;

    ConnectionPoolManager(HikariDataSource connectionPool) {
        this.connectionPool = connectionPool;
        connectionMap = new ConcurrentHashMap<>();
    }

    /**
     * Returns the connection held for this queue index, replacing it first if it is no longer
     * usable.
     *
     * <p>A connection is borrowed from the pool once per index and then held for the lifetime of
     * the writer, so it never goes back for Hikari's housekeeper to inspect. On a streaming job the
     * gap between writes can be hours, and the server or anything on the path can close the socket
     * in the meantime. Handing the cached connection back without checking it means the failure
     * surfaces as a write error after the record has already been consumed.
     */
    public Connection getConnection(int index) {
        return connectionMap.compute(
                index,
                (i, cached) -> {
                    if (cached != null && isUsable(cached)) {
                        return cached;
                    }
                    closeQuietly(cached);
                    try {
                        return connectionPool.getConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Uses the pool's own validation settings rather than a second opinion: {@code
     * connectionTestQuery} when one is configured, because it is configured precisely for drivers
     * whose {@code isValid} cannot be trusted, and {@code isValid} otherwise.
     */
    private boolean isUsable(Connection connection) {
        try {
            if (connection.isClosed()) {
                return false;
            }
            int timeoutSeconds =
                    (int)
                            Math.max(
                                    1,
                                    TimeUnit.MILLISECONDS.toSeconds(getValidationTimeoutMillis()));
            String testQuery = connectionPool.getConnectionTestQuery();
            if (testQuery == null) {
                return connection.isValid(timeoutSeconds);
            }
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(timeoutSeconds);
                statement.execute(testQuery);
                return true;
            }
        } catch (SQLException e) {
            log.debug("Cached connection for index is no longer usable, replacing it", e);
            return false;
        }
    }

    private long getValidationTimeoutMillis() {
        long configured = connectionPool.getValidationTimeout();
        return configured > 0 ? configured : DEFAULT_VALIDATION_TIMEOUT_MILLIS;
    }

    private void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            log.debug("Failed to close an unusable connection, discarding it anyway", e);
        }
    }

    public boolean containsConnection(int index) {
        return connectionMap.containsKey(index);
    }

    public Connection remove(int index) {
        return connectionMap.remove(index);
    }

    public String getPoolName() {
        return connectionPool.getPoolName();
    }

    public void close() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
    }
}
