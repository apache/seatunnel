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
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Getter
public class ConnectionPoolManager {

    private static final long DEFAULT_VALIDATION_TIMEOUT_MILLIS = 5000L;

    private static final long REPLACEMENT_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    private final HikariDataSource connectionPool;

    private final Map<Integer, Connection> connectionMap;

    private final AtomicLong replacementsSinceLastWarn = new AtomicLong();

    private final AtomicLong lastReplacementWarnNanos;

    ConnectionPoolManager(HikariDataSource connectionPool) {
        this.connectionPool = connectionPool;
        connectionMap = new ConcurrentHashMap<>();
        // Backdated so the first replacement warns immediately rather than waiting out an interval.
        lastReplacementWarnNanos =
                new AtomicLong(System.nanoTime() - REPLACEMENT_WARN_INTERVAL_NANOS);
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
                    if (cached != null) {
                        logReplacement(i);
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

    /**
     * Reports a replaced connection at WARN, rate limited to one message per minute per manager.
     *
     * <p>Replacing an occasionally idle connection is the expected case and is not worth a warning
     * on every occurrence. A validation that fails systematically is not: a misconfigured {@code
     * connectionTestQuery}, a persistent network fault or a server-side connection limit makes
     * every call replace the connection, which silently turns the cache off and churns connections
     * continuously. Logging only at DEBUG would leave that indistinguishable from healthy operation
     * in a default deployment, so the first occurrence and a periodic summary are surfaced.
     */
    private void logReplacement(int index) {
        long replacements = replacementsSinceLastWarn.incrementAndGet();
        long now = System.nanoTime();
        long last = lastReplacementWarnNanos.get();

        if (now - last >= REPLACEMENT_WARN_INTERVAL_NANOS
                && lastReplacementWarnNanos.compareAndSet(last, now)) {
            replacementsSinceLastWarn.addAndGet(-replacements);
            log.warn(
                    "Replaced an unusable pooled connection for queue index {}. "
                            + "{} replacement(s) since the last such message. Repeated messages "
                            + "indicate the connection is not surviving between writes.",
                    index,
                    replacements);

            return;
        }

        log.debug("Cached connection for queue index {} is no longer usable, replacing it", index);
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
