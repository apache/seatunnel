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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** A JDBC outputFormat */
public class JdbcOutputFormat<I, E extends JdbcBatchStatementExecutor<I>> implements Serializable {

    protected final JdbcConnectionProvider connectionProvider;

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

    private final JdbcConnectionConfig jdbcConnectionConfig;
    private final StatementExecutorFactory<E> statementExecutorFactory;

    private transient E jdbcStatementExecutor;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;
    private transient volatile boolean flushFailed = false;
    private transient volatile Exception flushException;
    private transient long lastFlushTimeMs;

    public JdbcOutputFormat(
            JdbcConnectionProvider connectionProvider,
            JdbcConnectionConfig jdbcConnectionConfig,
            StatementExecutorFactory<E> statementExecutorFactory) {
        this.connectionProvider = checkNotNull(connectionProvider);
        this.jdbcConnectionConfig = checkNotNull(jdbcConnectionConfig);
        this.statementExecutorFactory = checkNotNull(statementExecutorFactory);
    }

    /** Connects to the target database and initializes the prepared statement. */
    public void open() throws IOException {
        try {
            connectionProvider.getOrEstablishConnection();
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "unable to open JDBC writer",
                    e);
        }
        jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
        lastFlushTimeMs = System.currentTimeMillis();
    }

    private E createAndOpenStatementExecutor(StatementExecutorFactory<E> statementExecutorFactory) {
        E exec = statementExecutorFactory.get();
        try {
            exec.prepareStatements(connectionProvider.getConnection());
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "unable to open JDBC writer",
                    e);
        }
        return exec;
    }

    public void checkFlushException() {
        if (flushException != null) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to JDBC failed.",
                    flushException);
        }
    }

    public final synchronized void writeRecord(I record) {
        checkFlushException();
        try {
            addToBatch(record);
            batchCount++;
            if (batchCount > 0 && (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit())) {
                flush();
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Writing records to JDBC failed.",
                    e);
        }
    }

    protected void addToBatch(I record) throws SQLException {
        jdbcStatementExecutor.addToBatch(record);
    }

    public synchronized void flush() throws IOException {
        if (flushException != null) {
            LOG.warn(
                    String.format(
                            "An exception occurred during the previous flush process %s, skipping"
                                    + " this flush",
                            ExceptionUtils.getMessage(flushException)));
            return;
        }

        if (batchCount == 0) {
            LOG.debug("No data to flush.");
            return;
        }

        final int sleepMs = 1000;
        for (int i = 0; i <= jdbcConnectionConfig.getMaxRetries(); i++) {
            try {
                attemptFlush();
                batchCount = 0;
                flushFailed = false;
                flushException = null;
                lastFlushTimeMs = System.currentTimeMillis();
                break;
            } catch (SQLException e) {
                recordFlushException(e);
                LOG.error("JDBC executeBatch error, retry times = {}", i, e);

                List<SQLException> sqlExceptions = findSqlExceptions(e);
                SQLException nonRetryableDataException =
                        findNonRetryableDataException(sqlExceptions);
                if (nonRetryableDataException != null) {
                    boolean connectionValid;
                    try {
                        connectionValid = connectionProvider.isConnectionValid();
                    } catch (SQLException exception) {
                        LOG.error("JDBC connection validity check failed.", exception);
                        throw new JdbcConnectorException(
                                JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                                "JDBC connection validity check failed",
                                exception);
                    }
                    if (connectionValid) {
                        LOG.error(
                                "Flush failed by non-retryable data error. batchCount={}, retry"
                                        + " times = {}, sqlState={}, errorCode={}",
                                batchCount,
                                i,
                                nonRetryableDataException.getSQLState(),
                                nonRetryableDataException.getErrorCode(),
                                e);
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED, e);
                    }
                }

                if (i >= jdbcConnectionConfig.getMaxRetries()) {
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED, e);
                }
                try {
                    if (!connectionProvider.isConnectionValid()) {
                        updateExecutor(true);
                    }
                } catch (Exception exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed.",
                            exception);
                    throw new JdbcConnectorException(
                            JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                            "Reestablish JDBC connection failed",
                            exception);
                }
                try {
                    sleepBeforeFlushRetry((long) sleepMs * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "unable to flush; interrupted while doing another attempt",
                            e);
                }
            }
        }
    }

    protected void attemptFlush() throws SQLException {
        jdbcStatementExecutor.executeBatch();
    }

    /** Executes prepared statement and closes all resources of this instance. */
    public synchronized void close() {
        if (!closed) {
            closed = true;
            flushBufferedRecords();
            closeStatements();
        }
        connectionProvider.closeConnection();
        checkFlushException();
    }

    private void flushBufferedRecords() {
        if (batchCount > 0 && !flushFailed) {
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to JDBC failed.", e);
                flushFailed = true;
                flushException =
                        new JdbcConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                "Writing records to JDBC failed.",
                                e);
            }
        } else if (batchCount > 0) {
            LOG.warn(
                    "Skip flushing buffered JDBC records during close because the previous flush"
                            + " failed.");
        }
    }

    public void closeStatements() {
        try {
            if (jdbcStatementExecutor != null) {
                jdbcStatementExecutor.closeStatements();
            }
        } catch (SQLException | JdbcConnectorException e) {
            LOG.warn("Close JDBC writer failed.", e);
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        return jdbcConnectionConfig.getBatchSize() > 0
                && batchCount >= jdbcConnectionConfig.getBatchSize();
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long batchIntervalMs = jdbcConnectionConfig.getBatchIntervalMs();
        return batchIntervalMs > 0
                && (System.currentTimeMillis() - lastFlushTimeMs) >= batchIntervalMs;
    }

    private void recordFlushException(Exception e) {
        flushFailed = true;
        flushException =
                e instanceof JdbcConnectorException
                        ? e
                        : new JdbcConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                "Writing records to JDBC failed.",
                                e);
    }

    protected void sleepBeforeFlushRetry(long sleepMs) throws InterruptedException {
        Thread.sleep(sleepMs);
    }

    private List<SQLException> findSqlExceptions(Throwable throwable) {
        List<SQLException> sqlExceptions = new ArrayList<>();
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException) {
                collectSqlExceptionChain((SQLException) current, sqlExceptions);
            }
            current = current.getCause();
        }
        return sqlExceptions;
    }

    private void collectSqlExceptionChain(
            SQLException sqlException, List<SQLException> sqlExceptions) {
        SQLException current = sqlException;
        while (current != null) {
            sqlExceptions.add(current);
            current = current.getNextException();
        }
    }

    private SQLException findNonRetryableDataException(List<SQLException> sqlExceptions) {
        for (SQLException sqlException : sqlExceptions) {
            if (isNonRetryableDataException(sqlException)) {
                return sqlException;
            }
        }
        return null;
    }

    private boolean isNonRetryableDataException(SQLException sqlException) {
        if (sqlException instanceof SQLDataException
                || sqlException instanceof SQLIntegrityConstraintViolationException) {
            return true;
        }

        String sqlState = sqlException.getSQLState();
        if (sqlState != null && (sqlState.startsWith("22") || sqlState.startsWith("23"))) {
            return true;
        }

        return isOracleDataException(sqlException);
    }

    private boolean isOracleDataException(SQLException sqlException) {
        String message = sqlException.getMessage();
        if (message == null) {
            return false;
        }

        String normalizedMessage = message.toUpperCase(Locale.ROOT);
        int vendorCode = sqlException.getErrorCode();
        return (vendorCode == 1 && normalizedMessage.contains("ORA-00001"))
                || (vendorCode == 12899 && normalizedMessage.contains("ORA-12899"));
    }

    public void updateExecutor(boolean reconnect) throws SQLException, ClassNotFoundException {
        try {
            jdbcStatementExecutor.closeStatements();
        } catch (SQLException | JdbcConnectorException e) {
            if (!reconnect) {
                throw e;
            }
            LOG.error("Close JDBC statement failed on reconnect.", e);
        }
        jdbcStatementExecutor.prepareStatements(
                reconnect
                        ? connectionProvider.reestablishConnection()
                        : connectionProvider.getConnection());
    }

    /**
     * A factory for creating {@link JdbcBatchStatementExecutor} instance.
     *
     * @param <T> The type of instance.
     */
    public interface StatementExecutorFactory<T extends JdbcBatchStatementExecutor<?>>
            extends Supplier<T>, Serializable {}
}
