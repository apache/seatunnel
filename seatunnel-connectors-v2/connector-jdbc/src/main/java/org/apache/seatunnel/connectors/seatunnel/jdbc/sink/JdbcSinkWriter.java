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

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.common.error.RowErrorPhase;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionValidationUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dsql.DdsqlJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class JdbcSinkWriter extends AbstractJdbcSinkWriter<ConnectionPoolManager>
        implements SupportRowLevelErrorClassifier<SeaTunnelRow> {
    private final Integer primaryKeyIndex;
    private final Optional<RowErrorCollector> rowErrorCollector;
    private final int batchSize;
    private final Object batchLock = new Object();
    private List<SeaTunnelRow> pendingRows;
    // Marks the last auto-flushed batch inside the open JDBC transaction so a later row-level
    // failure can roll back only the failed batch without moving the durable commit boundary.
    private Savepoint lastSuccessfulBatchSavepoint;
    // Tracks the Connection instance that owns the current transaction (i.e., the
    // connection on which auto-flushed batches and savepoints were created since the
    // last commit). When ConnectionPoolManager silently swaps the underlying connection
    // (e.g., due to HikariCP eviction), any savepoint becomes invalid per the JDBC
    // contract and the swapped connection has no pending work to commit — tracking the
    // connection lets us detect the swap and either fall back to a full rollback
    // (rollbackIfNeeded) or fail the checkpoint explicitly (commitIfNeeded) so the
    // framework can retry from the last durable checkpoint.
    private Connection transactionConnection;
    private Boolean supportsSavepoints;
    private boolean savepointUnsupportedLogged;

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            Integer primaryKeyIndex) {
        this(
                sinkTablePath,
                null,
                dialect,
                jdbcSinkConfig,
                tableSchema,
                databaseTableSchema,
                primaryKeyIndex);
    }

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            SinkWriter.Context context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            Integer primaryKeyIndex) {
        this.sinkTablePath = sinkTablePath;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.databaseTableSchema = databaseTableSchema;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.primaryKeyIndex = primaryKeyIndex;
        this.rowErrorCollector =
                context == null ? Optional.empty() : context.getRowErrorCollector();
        this.batchSize = jdbcSinkConfig.getJdbcConnectionConfig().getBatchSize();
        if (rowErrorCollector.isPresent()) {
            // Only maintain pending rows when collector is available.
            this.pendingRows = new ArrayList<>(Math.max(this.batchSize, 16));
            if (context != null) {
                context.enableDeferredTerminalWriteOutcomes();
            }
        }
        this.connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
        configureOutputFormatForRowErrorHandling();
        if (context != null) {
            context.registerFlushAction(this::timerFlush);
        }
    }

    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (Exception e) {
            log.warn(
                    "Failed to load JDBC driver {}",
                    jdbcSinkConfig.getJdbcConnectionConfig().getDriverName(),
                    e);
        }
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        ds.setDriverClassName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        applyConnectionValidation(ds, jdbcSinkConfig.getJdbcConnectionConfig());
        // Forward remaining properties to the JDBC DataSource, excluding HikariCP pool-level
        // properties that were already applied in applyConnectionValidation() above.
        jdbcSinkConfig
                .getJdbcConnectionConfig()
                .getProperties()
                .forEach(
                        (key, value) -> {
                            if (!isHikariPoolProperty(key)) {
                                ds.addDataSourceProperty(key, value);
                            }
                        });
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    /**
     * Configures pool-level validation for JDBC drivers that cannot pass Hikari's default
     * Connection.isValid(timeout) probe, and applies user-specified HikariCP pool-level properties
     * from the {@code properties} configuration block.
     *
     * <p>HikariCP pool-level properties (e.g. {@code maxLifetime}, {@code keepaliveTime}, {@code
     * validationTimeout}) must be set via {@code HikariDataSource} setter methods. Passing them
     * through {@code addDataSourceProperty()} routes them to the underlying JDBC {@code
     * DataSource}, where they are silently ignored. This method ensures all user-configured
     * pool-level properties are properly applied.
     */
    static void applyConnectionValidation(
            HikariDataSource dataSource, JdbcConnectionConfig jdbcConnectionConfig) {
        JdbcConnectionValidationUtils.getConnectionValidationQuery(jdbcConnectionConfig)
                .ifPresent(dataSource::setConnectionTestQuery);

        // Apply HikariCP pool-level properties from the user's properties config.
        // These properties are silently ignored when passed via addDataSourceProperty(),
        // so they must be set explicitly on the HikariDataSource.
        java.util.Map<String, String> props = jdbcConnectionConfig.getProperties();
        if (props != null && !props.isEmpty()) {
            applyHikariIntProperty(
                    props, "maximumPoolSize", "maximum-pool-size", dataSource::setMaximumPoolSize);
            applyHikariIntProperty(
                    props, "minimumIdle", "minimum-idle", dataSource::setMinimumIdle);
            applyHikariLongProperty(
                    props,
                    "connectionTimeout",
                    "connection-timeout",
                    dataSource::setConnectionTimeout);
            applyHikariLongProperty(
                    props, "idleTimeout", "idle-timeout", dataSource::setIdleTimeout);
            applyHikariLongProperty(
                    props, "maxLifetime", "max-lifetime", dataSource::setMaxLifetime);
            applyHikariLongProperty(
                    props, "keepaliveTime", "keepalive-time", dataSource::setKeepaliveTime);
            applyHikariLongProperty(
                    props,
                    "validationTimeout",
                    "validation-timeout",
                    dataSource::setValidationTimeout);
        }
    }

    private static void applyHikariIntProperty(
            java.util.Map<String, String> props,
            String camelKey,
            String kebabKey,
            java.util.function.IntConsumer setter) {
        String value = props.get(camelKey);
        if (value == null) {
            value = props.get(kebabKey);
        }
        if (value != null) {
            try {
                setter.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                log.warn("Invalid integer value for HikariCP property '{}': {}", camelKey, value);
            }
        }
    }

    private static void applyHikariLongProperty(
            java.util.Map<String, String> props,
            String camelKey,
            String kebabKey,
            java.util.function.LongConsumer setter) {
        String value = props.get(camelKey);
        if (value == null) {
            value = props.get(kebabKey);
        }
        if (value != null) {
            try {
                setter.accept(Long.parseLong(value));
            } catch (NumberFormatException e) {
                log.warn("Invalid long value for HikariCP property '{}': {}", camelKey, value);
            }
        }
    }

    private static final java.util.Set<String> HIKARI_POOL_PROPERTIES =
            java.util.Collections.unmodifiableSet(
                    new java.util.HashSet<>(
                            java.util.Arrays.asList(
                                    "connectionTestQuery",
                                    "connection-test-query",
                                    "maximumPoolSize",
                                    "maximum-pool-size",
                                    "minimumIdle",
                                    "minimum-idle",
                                    "connectionTimeout",
                                    "connection-timeout",
                                    "idleTimeout",
                                    "idle-timeout",
                                    "maxLifetime",
                                    "max-lifetime",
                                    "keepaliveTime",
                                    "keepalive-time",
                                    "validationTimeout",
                                    "validation-timeout")));

    private static boolean isHikariPoolProperty(String key) {
        return HIKARI_POOL_PROPERTIES.contains(key);
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
            int queueIndex) {
        connectionProvider.closeConnection();
        if (this.dialect.dialectName().equals(DatabaseIdentifier.DSQL)) {
            this.connectionProvider =
                    new DdsqlJdbcConnectionPoolProviderProxy(
                            jdbcSinkConfig.getJdbcConnectionConfig(), queueIndex);
        } else {
            this.connectionProvider =
                    new SimpleJdbcConnectionPoolProviderProxy(
                            multiTableResourceManager.getSharedResource().get(),
                            jdbcSinkConfig.getJdbcConnectionConfig(),
                            queueIndex);
        }
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .build();
        configureOutputFormatForRowErrorHandling();
    }

    private void configureOutputFormatForRowErrorHandling() {
        outputFormat.setFailFastOnRowLevelSqlState(rowErrorCollector.isPresent());
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (element.getArity() == 0) {
            return;
        }

        if (rowErrorCollector.isPresent()) {
            synchronized (batchLock) {
                tryOpen();
                try {
                    pendingRows.add(element);
                    boolean autoFlushed = outputFormat.writeRecordWithAutoFlush(element);
                    reportAndClearPendingRowsIfCommitted(autoFlushed);
                } catch (Throwable e) {
                    if (!isRowLevelDataError(e)) {
                        throwAsIoException(e);
                    }
                    // DROP_BATCH: report and discard batch, then continue.
                    List<SeaTunnelRow> batchRows = swapPendingRowsLocked();
                    handleRowLevelBatchFailure(RowErrorPhase.WRITE, null, batchRows, e);
                }
            }
            return;
        }

        tryOpen();
        outputFormat.writeRecord(element);
    }

    @Override
    public RowErrorClassification classifyRowError(Throwable t, SeaTunnelRow row) {
        return isRowLevelDataError(t)
                ? RowErrorClassification.ROW_ERROR
                : RowErrorClassification.SYSTEM_ERROR;
    }

    private boolean isRowLevelDataError(Throwable t) {
        // Only treat SQL data/constraint violations as row-level errors.
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof SQLException) {
                if (isRowLevelSqlState((SQLException) cause)) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private boolean isRowLevelSqlState(SQLException sqlException) {
        // Scan both the exception and nextException chain for relevant SQLState.
        Set<SQLException> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        SQLException current = sqlException;
        while (current != null && visited.add(current)) {
            String sqlState = current.getSQLState();
            if (sqlState != null) {
                // 22XXX: Data exception (e.g. data too long, invalid data)
                // 23XXX: Integrity constraint violation (e.g. duplicate key)
                if (sqlState.startsWith("22") || sqlState.startsWith("23")) {
                    return true;
                }
            }
            current = current.getNextException();
        }
        return false;
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        return prepareCommitInternal(null);
    }

    @Override
    public Optional<XidInfo> prepareCommit(long checkpointId) throws IOException {
        return prepareCommitInternal(checkpointId);
    }

    private Optional<XidInfo> prepareCommitInternal(Long checkpointId) throws IOException {
        if (rowErrorCollector.isPresent()) {
            synchronized (batchLock) {
                tryOpen();
                outputFormat.checkFlushException();
                List<SeaTunnelRow> batchRows = swapPendingRowsLocked();
                try {
                    outputFormat.flush();
                    commitIfNeeded();
                    reportWriteSuccess(batchRows);
                } catch (Throwable e) {
                    if (!isRowLevelDataError(e)) {
                        throwAsIoException(e);
                    }
                    handleRowLevelBatchFailure(
                            RowErrorPhase.PREPARE_COMMIT, checkpointId, batchRows, e);
                }
            }
            return Optional.empty();
        }

        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            commitIfNeeded();
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (rowErrorCollector.isPresent()) {
            synchronized (batchLock) {
                tryOpen();
                List<SeaTunnelRow> batchRows = swapPendingRowsLocked();
                try {
                    outputFormat.flush();
                    commitIfNeeded();
                    reportWriteSuccess(batchRows);
                } catch (Throwable e) {
                    if (!isRowLevelDataError(e)) {
                        throwAsIoException(e);
                    }
                    handleRowLevelBatchFailure(RowErrorPhase.CLOSE, null, batchRows, e);
                } finally {
                    outputFormat.close();
                }
            }
            return;
        }

        tryOpen();
        outputFormat.flush();
        try {
            Connection connection = connectionProvider.getConnection();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "unable to close JDBC sink write",
                    e);
        } finally {
            outputFormat.close();
        }
    }

    private void reportAndClearPendingRowsIfCommitted(boolean autoFlushed) throws IOException {
        if (pendingRows == null) {
            return;
        }
        if (autoFlushed) {
            if (markSuccessfulAutoFlushBoundaryIfNeeded()) {
                reportWriteSuccess(pendingRows);
                pendingRows.clear();
            }
        }
    }

    private boolean markSuccessfulAutoFlushBoundaryIfNeeded() throws IOException {
        if (jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit()) {
            return true;
        }
        if (!supportsSavepoints()) {
            logSavepointUnsupported();
            return false;
        }
        try {
            Connection connection = connectionProvider.getConnection();
            Savepoint previousSavepoint = lastSuccessfulBatchSavepoint;
            lastSuccessfulBatchSavepoint = connection.setSavepoint();
            transactionConnection = connection;
            releaseSavepointSilently(connection, previousSavepoint);
            return true;
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "set savepoint failed," + e.getMessage(),
                    e);
        }
    }

    private boolean supportsSavepoints() {
        if (supportsSavepoints != null) {
            return supportsSavepoints;
        }
        try {
            DatabaseMetaData metaData = connectionProvider.getConnection().getMetaData();
            supportsSavepoints = metaData != null && metaData.supportsSavepoints();
        } catch (SQLException e) {
            supportsSavepoints = false;
            log.warn(
                    "Failed to check JDBC savepoint support; fallback to full transaction rollback.",
                    e);
        }
        return supportsSavepoints;
    }

    private void logSavepointUnsupported() {
        if (savepointUnsupportedLogged) {
            return;
        }
        savepointUnsupportedLogged = true;
        log.warn(
                "JDBC driver does not support savepoints. Row-error handling will keep "
                        + "auto-flushed rows pending until checkpoint commit and fall back to full "
                        + "transaction rollback on row-level write failure. table={}",
                sinkTablePath);
    }

    // Releasing an old savepoint is a best-effort cleanup. Some drivers invalidate savepoints after
    // rollback/commit and should not fail the writer just because cleanup is no longer possible.
    private void releaseSavepointSilently(Connection connection, Savepoint savepoint) {
        if (savepoint == null) {
            return;
        }
        try {
            Connection currentConnection = connectionProvider.getConnection();
            if (currentConnection == transactionConnection) {
                currentConnection.releaseSavepoint(savepoint);
            }
        } catch (SQLException e) {
            log.debug("Failed to release JDBC savepoint after moving row-error batch boundary.", e);
        }
    }

    private void reportWriteSuccess(List<SeaTunnelRow> rows) throws IOException {
        if (!rowErrorCollector.isPresent() || rows == null || rows.isEmpty()) {
            return;
        }
        try {
            for (SeaTunnelRow row : rows) {
                rowErrorCollector.get().collectWriteSuccess(row);
            }
        } catch (Exception collectorEx) {
            throw toIOException(collectorEx);
        }
    }

    private List<SeaTunnelRow> swapPendingRowsLocked() {
        if (pendingRows == null || pendingRows.isEmpty()) {
            return Collections.emptyList();
        }
        List<SeaTunnelRow> batchRows = pendingRows;
        pendingRows = new ArrayList<>(Math.max(batchSize, 16));
        return batchRows;
    }

    private void handleRowLevelBatchFailure(
            RowErrorPhase phase, Long checkpointId, List<SeaTunnelRow> batchRows, Throwable error)
            throws IOException {
        IOException failure = null;
        try {
            for (SeaTunnelRow row : batchRows) {
                rowErrorCollector.get().collect(new RowErrorEvent(phase, checkpointId, row, error));
            }
        } catch (Exception collectorEx) {
            failure = toIOException(collectorEx);
        } finally {
            try {
                outputFormat.clearBatchSilently();
            } catch (Throwable clearEx) {
                failure = appendFailure(failure, clearEx);
            }
            try {
                rollbackIfNeeded();
            } catch (Throwable rollbackEx) {
                failure = appendFailure(failure, rollbackEx);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void rollbackIfNeeded() throws SQLException {
        Connection connection = connectionProvider.getConnection();
        if (connection.getAutoCommit()) {
            return;
        }
        if (lastSuccessfulBatchSavepoint != null && connection == transactionConnection) {
            connection.rollback(lastSuccessfulBatchSavepoint);
        } else {
            connection.rollback();
        }
        lastSuccessfulBatchSavepoint = null;
        transactionConnection = null;
    }

    private void commitIfNeeded() throws SQLException {
        Connection connection = connectionProvider.getConnection();
        if (!connection.getAutoCommit()) {
            if (transactionConnection != null && connection != transactionConnection) {
                throw new SQLException(
                        "Connection was silently swapped; "
                                + "uncommitted data from the previous connection may be lost.");
            }
            connection.commit();
            lastSuccessfulBatchSavepoint = null;
            transactionConnection = null;
        }
    }

    private void throwAsIoException(Throwable e) throws IOException {
        throw toIOException(e);
    }

    private IOException toIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof RuntimeException) {
            return new IOException(e);
        }
        return new IOException(e);
    }

    private IOException appendFailure(IOException current, Throwable next) {
        IOException nextException = toIOException(next);
        if (current == null) {
            return nextException;
        }
        current.addSuppressed(nextException);
        return current;
    }

    /**
     * Flushes buffered records when the engine delivers a timer-driven flush signal.
     *
     * <p>This action is registered only for the non-XA writer. Flush and commit failures are
     * propagated to fail the sink task instead of being deferred to the next checkpoint.
     */
    public void timerFlush() throws IOException {
        if (rowErrorCollector.isPresent()) {
            synchronized (batchLock) {
                tryOpen();
                outputFormat.checkFlushException();
                List<SeaTunnelRow> batchRows = swapPendingRowsLocked();
                try {
                    outputFormat.flush();
                    commitIfNeeded();
                    reportWriteSuccess(batchRows);
                } catch (Throwable e) {
                    if (!isRowLevelDataError(e)) {
                        throwAsIoException(e);
                    }
                    handleRowLevelBatchFailure(RowErrorPhase.FLUSH, null, batchRows, e);
                }
            }
            return;
        }

        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            commitIfNeeded();
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "timer flush commit failed: " + e.getMessage(),
                    e);
        }
    }
}
