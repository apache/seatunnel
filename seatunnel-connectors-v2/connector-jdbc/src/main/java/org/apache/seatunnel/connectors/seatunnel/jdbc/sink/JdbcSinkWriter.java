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
    // Flush sequence of the shared transaction when lastSuccessfulBatchSavepoint was set, so a
    // rollback to it can tell whether another writer on the same connection flushed afterwards.
    private long lastSuccessfulBatchSavepointSequence;
    // Set once rows were reported as written at a savepoint boundary but are not committed yet. A
    // full rollback then discards rows that were already reported, so it must not count as
    // reported work.
    private boolean reportedUncommittedRows;
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
        this(
                sinkTablePath,
                context,
                dialect,
                jdbcSinkConfig,
                tableSchema,
                databaseTableSchema,
                primaryKeyIndex,
                true);
    }

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            SinkWriter.Context context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            Integer primaryKeyIndex,
            boolean checkpointEnabled) {
        this.sinkTablePath = sinkTablePath;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.databaseTableSchema = databaseTableSchema;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.primaryKeyIndex = primaryKeyIndex;
        // Without checkpointing there is no prepareCommit boundary to commit manual-commit
        // connections (for example Oracle, which is forced to manual commit above), so every
        // successful batch flush must carry its own commit or flushed rows stay in one unbounded
        // transaction until close. With checkpointing enabled the commit boundary stays at
        // prepareCommit to keep the existing checkpoint semantics.
        this.commitOnFlush = !checkpointEnabled;
        this.connectionProvider = dialect.getJdbcConnectionProvider(resolveSinkConnectionConfig());
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
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .commitOnFlush(commitOnFlush)
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
        ds.setAutoCommit(resolveSinkAutoCommit());
        applyConnectionValidation(ds, jdbcSinkConfig.getJdbcConnectionConfig());
        jdbcSinkConfig.getJdbcConnectionConfig().getProperties().forEach(ds::addDataSourceProperty);
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    private boolean resolveSinkAutoCommit() {
        // Oracle may partially commit a failed JDBC batch when auto-commit is enabled. Keep the
        // batch atomic there so the original data error is not masked by a later duplicate-key
        // error.
        if (DatabaseIdentifier.ORACLE.equals(dialect.dialectName())) {
            return false;
        }
        return jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit();
    }

    private JdbcConnectionConfig resolveSinkConnectionConfig() {
        JdbcConnectionConfig connectionConfig = jdbcSinkConfig.getJdbcConnectionConfig();
        if (!DatabaseIdentifier.ORACLE.equals(dialect.dialectName())) {
            return connectionConfig;
        }

        return copyConnectionConfig(connectionConfig, false);
    }

    private JdbcConnectionConfig copyConnectionConfig(
            JdbcConnectionConfig connectionConfig, boolean autoCommit) {
        JdbcConnectionConfig.Builder builder =
                JdbcConnectionConfig.builder()
                        .url(connectionConfig.getUrl())
                        .driverName(connectionConfig.getDriverName())
                        .compatibleMode(connectionConfig.getCompatibleMode())
                        .connectionCheckTimeoutSeconds(
                                connectionConfig.getConnectionCheckTimeoutSeconds())
                        .maxRetries(connectionConfig.getMaxRetries())
                        .query(connectionConfig.getQuery())
                        .autoCommit(autoCommit)
                        .batchSize(connectionConfig.getBatchSize())
                        .batchIntervalMs(connectionConfig.getBatchIntervalMs())
                        .isExactlyOnce(connectionConfig.isExactlyOnce())
                        .xaDataSourceClassName(connectionConfig.getXaDataSourceClassName())
                        .decimalTypeNarrowing(connectionConfig.isDecimalTypeNarrowing())
                        .intTypeNarrowing(connectionConfig.isIntTypeNarrowing())
                        .handleBlobAsString(connectionConfig.isHandleBlobAsString())
                        .maxCommitAttempts(connectionConfig.getMaxCommitAttempts())
                        .transactionTimeoutSec(
                                connectionConfig.getTransactionTimeoutSec().orElse(-1))
                        .socketTimeoutMs(connectionConfig.getSocketTimeoutMs())
                        .connectTimeoutMs(connectionConfig.getConnectTimeoutMs())
                        .properties(connectionConfig.getProperties())
                        .useKerberos(connectionConfig.isUseKerberos())
                        .kerberosPrincipal(connectionConfig.getKerberosPrincipal())
                        .kerberosKeytabPath(connectionConfig.getKerberosKeytabPath())
                        .krb5Path(connectionConfig.getKrb5Path())
                        .dialect(connectionConfig.getDialect())
                        .region(connectionConfig.getRegion())
                        .accessKeyId(connectionConfig.getAccessKeyId())
                        .secretAccessKey(connectionConfig.getSecretAccessKey());
        connectionConfig.getUsername().ifPresent(builder::username);
        connectionConfig.getPassword().ifPresent(builder::password);
        return builder.build();
    }

    /**
     * Configures pool-level validation for JDBC drivers that cannot pass Hikari's default
     * Connection.isValid(timeout) probe.
     */
    static void applyConnectionValidation(
            HikariDataSource dataSource, JdbcConnectionConfig jdbcConnectionConfig) {
        JdbcConnectionValidationUtils.getConnectionValidationQuery(jdbcConnectionConfig)
                .ifPresent(dataSource::setConnectionTestQuery);
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
                        .commitOnFlush(commitOnFlush)
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
                    // Report the failed rows of the batch, then continue. Rows written again by
                    // the replay sit behind a savepoint boundary, like an auto-flushed batch.
                    List<SeaTunnelRow> batchRows = swapPendingRowsLocked();
                    reportWriteSuccess(
                            handleRowLevelBatchFailure(RowErrorPhase.WRITE, null, batchRows, e));
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
        // A failed commit has an unknown durable outcome, even for SQLState 22/23.
        if (outputFormat.hasCommitFailed()) {
            return false;
        }
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
                    List<SeaTunnelRow> replayedRows =
                            handleRowLevelBatchFailure(
                                    RowErrorPhase.PREPARE_COMMIT, checkpointId, batchRows, e);
                    try {
                        // A handled row error must still finish or reject the current checkpoint.
                        commitIfNeeded();
                    } catch (Exception commitError) {
                        rollbackIfNeeded("prepare commit after row error");
                        throwAsIoException(commitError);
                    }
                    reportWriteSuccess(replayedRows);
                }
            }
            return Optional.empty();
        }

        tryOpen();
        try {
            outputFormat.checkFlushException();
            outputFormat.flush();
            commitIfNeeded();
        } catch (SQLException e) {
            rollbackIfNeeded("prepare commit");
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        } catch (IOException e) {
            rollbackIfNeeded("prepare commit");
            throw e;
        } catch (Exception e) {
            rollbackIfNeeded("prepare commit");
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED, "prepare commit failed", e);
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
                        // Roll back before the connection is closed below, as the other close
                        // path does. Some drivers (for example Oracle) commit an open transaction
                        // on close, which would commit a partial result.
                        rollbackIfNeeded("close");
                        throwAsIoException(e);
                    }
                    List<SeaTunnelRow> replayedRows =
                            handleRowLevelBatchFailure(RowErrorPhase.CLOSE, null, batchRows, e);
                    try {
                        // A handled close error must commit preserved rows or reject lost work.
                        commitIfNeeded();
                    } catch (Exception commitError) {
                        rollbackIfNeeded("close after row error");
                        throwAsIoException(commitError);
                    }
                    reportWriteSuccess(replayedRows);
                } finally {
                    outputFormat.close();
                }
            }
            return;
        }

        tryOpen();
        try {
            outputFormat.checkFlushException();
            outputFormat.flush();
            commitIfNeeded();
        } catch (Exception e) {
            rollbackIfNeeded("close");
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
        if (commitOnFlush || resolveSinkAutoCommit()) {
            lastSuccessfulBatchSavepoint = null;
            return true;
        }
        if (!supportsSavepoints()) {
            logSavepointUnsupported();
            return false;
        }
        try {
            moveSavepointBoundary(connectionProvider.getConnection());
            reportedUncommittedRows = true;
            return true;
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "set savepoint failed," + e.getMessage(),
                    e);
        }
    }

    /**
     * Replaces the savepoint boundary with a new savepoint at the current end of the transaction.
     *
     * <p>The previous savepoint is released first. In databases such as SQLite and PostgreSQL,
     * releasing a savepoint also destroys every savepoint taken after it, so releasing it after
     * taking the new one would destroy the new one too, and a later rollback to it would fail.
     */
    private void moveSavepointBoundary(Connection connection) throws SQLException {
        Savepoint previousSavepoint = lastSuccessfulBatchSavepoint;
        lastSuccessfulBatchSavepoint = null;
        releaseSavepointSilently(connection, previousSavepoint);
        lastSuccessfulBatchSavepoint = connection.setSavepoint();
        lastSuccessfulBatchSavepointSequence = outputFormat.currentFlushSequence();
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
            connection.releaseSavepoint(savepoint);
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

    /**
     * Handles a batch that failed with a row-level data error (SQLState class 22 or 23).
     *
     * <p>The batch is cleared from the statement and rolled back. When {@link #canReplayRows}
     * allows it, its rows are then written again one by one, so that only the rows that fail on
     * their own are reported to the row-error collector. Otherwise every row of the batch is
     * reported with the batch error, as no row of the batch can be told apart from the others.
     *
     * @param phase the writer phase whose flush failed
     * @param checkpointId the checkpoint of the failed phase, or null outside prepareCommit
     * @param batchRows the rows of the failed batch, in write order
     * @param error the error of the failed batch
     * @return the rows written again by the replay; the caller reports them as written once its
     *     phase is complete. Empty when the batch was not replayed.
     */
    private List<SeaTunnelRow> handleRowLevelBatchFailure(
            RowErrorPhase phase, Long checkpointId, List<SeaTunnelRow> batchRows, Throwable error)
            throws IOException {
        Throwable clearError = null;
        Throwable rollbackError = null;
        boolean rolledBack = false;
        try {
            outputFormat.clearBatchSilently();
        } catch (Throwable clearEx) {
            clearError = clearEx;
        }
        try {
            rolledBack = rollbackIfNeeded();
        } catch (Throwable rollbackEx) {
            rollbackError = rollbackEx;
        }
        if (clearError == null && rollbackError == null && rolledBack && canReplayRows(batchRows)) {
            outputFormat.resetAfterRowError();
            return replayRowByRow(phase, checkpointId, batchRows);
        }
        IOException failure = null;
        try {
            for (SeaTunnelRow row : batchRows) {
                rowErrorCollector.get().collect(new RowErrorEvent(phase, checkpointId, row, error));
            }
        } catch (Exception collectorEx) {
            failure = toIOException(collectorEx);
        }
        if (clearError != null) {
            failure = appendFailure(failure, clearError);
        }
        if (rollbackError != null) {
            failure = appendFailure(failure, rollbackError);
        }
        if (failure != null) {
            throw failure;
        }
        outputFormat.resetAfterRowError();
        return Collections.emptyList();
    }

    /**
     * Whether the rows of a failed batch can be written again one by one. The caller has already
     * rolled the batch back on a manual-commit connection, so none of its rows is in the database.
     * With auto-commit, part of a failed batch can already be committed, so its rows are never
     * written again. In addition:
     *
     * <ul>
     *   <li>the batch has more than one row; a single row is the failing row itself;
     *   <li>no flushed work was lost on the shared transaction. A commit is then refused anyway,
     *       and the job recovers from the last checkpoint;
     *   <li>one failing row can be undone without the rows before it: each flush commits when
     *       checkpointing is disabled, otherwise the driver must support savepoints.
     * </ul>
     */
    private boolean canReplayRows(List<SeaTunnelRow> batchRows) {
        return batchRows.size() > 1
                && !outputFormat.isPendingWorkLost()
                && (commitOnFlush || supportsSavepoints());
    }

    /**
     * Writes the rows of a rolled-back batch again, one row per flush, in their original order. A
     * row that fails with a row-level data error is rolled back alone (to the savepoint taken
     * before it, or by its own transaction when each flush commits) and reported with its own
     * error. Any other failure rolls the transaction back and fails the writer.
     *
     * <p>At the end, the written rows get a savepoint boundary like an auto-flushed batch, so a
     * later row-level error cannot roll them back without failing the checkpoint interval.
     *
     * @return the rows written again
     */
    private List<SeaTunnelRow> replayRowByRow(
            RowErrorPhase phase, Long checkpointId, List<SeaTunnelRow> batchRows)
            throws IOException {
        log.warn(
                "A JDBC batch of {} rows failed with a row-level data error. Writing its rows"
                        + " again one by one to find the failing rows. table={}",
                batchRows.size(),
                sinkTablePath);
        List<SeaTunnelRow> writtenRows = new ArrayList<>(batchRows.size());
        for (SeaTunnelRow row : batchRows) {
            try {
                setReplaySavepointIfNeeded();
                outputFormat.writeRecord(row);
                outputFormat.flush();
                writtenRows.add(row);
            } catch (Throwable rowError) {
                if (!isRowLevelDataError(rowError)) {
                    rollbackIfNeeded("row-by-row replay");
                    throwAsIoException(rowError);
                }
                discardReplayedRow(rowError);
                try {
                    rowErrorCollector
                            .get()
                            .collect(new RowErrorEvent(phase, checkpointId, row, rowError));
                } catch (Exception collectorEx) {
                    rollbackIfNeeded("row-by-row replay");
                    throwAsIoException(collectorEx);
                }
                outputFormat.resetAfterRowError();
            }
        }
        if (!writtenRows.isEmpty()) {
            try {
                markSuccessfulAutoFlushBoundaryIfNeeded();
            } catch (RuntimeException boundaryError) {
                rollbackIfNeeded("row-by-row replay");
                throwAsIoException(boundaryError);
            }
        }
        return writtenRows;
    }

    /**
     * Takes the savepoint that a failing replayed row is rolled back to. Not needed when each flush
     * commits, because the failing row is then the only uncommitted work.
     */
    private void setReplaySavepointIfNeeded() throws SQLException {
        if (commitOnFlush) {
            return;
        }
        moveSavepointBoundary(connectionProvider.getConnection());
    }

    /** Clears and rolls back one failed replayed row; fails the writer if that is not possible. */
    private void discardReplayedRow(Throwable rowError) throws IOException {
        IOException failure = null;
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
        if (failure != null) {
            failure.addSuppressed(rowError);
            rollbackIfNeeded("row-by-row replay");
            throw failure;
        }
    }

    /**
     * Rolls back the failed batch: to the last savepoint boundary when there is one, otherwise the
     * whole transaction.
     *
     * @return false when the connection is in auto-commit mode and nothing was rolled back
     */
    private boolean rollbackIfNeeded() throws SQLException {
        Connection connection = connectionProvider.getConnection();
        if (connection.getAutoCommit()) {
            return false;
        }
        if (lastSuccessfulBatchSavepoint != null) {
            // Batches up to the savepoint stay in the open transaction, still uncommitted.
            connection.rollback(lastSuccessfulBatchSavepoint);
            outputFormat.markRolledBackToSavepoint(lastSuccessfulBatchSavepointSequence);
        } else {
            connection.rollback();
            // The failed batch is reported to the row-error collector or written again. Rows this
            // writer already reported as written, and batches other writers flushed on the same
            // connection, are lost with it; the state records that.
            outputFormat.markRolledBack(connection, !reportedUncommittedRows);
            reportedUncommittedRows = false;
        }
        lastSuccessfulBatchSavepoint = null;
        return true;
    }

    private void commitIfNeeded() throws SQLException {
        Connection connection = connectionProvider.getConnection();
        outputFormat.checkUncommittedBatchesOn(connection);
        if (!connection.getAutoCommit()) {
            try {
                connection.commit();
            } catch (SQLException | RuntimeException commitError) {
                // Retrying only commit cannot replay batches discarded by the database. Record
                // the failure before cleanup, even if rollback fails or the SQLState is 22/23.
                outputFormat.markCommitFailed(commitError);
                throw commitError;
            }
            lastSuccessfulBatchSavepoint = null;
            reportedUncommittedRows = false;
            outputFormat.markCommitted(connection);
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
                    reportWriteSuccess(
                            handleRowLevelBatchFailure(RowErrorPhase.FLUSH, null, batchRows, e));
                }
            }
            return;
        }

        tryOpen();
        try {
            outputFormat.checkFlushException();
            outputFormat.flush();
            commitIfNeeded();
        } catch (SQLException e) {
            rollbackIfNeeded("timer flush");
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "timer flush commit failed: " + e.getMessage(),
                    e);
        } catch (IOException e) {
            rollbackIfNeeded("timer flush");
            throw e;
        } catch (Exception e) {
            rollbackIfNeeded("timer flush");
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED, "timer flush failed", e);
        }
    }

    private void rollbackIfNeeded(String phase) {
        try {
            Connection connection = connectionProvider.getConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
                // The rows of this failed phase are not reported anywhere, so if flushed work was
                // discarded, no writer on this connection may commit this interval any more.
                outputFormat.markRolledBack(connection, false);
                reportedUncommittedRows = false;
            }
        } catch (SQLException | RuntimeException rollbackException) {
            // Best effort: the caller is already failing with the original error. A pooled
            // provider can also throw a RuntimeException when it cannot hand out a connection.
            log.warn("Rollback jdbc sink writer failed during {}.", phase, rollbackException);
        }
    }
}
