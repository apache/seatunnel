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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class JdbcSinkWriter extends AbstractJdbcSinkWriter<ConnectionPoolManager> {
    private final Integer primaryKeyIndex;

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
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                connectionProvider,
                                jdbcSinkConfig,
                                tableSchema,
                                databaseTableSchema)
                        .commitOnFlush(commitOnFlush)
                        .build();
        context.registerFlushAction(this::timerFlush);
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

        tryOpen();
        outputFormat.writeRecord(element);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
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

    /**
     * Flushes buffered records when the engine delivers a timer-driven flush signal.
     *
     * <p>This action is registered only for the non-XA writer. Flush and commit failures are
     * propagated to fail the sink task instead of being deferred to the next checkpoint.
     */
    public void timerFlush() throws IOException {
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

    private void commitIfNeeded() throws SQLException {
        Connection connection = connectionProvider.getConnection();
        if (connection != null && !connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private void rollbackIfNeeded(String phase) {
        try {
            Connection connection = connectionProvider.getConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException rollbackException) {
            log.warn("Rollback jdbc sink writer failed during {}.", phase, rollbackException);
        }
    }
}
