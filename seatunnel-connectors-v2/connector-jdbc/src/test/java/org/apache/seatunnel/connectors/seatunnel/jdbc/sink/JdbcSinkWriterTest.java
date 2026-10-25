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

import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionValidationUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests JDBC sink writer helper behavior. */
class JdbcSinkWriterTest {

    @Test
    void testLegacyConstructorSignatureIsKept() throws Exception {
        Constructor<JdbcSinkWriter> constructor =
                JdbcSinkWriter.class.getConstructor(
                        TablePath.class,
                        JdbcDialect.class,
                        JdbcSinkConfig.class,
                        TableSchema.class,
                        TableSchema.class,
                        Integer.class);

        Assertions.assertNotNull(constructor);
    }

    @Test
    void testPendingRowsAreReportedAndClearedAfterAutoCommitAutoFlush() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        JdbcSinkWriter writer = createWriterWithRowErrorCollector(true, successCount);
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(writer, pendingRows);

        invokeReportAndClearPendingRowsIfCommitted(writer, true);

        Assertions.assertTrue(getPendingRows(writer).isEmpty());
        Assertions.assertEquals(1, successCount.get());
    }

    @Test
    void testTransactionalAutoFlushUsesSavepointWithoutCommit() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        WriterFixture fixture = createWriterFixture(false, successCount);
        JdbcSinkWriter writer = fixture.writer;
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(writer, pendingRows);

        invokeReportAndClearPendingRowsIfCommitted(writer, true);

        Assertions.assertTrue(getPendingRows(writer).isEmpty());
        Assertions.assertEquals(1, successCount.get());
        verify(fixture.connection, times(1)).setSavepoint();
        verify(fixture.connection, never()).commit();
    }

    @Test
    void testTransactionalAutoFlushKeepsPendingRowsWhenSavepointsUnsupported() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        WriterFixture fixture = createWriterFixture(false, false, successCount);
        JdbcSinkWriter writer = fixture.writer;
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(writer, pendingRows);

        invokeReportAndClearPendingRowsIfCommitted(writer, true);

        Assertions.assertEquals(1, getPendingRows(writer).size());
        Assertions.assertEquals(0, successCount.get());
        verify(fixture.connection, never()).setSavepoint();
        verify(fixture.connection, never()).commit();
    }

    @Test
    void testTransactionalRollbackUsesLastSuccessfulSavepoint() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        WriterFixture fixture = createWriterFixture(false, successCount);
        JdbcSinkWriter writer = fixture.writer;
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(writer, pendingRows);

        invokeReportAndClearPendingRowsIfCommitted(writer, true);
        invokeRollbackIfNeeded(writer);

        verify(fixture.connection, times(1)).rollback(fixture.savepoint);
        verify(fixture.connection, never()).rollback();
        Assertions.assertNull(getLastSuccessfulBatchSavepoint(writer));
    }

    @Test
    void testTransactionalCommitClearsSavepointBoundary() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        WriterFixture fixture = createWriterFixture(false, successCount);
        JdbcSinkWriter writer = fixture.writer;
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(writer, pendingRows);

        invokeReportAndClearPendingRowsIfCommitted(writer, true);
        invokeCommitIfNeeded(writer);

        verify(fixture.connection, times(1)).commit();
        Assertions.assertNull(getLastSuccessfulBatchSavepoint(writer));
    }

    @Test
    void testCommittedAutoFlushDoesNotCreateSavepoint() throws Exception {
        AtomicInteger successCount = new AtomicInteger();
        WriterFixture fixture = createWriterFixture(false, successCount);
        fixture.writer.commitOnFlush = true;
        List<SeaTunnelRow> pendingRows = new ArrayList<>();
        pendingRows.add(new SeaTunnelRow(new Object[] {1}));
        setPendingRows(fixture.writer, pendingRows);
        invokeReportAndClearPendingRowsIfCommitted(fixture.writer, true);
        Assertions.assertEquals(1, successCount.get());
        Assertions.assertTrue(getPendingRows(fixture.writer).isEmpty());
        verify(fixture.connection, never()).setSavepoint();
    }

    @Test
    void testCommitFailureIsNotClassifiedAsRowError() throws Exception {
        JdbcSinkWriter writer = createWriterWithRowErrorCollector(false);
        JdbcOutputFormat outputFormat = mock(JdbcOutputFormat.class);
        when(outputFormat.hasCommitFailed()).thenReturn(true);
        setOutputFormat(writer, outputFormat);
        Assertions.assertEquals(
                org.apache.seatunnel.api.common.error.RowErrorClassification.SYSTEM_ERROR,
                writer.classifyRowError(
                        new SQLException("commit failed", "23000"),
                        new SeaTunnelRow(new Object[] {1})));
    }

    @Test
    void testNonCollectorWriteDoesNotClearBatchOnRowLevelError() throws Exception {
        JdbcSinkWriter writer = createWriterWithoutRowErrorCollector();
        JdbcOutputFormat outputFormat = mock(JdbcOutputFormat.class);
        setOutputFormat(writer, outputFormat);
        setIsOpen(writer, true);
        JdbcConnectorException rowLevelError =
                new JdbcConnectorException(
                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                        "Writing records to JDBC failed.",
                        new SQLException("data too long", "22001"));
        doThrow(rowLevelError).when(outputFormat).writeRecord(any());

        JdbcConnectorException thrown =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> writer.write(new SeaTunnelRow(new Object[] {1})));

        Assertions.assertSame(rowLevelError, thrown);
        verify(outputFormat, never()).clearBatchSilently();
    }

    /** Verifies that Xugu pools use a validation query compatible with the driver. */
    @Test
    void testApplyConnectionValidationSetsXuguValidationQuery() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(JdbcConnectionValidationUtils.XUGU_DRIVER)
                        .url("jdbc:xugu://localhost:5138/SYSTEM")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertEquals(
                JdbcConnectionValidationUtils.XUGU_VALIDATION_QUERY,
                dataSource.getConnectionTestQuery());
        dataSource.close();
    }

    /** Verifies that other drivers keep Hikari's default validation behavior. */
    @Test
    void testApplyConnectionValidationKeepsDefaultDriverValidation() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName("org.postgresql.Driver")
                        .url("jdbc:postgresql://localhost:5432/test")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertNull(dataSource.getConnectionTestQuery());
        dataSource.close();
    }

    /** Oracle sink ignores user auto_commit=true at runtime to keep failed batches atomic. */
    @Test
    void testOracleSinkResourceManagerUsesManualCommit() {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(Mockito.mock(JdbcConnectionProvider.class));
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));

        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:oracle-auto-commit")
                        .autoCommit(true)
                        .build();
        JdbcSinkConfig jdbcSinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(jdbcConnectionConfig)
                        .simpleSql("INSERT INTO TEST_TABLE(ID) VALUES (?)")
                        .build();
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("ID", BasicType.INT_TYPE, 22L, false, null, "ID"))
                        .build();
        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        jdbcSinkConfig,
                        tableSchema,
                        tableSchema,
                        null,
                        true);

        MultiTableResourceManager<ConnectionPoolManager> resourceManager =
                writer.initMultiTableResourceManager(1, 1);

        try {
            ConnectionPoolManager connectionPoolManager =
                    resourceManager.getSharedResource().orElseThrow(AssertionError::new);
            Assertions.assertFalse(connectionPoolManager.getConnectionPool().isAutoCommit());
        } finally {
            resourceManager.close();
        }
    }

    /** Oracle single-table sink also ignores user auto_commit=true at runtime. */
    @Test
    void testOracleSinkSingleTableProviderUsesManualCommit() {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        ArgumentCaptor<JdbcConnectionConfig> configCaptor =
                ArgumentCaptor.forClass(JdbcConnectionConfig.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(configCaptor.capture()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));

        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:oracle-single-table")
                        .autoCommit(true)
                        .build();
        JdbcSinkConfig jdbcSinkConfig = buildJdbcSinkConfig(jdbcConnectionConfig);
        TableSchema tableSchema = buildTableSchema();

        new JdbcSinkWriter(
                null,
                Mockito.mock(SinkWriter.Context.class),
                dialect,
                jdbcSinkConfig,
                tableSchema,
                tableSchema,
                null,
                true);

        Assertions.assertTrue(jdbcConnectionConfig.isAutoCommit());
        Assertions.assertFalse(configCaptor.getValue().isAutoCommit());
    }

    /** Close must rollback, not commit, when a previous flush failure is already recorded. */
    @Test
    void testCloseShouldNotCommitAfterKnownFlushFailure() throws Exception {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.POSTGRESQL);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));
        Mockito.when(connectionProvider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        buildJdbcSinkConfig(
                                JdbcConnectionConfig.builder()
                                        .driverName(DummyDriver.class.getName())
                                        .url("jdbc:dummy:close-flush-failure")
                                        .autoCommit(false)
                                        .build()),
                        buildTableSchema(),
                        buildTableSchema(),
                        null,
                        true);
        JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat =
                Mockito.mock(JdbcOutputFormat.class);
        writer.outputFormat = outputFormat;
        writer.isOpen = true;
        Mockito.doThrow(new RuntimeException("previous flush failed"))
                .when(outputFormat)
                .checkFlushException();

        Assertions.assertThrows(JdbcConnectorException.class, writer::close);

        Mockito.verify(outputFormat, Mockito.never()).flush();
        Mockito.verify(connection, Mockito.never()).commit();
        Mockito.verify(connection).rollback();
        Mockito.verify(outputFormat).close();
    }

    /**
     * A batch_size-triggered flush must carry its own commit when checkpointing (and the engine
     * timer flush) is disabled, or a manual-commit connection such as Oracle would keep every
     * flushed batch in one unbounded transaction until close.
     */
    @Test
    void testBatchSizeFlushCommitsWhenCheckpointDisabled() throws Exception {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));
        Mockito.when(connectionProvider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:batch-flush-commit")
                        .autoCommit(true)
                        .batchSize(2)
                        .batchIntervalMs(0)
                        .build();
        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        buildJdbcSinkConfig(connectionConfig),
                        buildTableSchema(),
                        buildTableSchema(),
                        null,
                        false);
        Assertions.assertTrue(writer.commitOnFlush);

        CountingExecutor executor = new CountingExecutor();
        writer.outputFormat =
                new JdbcOutputFormat<>(connectionProvider, connectionConfig, () -> executor, true);

        // First record stays buffered: no flush and no commit yet.
        writer.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertEquals(0, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.never()).commit();

        // Second record hits batch_size: the internal flush must commit without any
        // prepareCommit, timer flush or close call.
        writer.write(new SeaTunnelRow(new Object[] {2}));
        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.times(1)).commit();
        Mockito.verify(connection, Mockito.never()).rollback();
    }

    /** With checkpointing enabled the commit boundary stays at prepareCommit. */
    @Test
    void testBatchSizeFlushDoesNotCommitWhenCheckpointEnabled() throws Exception {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));
        Mockito.when(connectionProvider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:batch-flush-no-commit")
                        .autoCommit(true)
                        .batchSize(2)
                        .batchIntervalMs(0)
                        .build();
        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        buildJdbcSinkConfig(connectionConfig),
                        buildTableSchema(),
                        buildTableSchema(),
                        null,
                        true);
        Assertions.assertFalse(writer.commitOnFlush);

        CountingExecutor executor = new CountingExecutor();
        writer.outputFormat =
                new JdbcOutputFormat<>(connectionProvider, connectionConfig, () -> executor, false);

        writer.write(new SeaTunnelRow(new Object[] {1}));
        writer.write(new SeaTunnelRow(new Object[] {2}));

        Assertions.assertEquals(1, executor.executeBatchCalls);
        Mockito.verify(connection, Mockito.never()).commit();
    }

    private static JdbcSinkConfig buildJdbcSinkConfig(JdbcConnectionConfig jdbcConnectionConfig) {
        return JdbcSinkConfig.builder()
                .jdbcConnectionConfig(jdbcConnectionConfig)
                .simpleSql("INSERT INTO TEST_TABLE(ID) VALUES (?)")
                .build();
    }

    private static TableSchema buildTableSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("ID", BasicType.INT_TYPE, 22L, false, null, "ID"))
                .build();
    }

    private static class CountingExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private int executeBatchCalls;

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {}

        @Override
        public void executeBatch() {
            executeBatchCalls++;
        }

        @Override
        public void closeStatements() {}
    }

    public static class DummyDriver implements Driver {
        @Override
        public Connection connect(String url, Properties info) {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:dummy:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }

    private static JdbcSinkWriter createWriterWithRowErrorCollector(boolean autoCommit) {
        return createWriterWithRowErrorCollector(autoCommit, new AtomicInteger());
    }

    private static JdbcSinkWriter createWriterWithRowErrorCollector(
            boolean autoCommit, AtomicInteger successCount) {
        return createWriterFixture(autoCommit, successCount).writer;
    }

    private static WriterFixture createWriterFixture(
            boolean autoCommit, AtomicInteger successCount) {
        return createWriterFixture(autoCommit, true, successCount);
    }

    private static WriterFixture createWriterFixture(
            boolean autoCommit, boolean supportsSavepoints, AtomicInteger successCount) {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().batchSize(100).autoCommit(autoCommit).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .database("test_db")
                        .table("test_table")
                        .build();
        JdbcDialect dialect = mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        Savepoint savepoint = mock(Savepoint.class);
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        RowErrorCollector rowErrorCollector =
                new RowErrorCollector() {
                    @Override
                    public void collect(RowErrorEvent event) {}

                    @Override
                    public void collectWriteSuccess(SeaTunnelRow row) {
                        successCount.incrementAndGet();
                    }
                };
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 10L, false, null, ""))
                        .build();

        when(context.getRowErrorCollector()).thenReturn(Optional.of(rowErrorCollector));
        when(dialect.getJdbcConnectionProvider(connectionConfig)).thenReturn(connectionProvider);
        try {
            when(connectionProvider.getConnection()).thenReturn(connection);
            when(connection.getAutoCommit()).thenReturn(autoCommit);
            when(connection.getMetaData()).thenReturn(metaData);
            when(metaData.supportsSavepoints()).thenReturn(supportsSavepoints);
            when(connection.setSavepoint()).thenReturn(savepoint);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to mock JDBC connection", e);
        }
        when(dialect.getInsertIntoStatement(anyString(), anyString(), any()))
                .thenReturn("insert into test_table(id) values(?)");

        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        TablePath.of("test_db", "test_table"),
                        context,
                        dialect,
                        sinkConfig,
                        schema,
                        schema,
                        null);
        return new WriterFixture(writer, connection, savepoint);
    }

    private static JdbcSinkWriter createWriterWithoutRowErrorCollector() {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().batchSize(100).autoCommit(false).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .database("test_db")
                        .table("test_table")
                        .build();
        JdbcDialect dialect = mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 10L, false, null, ""))
                        .build();

        when(dialect.getJdbcConnectionProvider(connectionConfig)).thenReturn(connectionProvider);
        when(dialect.getInsertIntoStatement(anyString(), anyString(), any()))
                .thenReturn("insert into test_table(id) values(?)");

        return new JdbcSinkWriter(
                TablePath.of("test_db", "test_table"),
                null,
                dialect,
                sinkConfig,
                schema,
                schema,
                null);
    }

    private static final class WriterFixture {
        private final JdbcSinkWriter writer;
        private final Connection connection;
        private final Savepoint savepoint;

        private WriterFixture(JdbcSinkWriter writer, Connection connection, Savepoint savepoint) {
            this.writer = writer;
            this.connection = connection;
            this.savepoint = savepoint;
        }
    }

    private static void invokeReportAndClearPendingRowsIfCommitted(
            JdbcSinkWriter writer, boolean autoFlushed) throws Exception {
        Method method =
                JdbcSinkWriter.class.getDeclaredMethod(
                        "reportAndClearPendingRowsIfCommitted", boolean.class);
        method.setAccessible(true);
        method.invoke(writer, autoFlushed);
    }

    private static void invokeRollbackIfNeeded(JdbcSinkWriter writer) throws Exception {
        Method method = JdbcSinkWriter.class.getDeclaredMethod("rollbackIfNeeded");
        method.setAccessible(true);
        method.invoke(writer);
    }

    private static void invokeCommitIfNeeded(JdbcSinkWriter writer) throws Exception {
        Method method = JdbcSinkWriter.class.getDeclaredMethod("commitIfNeeded");
        method.setAccessible(true);
        method.invoke(writer);
    }

    private static void setPendingRows(JdbcSinkWriter writer, List<SeaTunnelRow> pendingRows)
            throws Exception {
        Field field = JdbcSinkWriter.class.getDeclaredField("pendingRows");
        field.setAccessible(true);
        field.set(writer, pendingRows);
    }

    private static void setOutputFormat(JdbcSinkWriter writer, JdbcOutputFormat outputFormat)
            throws Exception {
        Field field = AbstractJdbcSinkWriter.class.getDeclaredField("outputFormat");
        field.setAccessible(true);
        field.set(writer, outputFormat);
    }

    private static void setIsOpen(JdbcSinkWriter writer, boolean isOpen) throws Exception {
        Field field = AbstractJdbcSinkWriter.class.getDeclaredField("isOpen");
        field.setAccessible(true);
        field.set(writer, isOpen);
    }

    @SuppressWarnings("unchecked")
    private static List<SeaTunnelRow> getPendingRows(JdbcSinkWriter writer) throws Exception {
        Field field = JdbcSinkWriter.class.getDeclaredField("pendingRows");
        field.setAccessible(true);
        return (List<SeaTunnelRow>) field.get(writer);
    }

    private static Savepoint getLastSuccessfulBatchSavepoint(JdbcSinkWriter writer)
            throws Exception {
        Field field = JdbcSinkWriter.class.getDeclaredField("lastSuccessfulBatchSavepoint");
        field.setAccessible(true);
        return (Savepoint) field.get(writer);
    }
}
