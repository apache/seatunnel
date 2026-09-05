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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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
