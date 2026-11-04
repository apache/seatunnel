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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers selected-column metadata, transaction ownership, and failed-batch replay safety.
 *
 * <p>The driver boundary is mocked while the executor and its metadata lifecycle remain real.
 */
class SqlserverBulkCopyBatchStatementExecutorTest {
    private final List<Column> columns =
            Arrays.asList(
                    PhysicalColumn.of("payload", BasicType.STRING_TYPE, 20L, true, null, null),
                    PhysicalColumn.of("id", BasicType.INT_TYPE, 10L, false, null, null));

    @Test
    void testProjectedRowsAndMetadataSurviveQueryResourceClosure() throws Exception {
        SQLServerConnection connection = metadataConnection();
        List<Object[]> observedRows = new ArrayList<>();
        try (MockedConstruction<SQLServerBulkCopy> mocked =
                Mockito.mockConstruction(
                        SQLServerBulkCopy.class,
                        (bulk, context) ->
                                Mockito.doAnswer(
                                                invocation -> {
                                                    ISQLServerBulkData data =
                                                            invocation.getArgument(0);
                                                    Assertions.assertEquals(
                                                            Arrays.asList(1, 2),
                                                            new ArrayList<>(
                                                                    data.getColumnOrdinals()));
                                                    Assertions.assertEquals(
                                                            "payload", data.getColumnName(1));
                                                    Assertions.assertEquals(
                                                            Types.VARCHAR, data.getColumnType(1));
                                                    Assertions.assertEquals(
                                                            20, data.getPrecision(1));
                                                    Assertions.assertEquals(0, data.getScale(1));
                                                    Assertions.assertEquals(
                                                            "id", data.getColumnName(2));
                                                    while (data.next()) {
                                                        observedRows.add(data.getRowData().clone());
                                                    }
                                                    return null;
                                                })
                                        .when(bulk)
                                        .writeToServer(Mockito.any(ISQLServerBulkData.class)))) {
            SqlserverBulkCopyBatchStatementExecutor executor =
                    new SqlserverBulkCopyBatchStatementExecutor("[master].[dbo].[target]", columns);
            executor.prepareStatements(connection);
            executor.addToBatch(new SeaTunnelRow(new Object[] {null, 7}));
            executor.executeBatch();
            executor.executeBatch();

            Assertions.assertEquals(1, observedRows.size());
            Assertions.assertArrayEquals(new Object[] {null, 7}, observedRows.get(0));
            Assertions.assertEquals(1, mocked.constructed().size());
            SQLServerBulkCopy bulk = mocked.constructed().get(0);
            Mockito.verify(bulk).addColumnMapping(1, "payload");
            Mockito.verify(bulk).addColumnMapping(2, "id");
            ArgumentCaptor<SQLServerBulkCopyOptions> options =
                    ArgumentCaptor.forClass(SQLServerBulkCopyOptions.class);
            Mockito.verify(bulk).setBulkCopyOptions(options.capture());
            Assertions.assertTrue(options.getValue().isKeepNulls());
            Assertions.assertTrue(options.getValue().isKeepIdentity());
            Assertions.assertTrue(options.getValue().isCheckConstraints());
            Assertions.assertTrue(options.getValue().isFireTriggers());
            Assertions.assertFalse(options.getValue().isUseInternalTransaction());
            Mockito.verify(connection, Mockito.never()).setAutoCommit(Mockito.anyBoolean());
            Mockito.verify(connection, Mockito.never()).commit();
            Mockito.verify(connection, Mockito.never()).rollback();
        }
    }

    @Test
    void testSqlExceptionRetainsBatchAcrossReconnectAndClearDiscardsIt() throws Exception {
        SQLServerConnection connection = metadataConnection();
        AtomicInteger attempts = new AtomicInteger();
        List<Object[]> observedRows = new ArrayList<>();
        SQLException failure = new SQLException("temporary bulk failure", "08006");
        try (MockedConstruction<SQLServerBulkCopy> mocked =
                Mockito.mockConstruction(
                        SQLServerBulkCopy.class,
                        (bulk, context) ->
                                Mockito.doAnswer(
                                                invocation -> {
                                                    if (attempts.incrementAndGet() == 1) {
                                                        throw failure;
                                                    }
                                                    ISQLServerBulkData data =
                                                            invocation.getArgument(0);
                                                    while (data.next()) {
                                                        observedRows.add(data.getRowData().clone());
                                                    }
                                                    return null;
                                                })
                                        .when(bulk)
                                        .writeToServer(Mockito.any(ISQLServerBulkData.class)))) {
            SqlserverBulkCopyBatchStatementExecutor executor =
                    new SqlserverBulkCopyBatchStatementExecutor("[master].[dbo].[target]", columns);
            executor.prepareStatements(connection);
            executor.addToBatch(new SeaTunnelRow(new Object[] {"retained", 8}));
            Assertions.assertSame(
                    failure, Assertions.assertThrows(SQLException.class, executor::executeBatch));
            executor.closeStatements();
            Assertions.assertEquals(1, attempts.get());
            executor.prepareStatements(metadataConnection());
            executor.executeBatch();
            Assertions.assertEquals(1, observedRows.size());
            Assertions.assertArrayEquals(new Object[] {"retained", 8}, observedRows.get(0));
            executor.addToBatch(new SeaTunnelRow(new Object[] {"discarded", 9}));
            executor.clearBatch();
            executor.executeBatch();
            executor.closeStatements();
            Assertions.assertEquals(1, observedRows.size());
            Assertions.assertEquals(2, attempts.get());
        }
    }

    @Test
    void testOffsetDateTimeUsesJdbc42MetadataAndDestinationScale() throws Exception {
        SQLServerConnection connection = Mockito.mock(SQLServerConnection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet result = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(result);
        Mockito.when(result.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnName(1)).thenReturn("offset_value");
        Mockito.when(metadata.getColumnType(1)).thenReturn(-155);
        Mockito.when(metadata.getPrecision(1)).thenReturn(34);
        Mockito.when(metadata.getScale(1)).thenReturn(7);
        OffsetDateTime value = OffsetDateTime.parse("2026-01-02T03:04:05.1234567+05:45");
        AtomicInteger observed = new AtomicInteger();
        try (MockedConstruction<SQLServerBulkCopy> mocked =
                Mockito.mockConstruction(
                        SQLServerBulkCopy.class,
                        (bulk, context) ->
                                Mockito.doAnswer(
                                                invocation -> {
                                                    ISQLServerBulkData data =
                                                            invocation.getArgument(0);
                                                    Assertions.assertEquals(
                                                            Types.TIMESTAMP_WITH_TIMEZONE,
                                                            data.getColumnType(1));
                                                    Assertions.assertEquals(
                                                            34, data.getPrecision(1));
                                                    Assertions.assertEquals(7, data.getScale(1));
                                                    Assertions.assertTrue(data.next());
                                                    Assertions.assertEquals(
                                                            value, data.getRowData()[0]);
                                                    Assertions.assertFalse(data.next());
                                                    observed.incrementAndGet();
                                                    return null;
                                                })
                                        .when(bulk)
                                        .writeToServer(Mockito.any(ISQLServerBulkData.class)))) {
            SqlserverBulkCopyBatchStatementExecutor executor =
                    new SqlserverBulkCopyBatchStatementExecutor(
                            "dbo.target",
                            Arrays.asList(
                                    PhysicalColumn.of(
                                            "offset_value",
                                            BasicType.OFFSET_DATE_TIME_TYPE,
                                            34L,
                                            true,
                                            null,
                                            null)));
            executor.prepareStatements(connection);
            executor.addToBatch(new SeaTunnelRow(new Object[] {value}));
            executor.executeBatch();
            Assertions.assertEquals(1, observed.get());
            Mockito.verify(mocked.constructed().get(0)).close();
        }
    }

    @Test
    void testNativeTimeUsesTimestampWithoutLosingFractionalSeconds() throws Exception {
        SQLServerConnection connection = Mockito.mock(SQLServerConnection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet result = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(result);
        Mockito.when(result.getMetaData()).thenReturn(metadata);
        Mockito.when(metadata.getColumnName(1)).thenReturn("time_value");
        Mockito.when(metadata.getColumnType(1)).thenReturn(Types.TIME);
        Mockito.when(metadata.getPrecision(1)).thenReturn(16);
        Mockito.when(metadata.getScale(1)).thenReturn(7);
        LocalTime value = LocalTime.parse("03:04:05.1234567");
        AtomicInteger observed = new AtomicInteger();
        try (MockedConstruction<SQLServerBulkCopy> mocked =
                Mockito.mockConstruction(
                        SQLServerBulkCopy.class,
                        (bulk, context) ->
                                Mockito.doAnswer(
                                                invocation -> {
                                                    ISQLServerBulkData data =
                                                            invocation.getArgument(0);
                                                    Assertions.assertEquals(
                                                            Types.TIME, data.getColumnType(1));
                                                    Assertions.assertEquals(7, data.getScale(1));
                                                    Assertions.assertTrue(data.next());
                                                    Timestamp adapted =
                                                            Assertions.assertInstanceOf(
                                                                    Timestamp.class,
                                                                    data.getRowData()[0]);
                                                    Assertions.assertEquals(
                                                            value,
                                                            adapted.toLocalDateTime()
                                                                    .toLocalTime());
                                                    Assertions.assertEquals(
                                                            123456700, adapted.getNanos());
                                                    Assertions.assertTrue(data.next());
                                                    Assertions.assertNull(data.getRowData()[0]);
                                                    Assertions.assertFalse(data.next());
                                                    observed.incrementAndGet();
                                                    return null;
                                                })
                                        .when(bulk)
                                        .writeToServer(Mockito.any(ISQLServerBulkData.class)))) {
            SqlserverBulkCopyBatchStatementExecutor executor =
                    new SqlserverBulkCopyBatchStatementExecutor(
                            "dbo.target",
                            Arrays.asList(
                                    PhysicalColumn.of(
                                            "time_value",
                                            LocalTimeType.LOCAL_TIME_TYPE,
                                            16L,
                                            true,
                                            null,
                                            null)));
            executor.prepareStatements(connection);
            executor.addToBatch(new SeaTunnelRow(new Object[] {value}));
            executor.addToBatch(new SeaTunnelRow(new Object[] {null}));
            executor.executeBatch();
            Assertions.assertEquals(1, observed.get());
            Mockito.verify(mocked.constructed().get(0)).close();
        }
    }

    @Test
    void testNonInsertRowsAreRejectedBeforeBuffering() {
        SqlserverBulkCopyBatchStatementExecutor executor =
                new SqlserverBulkCopyBatchStatementExecutor("dbo.target", columns);
        for (RowKind kind :
                new RowKind[] {RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE}) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"payload", 7});
            row.setRowKind(kind);
            SQLException error =
                    Assertions.assertThrows(SQLException.class, () -> executor.addToBatch(row));
            Assertions.assertEquals("0A000", error.getSQLState());
            Assertions.assertTrue(error.getMessage().contains("dbo.target"));
        }
    }

    private SQLServerConnection metadataConnection() throws Exception {
        SQLServerConnection connection = Mockito.mock(SQLServerConnection.class);
        Mockito.when(connection.unwrap(SQLServerConnection.class)).thenReturn(connection);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet result = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        AtomicBoolean closed = new AtomicBoolean();
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(
                        statement.executeQuery(
                                "SELECT [payload], [id] FROM [master].[dbo].[target] WHERE 1=0"))
                .thenReturn(result);
        Mockito.when(result.getMetaData()).thenReturn(metadata);
        Mockito.doAnswer(
                        invocation -> {
                            closed.set(true);
                            return null;
                        })
                .when(result)
                .close();
        for (int index = 1; index <= 2; index++) {
            final int ordinal = index;
            Mockito.when(metadata.getColumnName(index))
                    .thenAnswer(
                            invocation -> {
                                if (closed.get()) {
                                    throw new SQLException("metadata result is closed");
                                }
                                return ordinal == 1 ? "payload" : "id";
                            });
            Mockito.when(metadata.getColumnType(index))
                    .thenReturn(index == 1 ? Types.VARCHAR : Types.INTEGER);
            Mockito.when(metadata.getPrecision(index)).thenReturn(index == 1 ? 20 : 10);
            Mockito.when(metadata.getScale(index)).thenReturn(0);
        }
        return connection;
    }
}
