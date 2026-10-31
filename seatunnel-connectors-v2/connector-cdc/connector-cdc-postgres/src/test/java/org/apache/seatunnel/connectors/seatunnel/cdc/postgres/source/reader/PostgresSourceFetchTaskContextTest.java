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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresSourceFetchTaskContextTest {

    private static final String SLOT_NAME = "seatunnel";
    private static final String PLUGIN_NAME = "pgoutput";
    private static final int JOB_STATUS_OID = 24776;

    private final PostgresConnectorConfig connectorConfig =
            new PostgresConnectorConfig(
                    Configuration.create()
                            .with(PostgresConnectorConfig.SLOT_NAME, SLOT_NAME)
                            .with(PostgresConnectorConfig.PLUGIN_NAME, PLUGIN_NAME)
                            .with("database.server.name", "postgres_cdc_source")
                            .build());

    @Test
    public void testSnapshotterIsInitializedBeforeItIsAsked() throws SQLException {
        SlotState slotState = new SlotState(null, null, 0L, true);
        PostgresConnection dataConnection = connectionReturningSlot(null);
        when(dataConnection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME)).thenReturn(slotState);
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        Assertions.assertSame(
                slotState,
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertSame(slotState, snapshotter.slotState);
    }

    @Test
    public void testInvalidatedSlotFailsBeforeStreaming() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot("idle_timeout");
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                PostgresSourceFetchTaskContext.initSnapshotter(
                                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertEquals(
                PostgresConnectorErrorCode.REPLICATION_SLOT_INVALIDATED,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(snapshotter.initialized);
        verify(dataConnection, never()).getReplicationSlotState(anyString(), anyString());
    }

    @Test
    public void testInvalidatedSlotDoesNotBlockSnapshotOnlyJob() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot("wal_removed");
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(false);

        Assertions.assertNull(
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        Assertions.assertTrue(snapshotter.initialized);
        Assertions.assertNull(snapshotter.slotState);
        verify(dataConnection, never()).getReplicationSlotState(anyString(), anyString());
    }

    @Test
    public void testFailedCheckRollsBackAndReadsSlotState() throws SQLException {
        SlotState slotState = new SlotState(null, null, 0L, true);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("denied"));
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);
        when(dataConnection.serverInfo()).thenReturn(mock(ServerInfo.class));
        when(dataConnection.getReplicationSlotState(SLOT_NAME, PLUGIN_NAME)).thenReturn(slotState);
        InitRequiredSnapshotter snapshotter = new InitRequiredSnapshotter(true);

        Assertions.assertSame(
                slotState,
                PostgresSourceFetchTaskContext.initSnapshotter(
                        snapshotter, connectorConfig, null, dataConnection));
        verify(dataConnection).rollback();
        Assertions.assertSame(slotState, snapshotter.slotState);
    }

    @Test
    public void testTypeNameLookupIsCommitted() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getLong("oid")).thenReturn((long) JOB_STATUS_OID);
        when(resultSet.getString("typname")).thenReturn("JobStatus");
        when(resultSet.getString("typtype")).thenReturn("e");
        when(resultSet.getString("type_name")).thenReturn("\"JobStatus\"");
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(resultSet);
        Connection connection = mock(Connection.class);
        when(connection.createArrayOf(anyString(), any())).thenReturn(mock(Array.class));
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);

        Table qualified =
                PostgresSourceFetchTaskContext.qualifyTypeNames(
                        dataConnection, relationWithJobStatusColumn());

        Assertions.assertEquals("\"JobStatus\"", qualified.columnWithName("st2").typeName());
        InOrder inOrder = inOrder(statement, dataConnection);
        inOrder.verify(statement).executeQuery();
        inOrder.verify(dataConnection).commit();
    }

    @Test
    public void testFailedTypeNameLookupRollsBackAndNamesTable() throws SQLException {
        SQLException cause = new SQLException("denied");
        Connection connection = mock(Connection.class);
        when(connection.createArrayOf(anyString(), any())).thenReturn(mock(Array.class));
        when(connection.prepareStatement(anyString())).thenThrow(cause);
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                PostgresSourceFetchTaskContext.qualifyTypeNames(
                                        dataConnection, relationWithJobStatusColumn()));

        Assertions.assertEquals(
                PostgresConnectorErrorCode.RESOLVE_COLUMN_TYPE_NAME_FAILED,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("public.t5"), exception.getMessage());
        Assertions.assertSame(cause, exception.getCause());
        verify(dataConnection).rollback();
        verify(dataConnection, never()).commit();
    }

    private static Table relationWithJobStatusColumn() {
        return Table.editor()
                .tableId(new TableId(null, "public", "t5"))
                .setPrimaryKeyNames(Collections.singletonList("id"))
                .setColumns(
                        Arrays.asList(
                                Column.editor()
                                        .name("id")
                                        .jdbcType(Types.INTEGER)
                                        .nativeType(23)
                                        .type("int4", "int4")
                                        .position(1)
                                        .optional(false)
                                        .create(),
                                Column.editor()
                                        .name("st2")
                                        .jdbcType(Types.VARCHAR)
                                        .nativeType(JOB_STATUS_OID)
                                        .type("JobStatus", "JobStatus")
                                        .position(2)
                                        .optional(true)
                                        .create()))
                .create();
    }

    private static PostgresConnection connectionReturningSlot(String invalidationReason)
            throws SQLException {
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("slot_name");
        when(metaData.getColumnName(2)).thenReturn("invalidation_reason");
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getMetaData()).thenReturn(metaData);
        when(resultSet.getString("invalidation_reason")).thenReturn(invalidationReason);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(resultSet);
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);
        when(dataConnection.serverInfo()).thenReturn(mock(ServerInfo.class));
        return dataConnection;
    }

    /** A custom snapshotter that, like many real ones, decides from the state given to init. */
    private static class InitRequiredSnapshotter implements Snapshotter {
        private final boolean stream;
        private boolean initialized;
        private SlotState slotState;

        private InitRequiredSnapshotter(boolean stream) {
            this.stream = stream;
        }

        @Override
        public void init(
                PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
            this.initialized = true;
            this.slotState = slotState;
        }

        @Override
        public boolean shouldSnapshot() {
            checkInitialized();
            return !stream;
        }

        @Override
        public boolean shouldStream() {
            checkInitialized();
            return stream;
        }

        @Override
        public Optional<String> buildSnapshotQuery(
                TableId tableId, List<String> snapshotSelectColumns) {
            return Optional.empty();
        }

        private void checkInitialized() {
            if (!initialized) {
                throw new IllegalStateException("init must be called first");
            }
        }
    }
}
