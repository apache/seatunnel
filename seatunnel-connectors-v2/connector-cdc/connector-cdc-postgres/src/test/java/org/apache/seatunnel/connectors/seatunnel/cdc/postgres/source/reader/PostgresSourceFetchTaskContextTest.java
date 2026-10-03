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

import io.debezium.connector.postgresql.connection.PostgresConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PostgresSourceFetchTaskContextTest {

    @Test
    public void testSlotIsNotCheckedWhenJobDoesNotStream() throws SQLException {
        PostgresConnection dataConnection = mock(PostgresConnection.class);

        PostgresSourceFetchTaskContext.checkReplicationSlotNotInvalidated(
                false, dataConnection, "seatunnel");

        verify(dataConnection, never()).connection();
    }

    @Test
    public void testInvalidatedSlotFailsBeforeStreaming() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot("idle_timeout");

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                PostgresSourceFetchTaskContext.checkReplicationSlotNotInvalidated(
                                        true, dataConnection, "seatunnel"));
        Assertions.assertEquals(
                PostgresConnectorErrorCode.REPLICATION_SLOT_INVALIDATED,
                exception.getSeaTunnelErrorCode());
    }

    @Test
    public void testHealthySlotPasses() throws SQLException {
        PostgresConnection dataConnection = connectionReturningSlot(null);

        Assertions.assertDoesNotThrow(
                () ->
                        PostgresSourceFetchTaskContext.checkReplicationSlotNotInvalidated(
                                true, dataConnection, "seatunnel"));
        verify(dataConnection, never()).rollback();
    }

    @Test
    public void testFailedCheckRollsBackAndContinues() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException("denied"));
        PostgresConnection dataConnection = mock(PostgresConnection.class);
        when(dataConnection.connection()).thenReturn(connection);

        Assertions.assertDoesNotThrow(
                () ->
                        PostgresSourceFetchTaskContext.checkReplicationSlotNotInvalidated(
                                true, dataConnection, "seatunnel"));
        verify(dataConnection).rollback();
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
        return dataConnection;
    }
}
