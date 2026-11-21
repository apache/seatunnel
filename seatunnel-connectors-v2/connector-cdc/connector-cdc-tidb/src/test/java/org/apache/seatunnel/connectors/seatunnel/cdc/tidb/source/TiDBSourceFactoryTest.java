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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers explicit clustered-key metadata and the pre-common-handle TiDB compatibility boundary.
 * Metadata connection/permission failures must remain failures rather than select the wrong layout.
 */
class TiDBSourceFactoryTest {
    @Test
    void distinguishesClusteredAndNonClusteredPrimaryKeys() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn("CLUSTERED", "NONCLUSTERED");
        assertTrue(TiDBSourceFactory.hasClusteredPrimaryKey(connection, "test", "orders"));
        assertFalse(TiDBSourceFactory.hasClusteredPrimaryKey(connection, "test", "orders"));
    }

    @Test
    void preservesIntegerKeyCompatibilityOnPreCommonHandleServers() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.prepareStatement(anyString()))
                .thenThrow(new SQLException("Unknown column TIDB_PK_TYPE", "42S22", 1054));
        assertFalse(TiDBSourceFactory.hasClusteredPrimaryKey(connection, "test", "orders"));
    }

    @Test
    void doesNotHideMetadataPermissionOrConnectionFailures() throws Exception {
        Connection connection = mock(Connection.class);
        SQLException failure = new SQLException("Connection unavailable", "08001", 0);
        when(connection.prepareStatement(anyString())).thenThrow(failure);
        assertSame(
                failure,
                assertThrows(
                        SQLException.class,
                        () ->
                                TiDBSourceFactory.hasClusteredPrimaryKey(
                                        connection, "test", "orders")));
    }
}
