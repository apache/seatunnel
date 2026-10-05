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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests driver-specific JDBC connection validation fallbacks. */
class JdbcConnectionValidationUtilsTest {

    /** Verifies that Xugu uses an explicit SQL probe instead of Connection.isValid(timeout). */
    @Test
    void testXuguValidationUsesSqlProbe() throws SQLException {
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(JdbcConnectionValidationUtils.XUGU_DRIVER)
                        .url("jdbc:xugu://localhost:5138/SYSTEM")
                        .build();
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(JdbcConnectionValidationUtils.XUGU_VALIDATION_QUERY))
                .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        Assertions.assertTrue(
                JdbcConnectionValidationUtils.isConnectionValid(connection, jdbcConnectionConfig));
        verify(connection, never()).isValid(anyInt());
    }

    /** Verifies that default drivers still use the standard JDBC validation hook. */
    @Test
    void testDefaultValidationFallsBackToJdbcIsValid() throws SQLException {
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName("org.postgresql.Driver")
                        .url("jdbc:postgresql://localhost:5432/test")
                        .connectionCheckTimeoutSeconds(12)
                        .build();
        Connection connection = mock(Connection.class);
        when(connection.isValid(12)).thenReturn(true);

        Assertions.assertTrue(
                JdbcConnectionValidationUtils.isConnectionValid(connection, jdbcConnectionConfig));
        verify(connection).isValid(12);
        verify(connection, never())
                .prepareStatement(JdbcConnectionValidationUtils.XUGU_VALIDATION_QUERY);
    }
}
