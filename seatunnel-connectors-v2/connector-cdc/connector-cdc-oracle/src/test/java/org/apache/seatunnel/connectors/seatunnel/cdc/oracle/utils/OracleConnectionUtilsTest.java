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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.jdbc.JdbcConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OracleConnectionUtilsTest {

    @Mock private JdbcConnection jdbcConnection;
    @Mock private ResultSet resultSet;

    private static final String SHOW_CON_NAME =
            "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') CON_NAME FROM DUAL";

    @Test
    public void testGetCurrentContainerNameSuccess() throws SQLException {
        String expectedContainerName = "CDB$ROOT";
        when(jdbcConnection.queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class)))
                .thenAnswer(
                        invocation -> {
                            JdbcConnection.ResultSetMapper<String> mapper =
                                    invocation.getArgument(1);
                            ResultSet rs = mock(ResultSet.class);
                            when(rs.next()).thenReturn(true);
                            when(rs.getString(1)).thenReturn(expectedContainerName);
                            return mapper.apply(rs);
                        });

        String actualContainerName = OracleConnectionUtils.getCurrentContainerName(jdbcConnection);
        assertEquals(expectedContainerName, actualContainerName);
    }

    @Test
    public void testGetCurrentContainerNameNoResult() throws SQLException {
        when(jdbcConnection.queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class)))
                .thenAnswer(
                        invocation -> {
                            JdbcConnection.ResultSetMapper<String> mapper =
                                    invocation.getArgument(1);
                            ResultSet rs = mock(ResultSet.class);
                            when(rs.next()).thenReturn(false);
                            return mapper.apply(rs);
                        });

        assertThrows(
                SeaTunnelException.class,
                () -> OracleConnectionUtils.getCurrentContainerName(jdbcConnection));
    }

    @Test
    public void testGetCurrentContainerNameSQLException() throws SQLException {
        when(jdbcConnection.queryAndMap(anyString(), any()))
                .thenThrow(new SQLException("Database connection error"));

        assertThrows(
                SeaTunnelException.class,
                () -> OracleConnectionUtils.getCurrentContainerName(jdbcConnection));
    }
}
