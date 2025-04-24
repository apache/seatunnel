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

import io.debezium.connector.oracle.OracleConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OracleConnectionUtilsTest {

    @Mock private OracleConnection oracleConnection;
    @Mock private ResultSet resultSet;

    private static final String SHOW_CON_NAME =
            "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') CON_NAME FROM DUAL";

    @Test
    public void testGetCurrentContainerNameSuccess() throws SQLException {
        String expectedContainerName = "CDB$ROOT";
        when(oracleConnection.queryAndMap(anyString(), any())).thenReturn(expectedContainerName);

        String actualContainerName =
                OracleConnectionUtils.getCurrentContainerName(oracleConnection);
        assertEquals(expectedContainerName, actualContainerName);
    }

    @Test
    public void testGetCurrentContainerNameNoResult() throws SQLException {
        when(oracleConnection.queryAndMap(anyString(), any()))
                .thenThrow(
                        new SeaTunnelException(
                                "Cannot read the container name via '"
                                        + SHOW_CON_NAME
                                        + "'. Make sure your server is correctly configured"));

        assertThrows(
                SeaTunnelException.class,
                () -> OracleConnectionUtils.getCurrentContainerName(oracleConnection));
    }

    @Test
    public void testGetCurrentContainerNameSQLException() throws SQLException {
        when(oracleConnection.queryAndMap(anyString(), any()))
                .thenThrow(new SQLException("Database connection error"));

        assertThrows(
                SeaTunnelException.class,
                () -> OracleConnectionUtils.getCurrentContainerName(oracleConnection));
    }
}
