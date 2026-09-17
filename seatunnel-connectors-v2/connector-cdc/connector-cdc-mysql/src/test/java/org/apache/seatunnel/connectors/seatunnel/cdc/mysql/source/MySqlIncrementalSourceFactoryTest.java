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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class MySqlIncrementalSourceFactoryTest {

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testImplementsSupportSourceDryRunValidation() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Assertions.assertInstanceOf(SupportSourceDryRunValidation.class, factory);
    }

    @Test
    public void testValidateConnectionForDryRunRejectsMissingReplicationSlave() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();

        try (MockedStatic<MySqlConnectionUtils> mockedUtils =
                mockStatic(MySqlConnectionUtils.class)) {
            MySqlConnection mockConnection = mock(MySqlConnection.class);
            mockedUtils
                    .when(
                            () ->
                                    MySqlConnectionUtils.createMySqlConnection(
                                            any(Configuration.class)))
                    .thenReturn(mockConnection);

            // userHasPrivileges defaults to false, so REPLICATION SLAVE check fails first.
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    factory.validateConnectionForDryRun(
                                            createContext(), Collections.emptyList()));
            Assertions.assertTrue(
                    exception.getMessage().contains("REPLICATION SLAVE"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    public void testValidateConnectionForDryRunRejectsMissingReplicationClient() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();

        try (MockedStatic<MySqlConnectionUtils> mockedUtils =
                mockStatic(MySqlConnectionUtils.class)) {
            MySqlConnection mockConnection = mock(MySqlConnection.class);
            mockedUtils
                    .when(
                            () ->
                                    MySqlConnectionUtils.createMySqlConnection(
                                            any(Configuration.class)))
                    .thenReturn(mockConnection);
            when(mockConnection.userHasPrivileges("REPLICATION SLAVE")).thenReturn(true);

            // REPLICATION CLIENT defaults to false.
            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    factory.validateConnectionForDryRun(
                                            createContext(), Collections.emptyList()));
            Assertions.assertTrue(
                    exception.getMessage().contains("REPLICATION CLIENT"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    public void testValidateConnectionForDryRunPassesWithRequiredPrivileges() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();

        try (MockedStatic<MySqlConnectionUtils> mockedUtils =
                mockStatic(MySqlConnectionUtils.class)) {
            MySqlConnection mockConnection = mock(MySqlConnection.class);
            mockedUtils
                    .when(
                            () ->
                                    MySqlConnectionUtils.createMySqlConnection(
                                            any(Configuration.class)))
                    .thenReturn(mockConnection);
            when(mockConnection.userHasPrivileges("REPLICATION SLAVE")).thenReturn(true);
            when(mockConnection.userHasPrivileges("REPLICATION CLIENT")).thenReturn(true);

            Assertions.assertDoesNotThrow(
                    () ->
                            factory.validateConnectionForDryRun(
                                    createContext(), Collections.emptyList()));
        }
    }

    @Test
    public void testValidateConnectionForDryRunWrapsConnectionFailure() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();

        try (MockedStatic<MySqlConnectionUtils> mockedUtils =
                mockStatic(MySqlConnectionUtils.class)) {
            MySqlConnection mockConnection = mock(MySqlConnection.class);
            mockedUtils
                    .when(
                            () ->
                                    MySqlConnectionUtils.createMySqlConnection(
                                            any(Configuration.class)))
                    .thenReturn(mockConnection);
            when(mockConnection.userHasPrivileges("REPLICATION SLAVE"))
                    .thenThrow(new RuntimeException("Connection refused"));

            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    factory.validateConnectionForDryRun(
                                            createContext(), Collections.emptyList()));
            Assertions.assertTrue(
                    exception.getMessage().contains("Connection refused"),
                    "Actual: " + exception.getMessage());
        }
    }

    @Test
    public void testValidateConnectionErrorMessageContainsUsername() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        String testUsername = "my_test_user";

        try (MockedStatic<MySqlConnectionUtils> mockedUtils =
                mockStatic(MySqlConnectionUtils.class)) {
            MySqlConnection mockConnection = mock(MySqlConnection.class);
            mockedUtils
                    .when(
                            () ->
                                    MySqlConnectionUtils.createMySqlConnection(
                                            any(Configuration.class)))
                    .thenReturn(mockConnection);

            SeaTunnelException exception =
                    Assertions.assertThrows(
                            SeaTunnelException.class,
                            () ->
                                    factory.validateConnectionForDryRun(
                                            createContext(testUsername), Collections.emptyList()));
            Assertions.assertTrue(
                    exception.getMessage().contains(testUsername),
                    "Error message should contain username, actual: " + exception.getMessage());
        }
    }

    @Test
    public void testInferSchemaForDryRunDelegatesToCatalogTableUtil() throws Exception {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();

        try (MockedStatic<CatalogTableUtil> mockedCatalog = mockStatic(CatalogTableUtil.class)) {
            mockedCatalog
                    .when(
                            () ->
                                    CatalogTableUtil.getCatalogTables(
                                            any(ReadonlyConfig.class), any(ClassLoader.class)))
                    .thenReturn(Collections.emptyList());

            List<CatalogTable> result = factory.inferSchemaForDryRun(createContext());

            Assertions.assertTrue(result.isEmpty());
            mockedCatalog.verify(
                    () ->
                            CatalogTableUtil.getCatalogTables(
                                    any(ReadonlyConfig.class), any(ClassLoader.class)));
        }
    }

    private TableSourceFactoryContext createContext() {
        return createContext("testuser");
    }

    private TableSourceFactoryContext createContext(String username) {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://localhost:3306/testdb");
        config.put("username", username);
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        return new TableSourceFactoryContext(
                ReadonlyConfig.fromMap(config), Thread.currentThread().getContextClassLoader());
    }
}
