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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #10726: query + CUSTOM_PROCESSING + custom_sql must execute custom_sql
 * once at the save-mode stage.
 */
class JdbcQueryCustomSqlSaveModeHandlerTest {

    private static final String CUSTOM_SQL = "delete from table_name where day='2026-04-07'";

    @Test
    void executeCustomSqlOnceAtSaveModeStage() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.prepareStatement(CUSTOM_SQL)).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(true);

        JdbcQueryCustomSqlSaveModeHandler handler =
                new JdbcQueryCustomSqlSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        TablePath.of("db", "table_name"),
                        CUSTOM_SQL,
                        connectionProvider);

        handler.open();
        handler.handleSaveMode();
        handler.close();

        verify(connection, times(1)).prepareStatement(CUSTOM_SQL);
        verify(statement, times(1)).execute();
        verify(connectionProvider, times(1)).closeConnection();
    }

    @Test
    void jdbcSinkReturnsHandlerForQueryAndCustomProcessing() throws Exception {
        JdbcConnectionProvider connectionProvider = mock(JdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.getAutoCommit()).thenReturn(true);

        JdbcDialect dialect = mock(JdbcDialect.class);
        when(dialect.getJdbcConnectionProvider(
                        org.mockito.ArgumentMatchers.any(JdbcConnectionConfig.class)))
                .thenReturn(connectionProvider);

        Map<String, Object> options = new HashMap<>();
        options.put(JdbcSinkOptions.URL.key(), "jdbc:mysql://localhost:3306/test");
        options.put(JdbcSinkOptions.DRIVER.key(), "com.mysql.cj.jdbc.Driver");
        options.put(JdbcSinkOptions.QUERY.key(), "insert into table_name values (?, ?)");
        options.put(JdbcSinkOptions.DATA_SAVE_MODE.key(), DataSaveMode.CUSTOM_PROCESSING.name());
        options.put(JdbcSinkOptions.SCHEMA_SAVE_MODE.key(), SchemaSaveMode.IGNORE.name());
        options.put(JdbcSinkOptions.CUSTOM_SQL.key(), CUSTOM_SQL);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .simpleSql("insert into table_name values (?, ?)")
                        .build();

        JdbcSink sink =
                new JdbcSink(
                        config,
                        sinkConfig,
                        dialect,
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        createCatalogTable());

        Optional<SaveModeHandler> handlerOptional = sink.getSaveModeHandler();
        Assertions.assertTrue(handlerOptional.isPresent());
        Assertions.assertInstanceOf(JdbcQueryCustomSqlSaveModeHandler.class, handlerOptional.get());

        try (SaveModeHandler handler = handlerOptional.get()) {
            handler.open();
            handler.handleSaveMode();
        }

        verify(connection, times(1)).prepareStatement(CUSTOM_SQL);
        verify(statement, times(1)).execute();
    }

    @Test
    void jdbcSinkSkipsHandlerWhenQueryWithoutCustomProcessing() {
        JdbcDialect dialect = mock(JdbcDialect.class);
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcSinkOptions.URL.key(), "jdbc:mysql://localhost:3306/test");
        options.put(JdbcSinkOptions.DRIVER.key(), "com.mysql.cj.jdbc.Driver");
        options.put(JdbcSinkOptions.QUERY.key(), "insert into table_name values (?, ?)");
        options.put(JdbcSinkOptions.DATA_SAVE_MODE.key(), DataSaveMode.APPEND_DATA.name());
        options.put(JdbcSinkOptions.SCHEMA_SAVE_MODE.key(), SchemaSaveMode.IGNORE.name());
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .simpleSql("insert into table_name values (?, ?)")
                        .build();

        JdbcSink sink =
                new JdbcSink(
                        config,
                        sinkConfig,
                        dialect,
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.APPEND_DATA,
                        createCatalogTable());

        Assertions.assertFalse(sink.getSaveModeHandler().isPresent());
    }

    private static CatalogTable createCatalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "test", null, "table_name"),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                null,
                "catalog");
    }
}
