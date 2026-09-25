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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.SinkIdentifier;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite.SqliteDialect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for reconnecting the active JDBC writer in a multi-table sink. */
class JdbcMultiTableReconnectTest {

    private static final String ACTIVE_TABLE_ID = "source.active_table";
    private static final String IDLE_TABLE_ID = "source.idle_table";

    @TempDir Path tempDir;

    /**
     * Verifies that generated upsert SQL keeps the active table's reduced buffer across a broken
     * connection, rebuilds its statements, and replays each buffered row once.
     */
    @Test
    void generatedSqlReplaysActiveTableBufferAfterReconnect() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("multi-table-reconnect.db");
        createTables(jdbcUrl);

        TrackingSqliteDialect activeDialect = new TrackingSqliteDialect();
        TrackingSqliteDialect idleDialect = new TrackingSqliteDialect();
        TestJdbcSinkWriter activeWriter = createWriter(jdbcUrl, "active_table", activeDialect);
        TestJdbcSinkWriter idleWriter = createWriter(jdbcUrl, "idle_table", idleDialect);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        writers.put(SinkIdentifier.of(ACTIVE_TABLE_ID, 0), activeWriter);
        writers.put(SinkIdentifier.of(IDLE_TABLE_ID, 0), idleWriter);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        try {
            coordinator.write(insertRow(ACTIVE_TABLE_ID, 1, "first"));
            coordinator.write(insertRow(ACTIVE_TABLE_ID, 2, "second"));
            coordinator.snapshotState(1L);

            activeDialect.getConnectionProvider().failNextBatch();
            coordinator.prepareCommit(2L);
        } finally {
            coordinator.close();
        }

        assertEquals(Arrays.asList("1:first", "2:second"), queryRows(jdbcUrl, "active_table"));
        assertTrue(queryRows(jdbcUrl, "idle_table").isEmpty());
        assertEquals(1, activeDialect.getConnectionProvider().reestablishConnectionCalls);
        assertEquals(0, idleDialect.getConnectionProvider().reestablishConnectionCalls);
        assertTrue(activeDialect.generatedUpsertSqlCalls > 0);
    }

    private static TestJdbcSinkWriter createWriter(
            String jdbcUrl, String table, TrackingSqliteDialect dialect) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", "org.sqlite.JDBC");
        options.put("database", "main");
        options.put("table", table);
        options.put("generate_sink_sql", true);
        options.put("primary_keys", Arrays.asList("id"));
        options.put("max_retries", 1);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);

        assertTrue(config.get(JdbcSinkOptions.GENERATE_SINK_SQL));
        assertNull(sinkConfig.getSimpleSql());
        return new TestJdbcSinkWriter(
                TablePath.of("main", table),
                new TestSinkWriterContext(),
                dialect,
                sinkConfig,
                tableSchema(),
                tableSchema(),
                0);
    }

    private static TableSchema tableSchema() {
        return TableSchema.builder()
                .columns(
                        Arrays.asList(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, 10L, false, null, "INTEGER"),
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, "TEXT")))
                .build();
    }

    private static SeaTunnelRow insertRow(String tableId, int id, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setTableId(tableId);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static void createTables(String jdbcUrl) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE `active_table` (`id` INTEGER PRIMARY KEY, `name` TEXT)");
            statement.execute("CREATE TABLE `idle_table` (`id` INTEGER PRIMARY KEY, `name` TEXT)");
        }
    }

    private static List<String> queryRows(String jdbcUrl, String table) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT `id`, `name` FROM `%s` ORDER BY `id`", table))) {
            while (resultSet.next()) {
                rows.add(resultSet.getInt("id") + ":" + resultSet.getString("name"));
            }
        }
        return rows;
    }

    private static Map<SinkIdentifier, SinkWriter.Context> buildContextMap(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers) {
        Map<SinkIdentifier, SinkWriter.Context> contexts = new LinkedHashMap<>();
        for (SinkIdentifier identifier : writers.keySet()) {
            contexts.put(identifier, new TestSinkWriterContext());
        }
        return contexts;
    }

    private static class TrackingSqliteDialect extends SqliteDialect {
        private TrackingConnectionProvider connectionProvider;
        private int generatedUpsertSqlCalls;

        @Override
        public JdbcConnectionProvider getJdbcConnectionProvider(
                JdbcConnectionConfig jdbcConnectionConfig) {
            connectionProvider = new TrackingConnectionProvider(jdbcConnectionConfig);
            return connectionProvider;
        }

        @Override
        public java.util.Optional<String> getUpsertStatement(
                String database, String tableName, String[] fieldNames, String[] pkNames) {
            generatedUpsertSqlCalls++;
            return super.getUpsertStatement(database, tableName, fieldNames, pkNames);
        }

        private TrackingConnectionProvider getConnectionProvider() {
            return connectionProvider;
        }
    }

    private static class TrackingConnectionProvider implements JdbcConnectionProvider {
        private final JdbcConnectionConfig jdbcConfig;
        private Connection connection;
        private boolean failNextBatch;
        private int reestablishConnectionCalls;

        private TrackingConnectionProvider(JdbcConnectionConfig jdbcConfig) {
            this.jdbcConfig = jdbcConfig;
        }

        private void failNextBatch() {
            failNextBatch = true;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean isConnectionValid() throws SQLException {
            return connection != null && !connection.isClosed();
        }

        @Override
        public Connection getOrEstablishConnection() throws SQLException {
            if (!isConnectionValid()) {
                Connection delegate = DriverManager.getConnection(jdbcConfig.getUrl());
                delegate.setAutoCommit(jdbcConfig.isAutoCommit());
                connection = wrapConnection(delegate);
            }
            return connection;
        }

        @Override
        public void closeConnection() {
            if (connection == null) {
                return;
            }
            try {
                connection.close();
            } catch (SQLException ignored) {
                // The broken connection can already be closed by the simulated network failure.
            } finally {
                connection = null;
            }
        }

        @Override
        public Connection reestablishConnection() throws SQLException {
            reestablishConnectionCalls++;
            closeConnection();
            return getOrEstablishConnection();
        }

        private Connection wrapConnection(Connection delegate) {
            return (Connection)
                    Proxy.newProxyInstance(
                            Connection.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                try {
                                    Object result = method.invoke(delegate, args);
                                    if (result instanceof PreparedStatement
                                            && "prepareStatement".equals(method.getName())) {
                                        return wrapStatement(delegate, (PreparedStatement) result);
                                    }
                                    return result;
                                } catch (InvocationTargetException exception) {
                                    throw exception.getCause();
                                }
                            });
        }

        private PreparedStatement wrapStatement(
                Connection owner, PreparedStatement preparedStatement) {
            return (PreparedStatement)
                    Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            (proxy, method, args) -> {
                                if ("executeBatch".equals(method.getName()) && failNextBatch) {
                                    failNextBatch = false;
                                    owner.close();
                                    throw new SQLException("connection dropped", "08S01");
                                }
                                try {
                                    return method.invoke(preparedStatement, args);
                                } catch (InvocationTargetException exception) {
                                    throw exception.getCause();
                                }
                            });
        }
    }

    private static class TestJdbcSinkWriter extends JdbcSinkWriter {
        private TestJdbcSinkWriter(
                TablePath sinkTablePath,
                SinkWriter.Context context,
                SqliteDialect dialect,
                JdbcSinkConfig jdbcSinkConfig,
                TableSchema tableSchema,
                TableSchema databaseTableSchema,
                Integer primaryKeyIndex) {
            super(
                    sinkTablePath,
                    context,
                    dialect,
                    jdbcSinkConfig,
                    tableSchema,
                    databaseTableSchema,
                    primaryKeyIndex);
        }

        @Override
        public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
                int tableSize, int queueSize) {
            return new MultiTableResourceManager<ConnectionPoolManager>() {};
        }

        @Override
        public void setMultiTableResourceManager(
                MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
                int queueIndex) {
            // Keep the per-writer provider so the test can deterministically break one table only.
        }
    }

    private static class TestSinkWriterContext implements SinkWriter.Context {
        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return new DefaultEventProcessor();
        }
    }
}
