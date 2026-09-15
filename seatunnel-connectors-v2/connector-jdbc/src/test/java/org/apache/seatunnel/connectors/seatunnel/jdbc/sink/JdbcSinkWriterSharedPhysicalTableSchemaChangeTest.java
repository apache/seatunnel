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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.SinkIdentifier;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite.SqliteDialect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Real JDBC regression coverage for shared physical sink routing. Two different upstream tables can
 * collapse into one physical sink table through a sink-table template, so a DDL emitted for {@code
 * dbA.users} must also rebuild {@code dbB.users}'s JDBC writer before the sibling starts writing
 * with the new schema.
 */
class JdbcSinkWriterSharedPhysicalTableSchemaChangeTest {

    /** Temporary database directory for the real SQLite-backed validation. */
    @TempDir Path tempDir;

    /** Shared physical sink table used by both logical source-table writers. */
    private static final TablePath SHARED_SINK_TABLE = TablePath.of("main", "shared_users");

    /** Logical source table that triggers the schema change event. */
    private static final String SOURCE_A = "dbA.users";

    /** Logical sibling source table that must receive the same schema update by broadcast. */
    private static final String SOURCE_B = "dbB.users";

    /**
     * Validates the production path end to end: two real JdbcSinkWriters share one physical sink
     * table, source A drops a column, and source B can still write rows with the new two-column
     * schema because the coordinator broadcast rebuilt the sibling writer first.
     */
    @Test
    void dropColumnSchemaChangeKeepsSiblingJdbcWriterUsableOnSharedPhysicalTable()
            throws Exception {
        Path databaseFile = tempDir.resolve("shared-physical-sink.db");
        String jdbcUrl = "jdbc:sqlite:" + databaseFile;
        createInitialTable(jdbcUrl);

        TableSchema schemaBeforeDrop = schemaWithAge();
        JdbcSinkConfig sinkConfig = buildSinkConfig(jdbcUrl);
        SqliteDialect dialect = new SqliteDialect();

        JdbcSinkWriter writerA =
                new JdbcSinkWriter(
                        SHARED_SINK_TABLE,
                        new TestSinkWriterContext(),
                        dialect,
                        sinkConfig,
                        schemaBeforeDrop,
                        schemaBeforeDrop,
                        null,
                        // baseConfig only feeds runtime sink-table resolution, which this
                        // schema-change case never reaches. An empty config keeps it non-null.
                        ReadonlyConfig.fromMap(new LinkedHashMap<>()));
        JdbcSinkWriter writerB =
                new JdbcSinkWriter(
                        SHARED_SINK_TABLE,
                        new TestSinkWriterContext(),
                        dialect,
                        sinkConfig,
                        schemaBeforeDrop,
                        schemaBeforeDrop,
                        null,
                        // baseConfig only feeds runtime sink-table resolution, which this
                        // schema-change case never reaches. An empty config keeps it non-null.
                        ReadonlyConfig.fromMap(new LinkedHashMap<>()));

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new LinkedHashMap<>();
        writers.put(SinkIdentifier.of(SOURCE_A, 0), writerA);
        writers.put(SinkIdentifier.of(SOURCE_B, 0), writerB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        try {
            coordinator.write(insertRow(SOURCE_A, 1, "alice", 18));
            coordinator.prepareCommit(1L);

            AlterTableDropColumnEvent dropAgeEvent =
                    new AlterTableDropColumnEvent(
                            TableIdentifier.of("mysql-cdc", TablePath.of("dbA", "users")), "age");
            coordinator.applySchemaChange(dropAgeEvent);

            coordinator.write(insertRow(SOURCE_B, 2, "bob"));
            coordinator.prepareCommit(2L);
        } finally {
            coordinator.close();
        }

        assertEquals(Arrays.asList("id", "name"), queryColumnNames(jdbcUrl));
        assertEquals(Arrays.asList("1:alice", "2:bob"), queryRows(jdbcUrl));
    }

    /** Creates the pre-DDL table shape that still contains the column later dropped by source A. */
    private static void createInitialTable(String jdbcUrl) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE `shared_users` (`id` INTEGER, `name` TEXT, `age` INTEGER)");
        }
    }

    /** Builds the JDBC sink config used by both logical writers that share one sink table. */
    private static JdbcSinkConfig buildSinkConfig(String jdbcUrl) {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().url(jdbcUrl).driverName("org.sqlite.JDBC").build();
        return JdbcSinkConfig.builder()
                .jdbcConnectionConfig(connectionConfig)
                .database("main")
                .table("shared_users")
                .build();
    }

    /** Returns the original three-column sink schema before the DDL drops {@code age}. */
    private static TableSchema schemaWithAge() {
        return TableSchema.builder()
                .columns(Arrays.asList(intColumn("id"), stringColumn("name"), intColumn("age")))
                .build();
    }

    /** Creates a plain integer column definition for the SQLite test table. */
    private static Column intColumn(String name) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(BasicType.INT_TYPE)
                .sourceType("INTEGER")
                .build();
    }

    /** Creates a plain string column definition for the SQLite test table. */
    private static Column stringColumn(String name) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(BasicType.STRING_TYPE)
                .sourceType("TEXT")
                .build();
    }

    /** Creates one row that mimics the SeaTunnel row payload forwarded to the sink writer. */
    private static SeaTunnelRow insertRow(String tableId, Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tableId);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    /** Builds the writer-context map required by the multi-table coordinator constructor. */
    private static Map<SinkIdentifier, SinkWriter.Context> buildContextMap(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers) {
        Map<SinkIdentifier, SinkWriter.Context> contextMap = new LinkedHashMap<>();
        for (SinkIdentifier sinkIdentifier : writers.keySet()) {
            contextMap.put(sinkIdentifier, new TestSinkWriterContext());
        }
        return contextMap;
    }

    /**
     * Reads the physical sink columns after the DDL to prove the database schema really changed.
     */
    private static List<String> queryColumnNames(String jdbcUrl) throws Exception {
        List<String> columns = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("PRAGMA table_info(`shared_users`)")) {
            while (resultSet.next()) {
                columns.add(resultSet.getString("name"));
            }
        }
        return columns;
    }

    /** Reads the final sink rows to prove the sibling writer could still insert after the DDL. */
    private static List<String> queryRows(String jdbcUrl) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT `id`, `name` FROM `shared_users` ORDER BY `id`")) {
            while (resultSet.next()) {
                rows.add(resultSet.getInt("id") + ":" + resultSet.getString("name"));
            }
        }
        return rows;
    }

    /** Minimal writer context used by the multi-table coordinator constructor inside this test. */
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
