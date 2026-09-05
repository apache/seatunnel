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
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcCoordinatedSchemaEvolutionTest {

    private static final TablePath SINK_TABLE = TablePath.of("main", "users");

    @TempDir Path tempDir;

    @Test
    void appliesExternalChangeOnceAndRefreshesEveryWriter() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("coordinated-schema.db");
        createInitialTable(jdbcUrl);

        TableSchema initialSchema = schemaWithAge();
        CatalogTable evolvedTable = catalogTable(schemaWithoutAge());
        JdbcSinkConfig sinkConfig = buildSinkConfig(jdbcUrl);
        SqliteDialect dialect = new SqliteDialect();
        JdbcSinkWriter firstWriter = createWriter(dialect, sinkConfig, initialSchema);
        JdbcSinkWriter secondWriter = createWriter(dialect, sinkConfig, initialSchema);

        AlterTableDropColumnEvent event =
                new AlterTableDropColumnEvent(evolvedTable.getTableId(), "age");
        event.setChangeAfter(evolvedTable);

        try {
            firstWriter.write(row(1, "alice", 18));
            firstWriter.prepareCommit();

            new JdbcSchemaChangeApplier(dialect, sinkConfig, SINK_TABLE).apply(event);
            firstWriter.refreshSchema(evolvedTable);
            secondWriter.refreshSchema(evolvedTable);

            secondWriter.write(row(2, "bob"));
            secondWriter.prepareCommit();
        } finally {
            firstWriter.close();
            secondWriter.close();
        }

        assertEquals(Arrays.asList("1:alice", "2:bob"), queryRows(jdbcUrl));
        assertEquals(2, queryColumnCount(jdbcUrl));
    }

    @Test
    void applySchemaChangeUsesCompleteChangeAfterSchema() throws Exception {
        String jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("writer-schema-change.db");
        createInitialTable(jdbcUrl);

        TableSchema initialSchema = schemaWithAge();
        CatalogTable evolvedTable = catalogTable(schemaWithoutAge());
        JdbcSinkWriter writer =
                createWriter(new SqliteDialect(), buildSinkConfig(jdbcUrl), initialSchema);
        AlterTableDropColumnEvent event =
                new AlterTableDropColumnEvent(evolvedTable.getTableId(), "age");
        event.setChangeAfter(evolvedTable);

        try {
            writer.applySchemaChange(event);
            writer.write(row(1, "alice"));
            writer.prepareCommit();

            assertEquals(evolvedTable.getTableSchema(), writer.tableSchema);
        } finally {
            writer.close();
        }

        assertEquals(Collections.singletonList("1:alice"), queryRows(jdbcUrl));
        assertEquals(2, queryColumnCount(jdbcUrl));
    }

    private JdbcSinkWriter createWriter(
            SqliteDialect dialect, JdbcSinkConfig sinkConfig, TableSchema tableSchema) {
        return new JdbcSinkWriter(
                SINK_TABLE,
                new TestSinkWriterContext(),
                dialect,
                sinkConfig,
                tableSchema,
                tableSchema,
                null);
    }

    private static void createInitialTable(String jdbcUrl) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE `users` (`id` INTEGER, `name` TEXT, `age` INTEGER)");
        }
    }

    private static JdbcSinkConfig buildSinkConfig(String jdbcUrl) {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().url(jdbcUrl).driverName("org.sqlite.JDBC").build();
        return JdbcSinkConfig.builder()
                .jdbcConnectionConfig(connectionConfig)
                .database("main")
                .table("users")
                .build();
    }

    private static CatalogTable catalogTable(TableSchema tableSchema) {
        return CatalogTable.of(
                TableIdentifier.of("jdbc", SINK_TABLE),
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static TableSchema schemaWithAge() {
        return TableSchema.builder()
                .columns(Arrays.asList(intColumn("id"), stringColumn("name"), intColumn("age")))
                .build();
    }

    private static TableSchema schemaWithoutAge() {
        return TableSchema.builder()
                .columns(Arrays.asList(intColumn("id"), stringColumn("name")))
                .build();
    }

    private static Column intColumn(String name) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(BasicType.INT_TYPE)
                .sourceType("INTEGER")
                .build();
    }

    private static Column stringColumn(String name) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(BasicType.STRING_TYPE)
                .sourceType("TEXT")
                .build();
    }

    private static SeaTunnelRow row(Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static int queryColumnCount(String jdbcUrl) throws Exception {
        int count = 0;
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("PRAGMA table_info(`users`)")) {
            while (resultSet.next()) {
                count++;
            }
        }
        return count;
    }

    private static java.util.List<String> queryRows(String jdbcUrl) throws Exception {
        java.util.List<String> rows = new java.util.ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT `id`, `name` FROM `users` ORDER BY `id`")) {
            while (resultSet.next()) {
                rows.add(resultSet.getInt("id") + ":" + resultSet.getString("name"));
            }
        }
        return rows;
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
