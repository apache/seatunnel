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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.operation.event.TruncateTableEvent;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link AbstractJdbcSinkWriter#applyTableOperation} flushes buffered rows before
 * truncating. SQLite has no {@code TRUNCATE TABLE}, so the test dialect uses {@code DELETE FROM}.
 */
class JdbcSinkWriterTruncateTableTest {

    @TempDir Path tempDir;

    private static final TablePath SINK_TABLE = TablePath.of("main", "shared_users");

    @Test
    void applyTableOperationFlushesThenTruncates() throws Exception {
        Path databaseFile = tempDir.resolve("truncate-sink.db");
        String jdbcUrl = "jdbc:sqlite:" + databaseFile;
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE `shared_users` (`id` INTEGER, `name` TEXT)");
        }

        TableSchema schema =
                TableSchema.builder()
                        .columns(Arrays.asList(intColumn("id"), stringColumn("name")))
                        .build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url(jdbcUrl)
                                        .driverName("org.sqlite.JDBC")
                                        .build())
                        .database("main")
                        .table("shared_users")
                        .build();

        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        SINK_TABLE,
                        new TestSinkWriterContext(),
                        new SqliteDeleteAllDialect(),
                        sinkConfig,
                        schema,
                        schema,
                        null);
        try {
            writer.write(insertRow(1, "alice"));
            writer.write(insertRow(2, "bob"));
            writer.applyTableOperation(
                    TruncateTableEvent.of(TableIdentifier.of("", "main", "shared_users")));
        } finally {
            writer.close();
        }

        assertEquals(Collections.emptyList(), queryRows(jdbcUrl));
    }

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

    private static SeaTunnelRow insertRow(Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId("dbA.users");
        row.setRowKind(RowKind.INSERT);
        return row;
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

    private static final class SqliteDeleteAllDialect extends SqliteDialect {
        @Override
        public void applyTruncateTable(Connection connection, TablePath tablePath)
                throws SQLException {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DELETE FROM `shared_users`");
            }
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
