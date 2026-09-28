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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;

import org.duckdb.DuckDBDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The real DuckLake file-count test is opt-in and skipped in default CI. Run it with
 * -Dducklake.extension=/path/ducklake.duckdb_extension and -Dsqlite.scanner.extension=... The exact
 * file counts apply to the pinned JDBC 1.3.1 and extension fixture, not all versions.
 */
public class DuckLakeBulkWriteTest {
    @TempDir Path tempDir;

    @Test
    void preservesDecimalTimestampAndNullThroughStage() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "amount", new DecimalType(18, 2), 18, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "event_time",
                                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                        8,
                                        false,
                                        null,
                                        ""))
                        .column(
                                PhysicalColumn.of(
                                        "note", BasicType.STRING_TYPE, 128, true, null, ""))
                        .build();
        String url = "jdbc:duckdb:" + tempDir.resolve("types.db");
        try (Connection connection = new DuckDBDriver().connect(url, new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE main.events (amount DECIMAL(18,2), event_time TIMESTAMPTZ,"
                            + " note VARCHAR)");
            DuckLakeBulkStatementExecutor executor =
                    new DuckLakeBulkStatementExecutor(
                            "main", "events", schema, new DuckDBJdbcRowConverter());
            executor.prepareStatements(connection);
            executor.addToBatch(
                    new SeaTunnelRow(
                            new Object[] {
                                new BigDecimal("12.34"),
                                OffsetDateTime.parse("2026-09-27T12:00:00Z"),
                                null
                            }));
            executor.executeBatch();
            executor.closeStatements();
            try (ResultSet result =
                    statement.executeQuery("SELECT amount, event_time, note FROM main.events")) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(new BigDecimal("12.34"), result.getBigDecimal(1));
                Assertions.assertEquals(
                        OffsetDateTime.parse("2026-09-27T12:00:00Z").toInstant(),
                        result.getObject(2, OffsetDateTime.class).toInstant());
                Assertions.assertNull(result.getString(3));
            }
        }
    }

    @Test
    void failedTransferDoesNotPublishPartialBatchAndReplayWorks() throws Exception {
        String url = "jdbc:duckdb:" + tempDir.resolve("local.db");
        try (Connection connection = new DuckDBDriver().connect(url, new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE main.events (id INTEGER CHECK (id <> 2), val VARCHAR)");
            statement.execute("INSERT INTO main.events VALUES (0, 'existing')");
            DuckLakeBulkStatementExecutor executor =
                    new DuckLakeBulkStatementExecutor(
                            "main", "events", schema(), new DuckDBJdbcRowConverter());
            executor.prepareStatements(connection);
            SeaTunnelRow update = new SeaTunnelRow(new Object[] {0, "not-insert"});
            update.setRowKind(RowKind.UPDATE_AFTER);
            Assertions.assertThrows(SQLException.class, () -> executor.addToBatch(update));

            executor.addToBatch(new SeaTunnelRow(new Object[] {1, "one"}));
            executor.addToBatch(new SeaTunnelRow(new Object[] {2, "invalid"}));
            Assertions.assertThrows(SQLException.class, executor::executeBatch);
            // The target still exists: a failed INSERT SELECT must not publish its valid first row.
            try (ResultSet result = statement.executeQuery("SELECT id FROM main.events")) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(0, result.getInt(1));
                Assertions.assertFalse(result.next());
            }
            // Discard the invalid batch explicitly; this is not an automatic JDBC retry.
            executor.clearBatch();
            executor.addToBatch(new SeaTunnelRow(new Object[] {1, "one"}));
            executor.addToBatch(new SeaTunnelRow(new Object[] {3, "three"}));
            executor.executeBatch();
            executor.addToBatch(new SeaTunnelRow(new Object[] {4, "four"}));
            executor.executeBatch();
            executor.closeStatements();
            try (ResultSet result =
                    statement.executeQuery(
                            "SELECT COUNT(*), COUNT(DISTINCT id) FROM main.events")) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(4, result.getInt(1));
                Assertions.assertEquals(4, result.getInt(2));
            }
        }
    }

    @Test
    void closesStageWithoutClosingReusableConnection() throws Exception {
        try (Connection connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE main.events (id INTEGER, val VARCHAR)");
            DuckLakeBulkStatementExecutor executor =
                    new DuckLakeBulkStatementExecutor(
                            "main", "events", schema(), new DuckDBJdbcRowConverter());
            executor.prepareStatements(connection);
            for (int batch = 0; batch < 50; batch++) {
                executor.addToBatch(new SeaTunnelRow(new Object[] {batch, "value-" + batch}));
                executor.executeBatch();
            }
            executor.closeStatements();
            executor.closeStatements();
            try (ResultSet result =
                    statement.executeQuery(
                            "SELECT COUNT(*) FROM duckdb_tables() WHERE temporary")) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(0, result.getInt(1));
            }
            try (ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM main.events")) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(50, result.getInt(1));
            }
        }
    }

    @Test
    void closesRemainingResourcesWhenStageInsertCloseFails() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        Statement validation = Mockito.mock(Statement.class);
        Statement flush = Mockito.mock(Statement.class);
        Statement cleanup = Mockito.mock(Statement.class);
        PreparedStatement insert = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.createStatement()).thenReturn(validation, flush, cleanup);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(insert);
        DuckLakeBulkStatementExecutor executor =
                new DuckLakeBulkStatementExecutor(
                        "main", "events", schema(), new DuckDBJdbcRowConverter());
        executor.prepareStatements(connection);
        executor.addToBatch(new SeaTunnelRow(new Object[] {1, "one"}));
        SQLException insertFailure = new SQLException("insert close failed");
        SQLException statementFailure = new SQLException("statement close failed");
        Mockito.doThrow(insertFailure).when(insert).close();
        Mockito.doThrow(statementFailure).when(flush).close();
        SQLException failure = Assertions.assertThrows(SQLException.class, executor::executeBatch);
        Assertions.assertSame(insertFailure, failure);
        Assertions.assertArrayEquals(new Throwable[] {statementFailure}, failure.getSuppressed());
        Mockito.verify(flush).close();
        SQLException dropFailure = new SQLException("drop failed");
        SQLException cleanupFailure = new SQLException("cleanup close failed");
        Mockito.when(cleanup.execute(Mockito.startsWith("DROP TABLE IF EXISTS ")))
                .thenThrow(dropFailure);
        Mockito.doThrow(cleanupFailure).when(cleanup).close();
        SQLException closeFailure =
                Assertions.assertThrows(SQLException.class, executor::closeStatements);
        Assertions.assertSame(dropFailure, closeFailure);
        Assertions.assertArrayEquals(
                new Throwable[] {cleanupFailure}, closeFailure.getSuppressed());
        Mockito.verify(cleanup).close();
        // Closing again must not reuse a failed cleanup statement or close the pooled connection.
        executor.closeStatements();
        Mockito.verify(connection, Mockito.times(3)).createStatement();
        Mockito.verify(connection, Mockito.never()).close();
    }

    @Test
    void bulkWriteCreatesOneFileAndSurvivesReconnect() throws Exception {
        String duckLakeExtension = System.getProperty("ducklake.extension");
        String sqliteExtension = System.getProperty("sqlite.scanner.extension");
        Assumptions.assumeTrue(duckLakeExtension != null && sqliteExtension != null);
        Path dataPath = Files.createDirectory(tempDir.resolve("data"));
        Path catalogPath = tempDir.resolve("catalog.sqlite");
        Path initFile = tempDir.resolve("init.sql");
        Files.write(
                initFile,
                ("LOAD '"
                                + duckLakeExtension.replace("'", "''")
                                + "';\nLOAD '"
                                + sqliteExtension.replace("'", "''")
                                + "';\nATTACH 'ducklake:sqlite:"
                                + catalogPath
                                + "' AS lake (DATA_PATH '"
                                + dataPath
                                + "');\n")
                        .getBytes(StandardCharsets.UTF_8));
        String url = "jdbc:duckdb:;session_init_sql_file=" + initFile;
        Properties properties = new Properties();
        properties.setProperty("extension_directory", tempDir.resolve("extensions").toString());

        try (Connection connection = new DuckDBDriver().connect(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE lake.main.direct_events (id INTEGER, val VARCHAR)");
            statement.execute("CREATE TABLE lake.main.bulk_events (id INTEGER, val VARCHAR)");
            try (PreparedStatement insert =
                    connection.prepareStatement(
                            "INSERT INTO lake.main.direct_events VALUES (?, ?)")) {
                for (int i = 0; i < 100; i++) {
                    insert.setInt(1, i);
                    insert.setString(2, "value-" + i);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
        Assertions.assertEquals(100, parquetCount(dataPath));

        TableSchema schema = schema();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("duckdb", "lake", "main", "bulk_events"),
                        schema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null,
                        "duckdb");
        List<SeaTunnelRow> rows = new ArrayList<>();
        IntStream.range(0, 100)
                .forEach(i -> rows.add(new SeaTunnelRow(new Object[] {i, "value-" + i})));
        Map<String, Object> options = new HashMap<>();
        options.put("url", url);
        options.put("driver", "org.duckdb.DuckDBDriver");
        options.put(
                "properties",
                Collections.singletonMap(
                        "extension_directory", tempDir.resolve("extensions").toString()));
        options.put("database", "lake");
        options.put("table", "main.bulk_events");
        options.put("generate_sink_sql", true);
        options.put("ducklake_bulk_write", true);
        options.put("schema_save_mode", "IGNORE");
        options.put("data_save_mode", "APPEND_DATA");
        options.put("batch_size", 100);
        JdbcSinkFactory factory = new JdbcSinkFactory();
        factory.validateConnectionForDryRun(
                new TableSinkFactoryContext(
                        catalogTable,
                        ReadonlyConfig.fromMap(options),
                        getClass().getClassLoader()));
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable, ReadonlyConfig.fromMap(options), factory, rows);

        Assertions.assertEquals(101, parquetCount(dataPath));
        try (Connection connection = new DuckDBDriver().connect(url, properties);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT COUNT(*), COUNT(DISTINCT id) FROM lake.main.bulk_events")) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals(100, result.getInt(1));
            Assertions.assertEquals(100, result.getInt(2));
        }
    }

    private static TableSchema schema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 22, false, null, "id"))
                .column(PhysicalColumn.of("val", BasicType.STRING_TYPE, 128, true, null, "val"))
                .build();
    }

    private static long parquetCount(Path dataPath) throws Exception {
        try (Stream<Path> files = Files.walk(dataPath)) {
            return files.filter(path -> path.toString().endsWith(".parquet")).count();
        }
    }
}
