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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;
import org.apache.seatunnel.connectors.seatunnel.source.SourceFlowTestUtils;

import org.duckdb.DuckDBDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckDBSourceAndSinkTest {

    @TempDir Path tempDir;

    private static final String DATABASE_NAME = "default";
    private static final String SCHEMA_NAME = "main";
    private static final String SOURCE_TABLE_NAME = "source";
    private static final String SINK_TABLE_NAME = "sink";
    private static final String CATALOG_NAME = "duckdb";
    private static final String DB_FILE = "DuckDBSourceAndSinkTest.db";
    private static String jdbcUrl;

    @BeforeAll
    public void setUp() throws Exception {
        // Delete existing database file if it exists
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        // Setup JDBC connection
        jdbcUrl = "jdbc:duckdb:" + dbFile.getAbsolutePath();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(getCreateTableTemplate(), SCHEMA_NAME, SOURCE_TABLE_NAME));
            statement.execute(
                    String.format(getCreateTableTemplate(), SCHEMA_NAME, SINK_TABLE_NAME));
            for (String insertSql : getInsertRowSql(SCHEMA_NAME, SOURCE_TABLE_NAME)) {
                statement.execute(insertSql);
            }
        }
    }

    @Test
    public void testSourceTimePrecision() throws Exception {
        verifyTimePrecision(true);
    }

    @Test
    public void testSinkTimePrecision() throws Exception {
        verifyTimePrecision(false);
    }

    private void verifyTimePrecision(boolean verifySource) throws Exception {
        TimeZone original = TimeZone.getDefault();
        LocalTime[] times = {
            LocalTime.parse("00:00:00"),
            LocalTime.parse("00:00:00.000001"),
            LocalTime.parse("12:34:56.123456"),
            LocalTime.parse("23:59:59.999999"),
            null
        };
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                try (Connection connection = DriverManager.getConnection(jdbcUrl);
                        Statement statement = connection.createStatement()) {
                    statement.execute(
                            "CREATE OR REPLACE TABLE main.time_precision_source (id INTEGER, value TIME)");
                    statement.execute(
                            "CREATE OR REPLACE TABLE main.time_precision_sink (id INTEGER, value TIME)");
                    statement.execute(
                            "INSERT INTO main.time_precision_source VALUES (0, TIME '00:00:00'), (1, TIME '00:00:00.000001'), (2, TIME '12:34:56.123456'), (3, TIME '23:59:59.999999'), (4, NULL)");
                }
                Map<String, Object> sourceOptions = new HashMap<>();
                sourceOptions.put("url", jdbcUrl);
                sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
                sourceOptions.put("table_path", "main.time_precision_source");
                ReadonlyConfig config = ReadonlyConfig.fromMap(sourceOptions);
                CatalogTable table =
                        new JdbcSourceFactory()
                                .inferSchemaForDryRun(
                                        new TableSourceFactoryContext(
                                                config, getClass().getClassLoader()))
                                .get(0);
                List<SeaTunnelRow> rows =
                        SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                config, new JdbcSourceFactory());
                Assertions.assertEquals(times.length, rows.size());
                if (verifySource) {
                    for (SeaTunnelRow row : rows) {
                        Assertions.assertEquals(
                                times[(Integer) row.getField(0)],
                                row.getField(1),
                                "Source precision in " + zone);
                    }
                }
                // Use the original values independently of Source so a matching truncation on
                // both sides cannot make the Sink regression pass.
                List<SeaTunnelRow> sinkRows = new ArrayList<>();
                for (int id = 0; id < times.length; id++) {
                    sinkRows.add(new SeaTunnelRow(new Object[] {id, times[id]}));
                }
                Map<String, Object> sinkOptions = new HashMap<>();
                sinkOptions.put("url", jdbcUrl);
                sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
                sinkOptions.put("query", "INSERT INTO main.time_precision_sink VALUES (?, ?)");
                sinkOptions.put("schema_save_mode", SchemaSaveMode.IGNORE);
                sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
                SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                        table,
                        ReadonlyConfig.fromMap(sinkOptions),
                        new JdbcSinkFactory(),
                        sinkRows);
                try (Connection connection = DriverManager.getConnection(jdbcUrl);
                        Statement statement = connection.createStatement();
                        ResultSet result =
                                statement.executeQuery(
                                        "SELECT value::VARCHAR FROM main.time_precision_sink ORDER BY id")) {
                    for (LocalTime time : times) {
                        Assertions.assertTrue(result.next());
                        String value = result.getString(1);
                        Assertions.assertEquals(
                                time,
                                value == null ? null : LocalTime.parse(value),
                                "Sink precision in " + zone);
                    }
                    Assertions.assertFalse(result.next());
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @SneakyThrows
    @Test
    public void testFlow() {
        // test source
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("url", jdbcUrl);
        sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
        sourceOptions.put("table_path", String.format("%s.%s", SCHEMA_NAME, SOURCE_TABLE_NAME));
        List<SeaTunnelRow> rows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
        Assertions.assertEquals(2, rows.size());
        // test sink
        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("url", jdbcUrl);
        sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
        sinkOptions.put("schema_save_mode", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
        sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
        sinkOptions.put("database", SCHEMA_NAME);
        sinkOptions.put("table", SINK_TABLE_NAME);
        sinkOptions.put("query", "");
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(jdbcUrl);
        DuckDBCatalog catalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME);
        catalog.open();
        CatalogTable catalogTable =
                catalog.getTable(TablePath.of(DATABASE_NAME, SCHEMA_NAME, SINK_TABLE_NAME));
        catalog.close();
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable, ReadonlyConfig.fromMap(sinkOptions), new JdbcSinkFactory(), rows);
        Assertions.assertEquals(
                2, countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, SINK_TABLE_NAME)));
    }

    @Test
    public void testSinkWithUnattachedUpstreamDatabaseFailsClearly() throws Exception {
        CatalogTable upstream =
                CatalogTable.of(
                        TableIdentifier.of("mysql", "mydb", "tbl"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 10, false, null, null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", "org.duckdb.DuckDBDriver");
        options.put("table", "main.legacy_sink");
        options.put("data_save_mode", DataSaveMode.APPEND_DATA);
        List<SeaTunnelRow> rows = Collections.singletonList(new SeaTunnelRow(new Object[] {1}));
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE main.legacy_sink (id INTEGER)");
            statement.execute("INSERT INTO main.legacy_sink VALUES (42)");
        }
        try {
            for (SchemaSaveMode mode :
                    new SchemaSaveMode[] {
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, SchemaSaveMode.IGNORE
                    }) {
                options.put("schema_save_mode", mode);
                Exception failure =
                        Assertions.assertThrows(
                                Exception.class, () -> prepareSinkSaveMode(upstream, options));
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                Assertions.assertInstanceOf(CatalogException.class, cause);
                Assertions.assertTrue(
                        cause.getMessage().contains("database 'mydb' is not an attached catalog"));
                Assertions.assertTrue(cause.getMessage().contains("set database to main/default"));
                Assertions.assertEquals(
                        1, countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, "legacy_sink")));
            }
            options.put("database", "main");
            options.put("schema_save_mode", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
            prepareSinkSaveMode(upstream, options);
            SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                    upstream, ReadonlyConfig.fromMap(options), new JdbcSinkFactory(), rows);
            Assertions.assertEquals(
                    2, countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, "legacy_sink")));
        } finally {
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                    Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE main.legacy_sink");
            }
        }
    }

    private void prepareSinkSaveMode(CatalogTable upstream, Map<String, Object> options)
            throws Exception {
        JdbcSink sink =
                (JdbcSink)
                        new JdbcSinkFactory()
                                .createSink(
                                        new TableSinkFactoryContext(
                                                upstream,
                                                ReadonlyConfig.fromMap(options),
                                                getClass().getClassLoader()))
                                .createSink();
        try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
            handler.open();
            handler.handleSaveMode();
        }
    }

    @SneakyThrows
    @Test
    public void testAttachedCatalogSourceAndSink() {
        runAttachedCatalogSourceAndSink(false);
    }

    @SneakyThrows
    @Test
    public void testDuckLakeSourceAndSink() {
        Assumptions.assumeTrue(
                System.getProperty("ducklake.extension") != null
                        && System.getProperty("sqlite.scanner.extension") != null);
        runAttachedCatalogSourceAndSink(true);
    }

    @SneakyThrows
    @Test
    public void testDuckLakeInMemorySourceAndSink() {
        Assumptions.assumeTrue(
                System.getProperty("ducklake.extension") != null
                        && System.getProperty("sqlite.scanner.extension") != null);
        runAttachedCatalogSourceAndSink(true, true);
    }

    @SneakyThrows
    @Test
    public void testPostgresS3DuckLakeSourceAndSink() {
        String duckLakeExtension = System.getProperty("ducklake.extension");
        String pgConnection = System.getProperty("ducklake.pg.connection");
        String s3DataPath = System.getProperty("ducklake.s3.data_path");
        String s3Endpoint = System.getProperty("ducklake.s3.endpoint");
        String s3Key = System.getProperty("ducklake.s3.key");
        String s3Secret = System.getProperty("ducklake.s3.secret");
        Assumptions.assumeTrue(
                duckLakeExtension != null
                        && pgConnection != null
                        && s3DataPath != null
                        && s3Endpoint != null
                        && s3Key != null
                        && s3Secret != null);
        List<String> initStatements = new ArrayList<>();
        initStatements.add("LOAD '" + duckLakeExtension.replace("'", "''") + "'");
        initStatements.add("LOAD postgres");
        initStatements.add("LOAD httpfs");
        initStatements.add(
                "CREATE OR REPLACE TEMPORARY SECRET smoke_s3 (TYPE s3, KEY_ID '"
                        + s3Key.replace("'", "''")
                        + "', SECRET '"
                        + s3Secret.replace("'", "''")
                        + "', ENDPOINT '"
                        + s3Endpoint.replace("'", "''")
                        + "', URL_STYLE 'path', USE_SSL false)");
        initStatements.add(
                "ATTACH IF NOT EXISTS 'ducklake:postgres:"
                        + pgConnection.replace("'", "''")
                        + "' AS lake (DATA_PATH '"
                        + s3DataPath.replace("'", "''")
                        + "')");
        runAttachedCatalogSourceAndSink(
                initStatements,
                "route_" + UUID.randomUUID().toString().replace("-", ""),
                Integer.getInteger("ducklake.s3.parallelism", 1));
    }

    @Test
    public void testDuckLakePartitionedSnapshotSource() throws Exception {
        Assumptions.assumeTrue(
                System.getProperty("ducklake.extension") != null
                        && System.getProperty("sqlite.scanner.extension") != null);
        Path data = Files.createDirectory(tempDir.resolve("snapshot-data"));
        List<String> loads = new ArrayList<>();
        loads.add("LOAD '" + System.getProperty("ducklake.extension").replace("'", "''") + "'");
        loads.add(
                "LOAD '" + System.getProperty("sqlite.scanner.extension").replace("'", "''") + "'");
        String attach =
                "ATTACH IF NOT EXISTS 'ducklake:sqlite:"
                        + tempDir.resolve("snapshot.sqlite").toString().replace("'", "''")
                        + "' AS lake (DATA_PATH '"
                        + data.toString().replace("'", "''")
                        + "/'";
        long snapshot;
        try (Connection connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
                Statement statement = connection.createStatement()) {
            executeInitStatements(statement, loads);
            statement.execute(attach + ")");
            statement.execute("CREATE TABLE lake.main.events (id INTEGER)");
            statement.execute("INSERT INTO lake.main.events SELECT range::INTEGER FROM range(12)");
            try (ResultSet result =
                    statement.executeQuery("SELECT max(snapshot_id) FROM lake.snapshots()")) {
                result.next();
                snapshot = result.getLong(1);
            }
            statement.execute("INSERT INTO lake.main.events VALUES (99)");
        }
        for (boolean pinned : new boolean[] {false, true}) {
            Path init = tempDir.resolve(pinned ? "pinned.sql" : "current.sql");
            String sql =
                    "/* DUCKDB_CONNECTION_INIT_BELOW_MARKER */\n"
                            + String.join(";\n", loads)
                            + ";\n"
                            + attach
                            + (pinned ? ", SNAPSHOT_VERSION " + snapshot : "")
                            + ");\n";
            Files.write(init, sql.getBytes(StandardCharsets.UTF_8));
            Map<String, Object> options = new HashMap<>();
            options.put("url", "jdbc:duckdb:;session_init_sql_file=" + init);
            options.put("driver", "org.duckdb.DuckDBDriver");
            options.put("table_path", "lake.main.events");
            options.put("partition_column", "id");
            options.put("partition_num", 3);
            options.put("partition_lower_bound", "0");
            options.put("partition_upper_bound", "12");
            options.put("split.size", 2);
            List<SeaTunnelRow> rows =
                    SourceFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                            ReadonlyConfig.fromMap(options), new JdbcSourceFactory(), 3);
            int expected = pinned ? 12 : 13;
            Assertions.assertEquals(expected, rows.size());
            Assertions.assertEquals(
                    expected, rows.stream().map(row -> row.getField(0)).distinct().count());
            Assertions.assertEquals(
                    !pinned,
                    rows.stream().anyMatch(row -> Integer.valueOf(99).equals(row.getField(0))));
        }
    }

    private void runAttachedCatalogSourceAndSink(boolean duckLake) throws Exception {
        runAttachedCatalogSourceAndSink(duckLake, false);
    }

    private void runAttachedCatalogSourceAndSink(boolean duckLake, boolean inMemory)
            throws Exception {
        Path lakePath = tempDir.resolve("lake.db");
        List<String> initStatements = new ArrayList<>();
        if (duckLake) {
            initStatements.add(
                    "LOAD '" + System.getProperty("ducklake.extension").replace("'", "''") + "'");
            initStatements.add(
                    "LOAD '"
                            + System.getProperty("sqlite.scanner.extension").replace("'", "''")
                            + "'");
            Path dataPath = Files.createDirectory(tempDir.resolve("data"));
            initStatements.add(
                    "ATTACH IF NOT EXISTS 'ducklake:sqlite:"
                            + tempDir.resolve("catalog.sqlite").toString().replace("'", "''")
                            + "' AS lake (DATA_PATH '"
                            + dataPath.toString().replace("'", "''")
                            + "')");
        } else {
            initStatements.add(
                    "ATTACH IF NOT EXISTS '"
                            + lakePath.toString().replace("'", "''")
                            + "' AS lake");
        }
        runAttachedCatalogSourceAndSink(initStatements, "route", duckLake ? 1 : 2, inMemory);
    }

    private void runAttachedCatalogSourceAndSink(
            List<String> initStatements, String tableName, int parallelism) throws Exception {
        runAttachedCatalogSourceAndSink(initStatements, tableName, parallelism, false);
    }

    private void runAttachedCatalogSourceAndSink(
            List<String> initStatements, String tableName, int parallelism, boolean inMemory)
            throws Exception {
        Path localPath = tempDir.resolve("local.db");
        Path initPath = tempDir.resolve("init.sql");
        String localUrl = inMemory ? "jdbc:duckdb:" : "jdbc:duckdb:" + localPath;
        try (Connection connection = new DuckDBDriver().connect(localUrl, new Properties());
                Statement statement = connection.createStatement()) {
            executeInitStatements(statement, initStatements);
            if (!inMemory) {
                statement.execute("CREATE TABLE main." + tableName + " (id INTEGER)");
                statement.execute("INSERT INTO main." + tableName + " VALUES (1)");
            }
            statement.execute("CREATE TABLE lake.main." + tableName + " (id INTEGER)");
            statement.execute("INSERT INTO lake.main." + tableName + " VALUES (2)");
        }
        Files.write(
                initPath,
                ("/* DUCKDB_CONNECTION_INIT_BELOW_MARKER */\n"
                                + String.join(";\n", initStatements)
                                + ";\n")
                        .getBytes(StandardCharsets.UTF_8));
        String attachedUrl = localUrl + ";session_init_sql_file=" + initPath;
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("url", attachedUrl);
        sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
        sourceOptions.put("table_path", "lake.main." + tableName);
        List<SeaTunnelRow> rows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(2, rows.get(0).getField(0));

        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("url", attachedUrl);
        sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
        sinkOptions.put("schema_save_mode", SchemaSaveMode.IGNORE);
        sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
        sinkOptions.put("database", "lake");
        sinkOptions.put("table", "main." + tableName);
        sinkOptions.put("generate_sink_sql", true);
        sinkOptions.put("query", "");
        DuckDBCatalog catalog =
                new DuckDBCatalog(CATALOG_NAME, DuckDBURLParser.parse(attachedUrl), SCHEMA_NAME);
        catalog.open();
        CatalogTable catalogTable;
        try {
            catalogTable = catalog.getTable(TablePath.of("lake", "main", tableName));
        } finally {
            catalog.close();
        }
        SinkFlowTestUtils.runParallelSubtasksBatchWithCheckpointDisabled(
                catalogTable,
                ReadonlyConfig.fromMap(sinkOptions),
                new JdbcSinkFactory(),
                rows,
                parallelism);
        try (Connection connection = new DuckDBDriver().connect(localUrl, new Properties());
                Statement statement = connection.createStatement()) {
            executeInitStatements(statement, initStatements);
            try (ResultSet result =
                    statement.executeQuery(
                            "SELECT SUM(id), COUNT(*) FROM lake.main." + tableName)) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(2 * (parallelism + 1), result.getInt(1));
                Assertions.assertEquals(parallelism + 1, result.getInt(2));
            }
            if (!inMemory) {
                try (ResultSet result =
                        statement.executeQuery("SELECT id FROM main." + tableName)) {
                    Assertions.assertTrue(result.next());
                    Assertions.assertEquals(1, result.getInt(1));
                    Assertions.assertFalse(result.next());
                }
            }
        }
    }

    private void executeInitStatements(Statement statement, List<String> statements)
            throws Exception {
        for (String sql : statements) {
            statement.execute(sql);
        }
    }

    @AfterAll
    public void tearDown() {
        // Delete database file
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    private String getCreateTableTemplate() {
        return "CREATE TABLE \"%s\".\"%s\" (\n"
                + "    c_boolean BOOLEAN,\n"
                + "    c_tinyint     TINYINT,\n"
                + "    c_smallint   SMALLINT,\n"
                + "    c_integer    INTEGER,\n"
                + "    c_bigint     BIGINT,\n"
                + "    c_hugeint    HUGEINT,\n"
                + "    c_utinyint   UTINYINT,\n"
                + "    c_usmallint  USMALLINT,\n"
                + "    c_uinteger   UINTEGER,\n"
                + "    c_ubigint    UBIGINT,\n"
                + "    c_uhugeint   UHUGEINT,\n"
                + "    c_real       REAL,\n"
                + "    c_double     DOUBLE,\n"
                + "    c_decimal    DECIMAL(18, 6),\n"
                + "    c_varchar    VARCHAR,\n"
                + "    c_varchar_n  VARCHAR(100),\n"
                + "    c_text       TEXT,\n"
                + "    c_char       CHAR(10),\n"
                + "    c_bpchar     BPCHAR(10),\n"
                + "    c_blob       BLOB,\n"
                + "    c_date           DATE,\n"
                + "    c_time           TIME,\n"
                + "    c_timestamp      TIMESTAMP,\n"
                + "    c_timestamptz    TIMESTAMP WITH TIME ZONE,\n"
                + "    c_interval       INTERVAL,\n"
                + "    c_uuid       UUID\n"
                + ");\n";
    }

    private List<String> getInsertRowSql(String schemaName, String tableName) {
        List<String> insertSqls = new ArrayList<>();
        insertSqls.add(
                String.format(
                        "INSERT INTO \"%s\".\"%s\" VALUES (\n"
                                + "    TRUE,\n"
                                + "    1,\n"
                                + "    2,\n"
                                + "    3,\n"
                                + "    4,\n"
                                + "    5,\n"
                                + "    6,\n"
                                + "    7,\n"
                                + "    8,\n"
                                + "    9,\n"
                                + "    10,\n"
                                + "    1.23,\n"
                                + "    4.56,\n"
                                + "    12345.678901,\n"
                                + "    'hello',\n"
                                + "    'varchar_100',\n"
                                + "    'text_value',\n"
                                + "    'char10',\n"
                                + "    'bpchar10',\n"
                                + "    X'010203',\n"
                                + "    DATE '2024-01-01',\n"
                                + "    TIME '12:34:56',\n"
                                + "    TIMESTAMP '2024-01-01 12:34:56',\n"
                                + "    TIMESTAMPTZ '2024-01-01 12:34:56+08',\n"
                                + "    INTERVAL '1 day 2 hours 3 minutes',\n"
                                + "    '550e8400-e29b-41d4-a716-446655440000'\n"
                                + ");",
                        schemaName, tableName));
        insertSqls.add(
                String.format(
                        "INSERT INTO \"%s\".\"%s\" VALUES (\n"
                                + "    FALSE,\n"
                                + "    -1,\n"
                                + "    -2,\n"
                                + "    -3,\n"
                                + "    -4,\n"
                                + "    -5,\n"
                                + "    1,\n"
                                + "    2,\n"
                                + "    3,\n"
                                + "    4,\n"
                                + "    5,\n"
                                + "    -1.23,\n"
                                + "    -4.56,\n"
                                + "    -98765.432100,\n"
                                + "    'world',\n"
                                + "    'varchar_test',\n"
                                + "    'another_text',\n"
                                + "    'char_val',\n"
                                + "    'bpcharval',\n"
                                + "    X'0A0B0C',\n"
                                + "    DATE '2025-06-30',\n"
                                + "    TIME '23:59:59',\n"
                                + "    TIMESTAMP '2025-06-30 23:59:59',\n"
                                + "    TIMESTAMPTZ '2025-06-30 23:59:59+00',\n"
                                + "    INTERVAL '2 days 4 hours',\n"
                                + "    '123e4567-e89b-12d3-a456-426614174000'\n"
                                + ");",
                        schemaName, tableName));
        return insertSqls;
    }

    private int countRows(TablePath tablePath) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                String.format(
                                        "SELECT COUNT(*) FROM \"%s\".\"%s\"",
                                        tablePath.getSchemaName(), tablePath.getTableName()))) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (Exception e) {
            throw new RuntimeException("Failed to count rows for " + tablePath, e);
        }
    }
}
