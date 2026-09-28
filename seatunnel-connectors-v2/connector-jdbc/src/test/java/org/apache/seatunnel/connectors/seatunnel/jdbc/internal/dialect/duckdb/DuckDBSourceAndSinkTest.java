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
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode.JdbcSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;
import org.apache.seatunnel.connectors.seatunnel.source.SourceFlowTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckDBSourceAndSinkTest {

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
    public void testDropDataSaveMode() throws Exception {
        verifySaveMode(
                "drop_data", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, DataSaveMode.DROP_DATA);
    }

    @Test
    public void testRecreateSchemaSaveMode() throws Exception {
        verifySaveMode("recreate", SchemaSaveMode.RECREATE_SCHEMA, DataSaveMode.APPEND_DATA);
    }

    @Test
    public void testAppendDataSaveMode() throws Exception {
        verifySaveMode(
                "append", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, DataSaveMode.APPEND_DATA);
    }

    @Test
    public void testErrorWhenDataExistsSaveMode() throws Exception {
        String tableName = "save_mode_error";
        Map<String, Object> options =
                saveModeOptions(
                        tableName,
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.ERROR_WHEN_DATA_EXISTS);
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE main." + tableName + " (id INTEGER)");
        }
        try {
            prepareSaveMode(options);
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                    Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO main." + tableName + " VALUES (99)");
            }
            SeaTunnelRuntimeException failure =
                    Assertions.assertThrows(
                            SeaTunnelRuntimeException.class, () -> prepareSaveMode(options));
            Assertions.assertTrue(failure.getMessage().contains("already has data"));
            Assertions.assertEquals(
                    1, countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, tableName)));
        } finally {
            dropSaveModeTable(tableName);
        }
    }

    /** Opt-in: executes save-mode operations against the pinned DuckLake extension, not a mock. */
    @Test
    public void testDuckLakeSaveModeOperations(@TempDir Path directory) throws Exception {
        Assumptions.assumeTrue(
                System.getProperty("ducklake.extension") != null
                        && System.getProperty("sqlite.scanner.extension") != null);
        String url = "jdbc:duckdb:" + directory.resolve("lake-host.db");
        TablePath table = TablePath.of("lake", SCHEMA_NAME, "save_mode_target");
        try (DuckDBCatalog lakeCatalog =
                new DuckDBCatalog(CATALOG_NAME, DuckDBURLParser.parse(url), SCHEMA_NAME)) {
            lakeCatalog.open();
            try (Statement statement = lakeCatalog.getConnection(url).createStatement();
                    SaveModeHandler clear =
                            new JdbcSaveModeHandler(
                                    SchemaSaveMode.IGNORE,
                                    DataSaveMode.DROP_DATA,
                                    lakeCatalog,
                                    table,
                                    saveModeInput(),
                                    null,
                                    false);
                    SaveModeHandler reject =
                            new JdbcSaveModeHandler(
                                    SchemaSaveMode.IGNORE,
                                    DataSaveMode.ERROR_WHEN_DATA_EXISTS,
                                    lakeCatalog,
                                    table,
                                    saveModeInput(),
                                    null,
                                    false)) {
                statement.execute(
                        "LOAD '"
                                + System.getProperty("ducklake.extension").replace("'", "''")
                                + "'");
                statement.execute(
                        "LOAD '"
                                + System.getProperty("sqlite.scanner.extension").replace("'", "''")
                                + "'");
                statement.execute(
                        "ATTACH 'ducklake:sqlite:"
                                + directory.resolve("metadata.sqlite").toString().replace("'", "''")
                                + "' AS lake (DATA_PATH '"
                                + directory.resolve("data").toString().replace("'", "''")
                                + "')");
                statement.execute("CREATE TABLE main.save_mode_target (id INTEGER)");
                statement.execute("INSERT INTO main.save_mode_target VALUES (42)");
                statement.execute("CREATE TABLE lake.main.save_mode_target (id INTEGER)");
                statement.execute("INSERT INTO lake.main.save_mode_target VALUES (7)");
                clear.open();
                clear.handleSaveMode();
                Assertions.assertFalse(lakeCatalog.isExistsData(table));
                reject.open();
                reject.handleSaveMode();
                statement.execute("INSERT INTO lake.main.save_mode_target VALUES (8)");
                Assertions.assertThrows(SeaTunnelRuntimeException.class, reject::handleSaveMode);
                try (ResultSet result =
                        statement.executeQuery("SELECT id FROM lake.main.save_mode_target")) {
                    Assertions.assertTrue(result.next());
                    Assertions.assertEquals(8, result.getInt(1));
                    Assertions.assertFalse(result.next());
                }
                lakeCatalog.dropTable(table, false);
                try (ResultSet result =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'lake' AND table_name = 'save_mode_target'")) {
                    Assertions.assertTrue(result.next());
                    Assertions.assertEquals(0, result.getInt(1));
                }
                try (ResultSet result =
                        statement.executeQuery("SELECT id FROM main.save_mode_target")) {
                    Assertions.assertTrue(result.next());
                    Assertions.assertEquals(42, result.getInt(1));
                    Assertions.assertFalse(result.next());
                }
            }
        }
    }

    private void verifySaveMode(String suffix, SchemaSaveMode schemaMode, DataSaveMode dataMode)
            throws Exception {
        String tableName = "save_mode_" + suffix;
        Map<String, Object> options = saveModeOptions(tableName, schemaMode, dataMode);
        boolean recreate = schemaMode == SchemaSaveMode.RECREATE_SCHEMA;
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE main."
                            + tableName
                            + " (id INTEGER"
                            + (recreate ? ", obsolete VARCHAR" : "")
                            + ")");
            statement.execute("INSERT INTO main." + tableName + " (id) VALUES (99)");
        }
        try {
            prepareSaveMode(options);
            boolean retainsOldRow = !recreate && dataMode == DataSaveMode.APPEND_DATA;
            Assertions.assertEquals(
                    retainsOldRow ? 1 : 0,
                    countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, tableName)));
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT * FROM main." + tableName)) {
                Assertions.assertEquals(1, result.getMetaData().getColumnCount());
            }
            SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                    saveModeInput(),
                    ReadonlyConfig.fromMap(options),
                    new JdbcSinkFactory(),
                    Collections.singletonList(new SeaTunnelRow(new Object[] {1})));
            Assertions.assertEquals(
                    retainsOldRow ? 2 : 1,
                    countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, tableName)));
        } finally {
            dropSaveModeTable(tableName);
        }
    }

    private Map<String, Object> saveModeOptions(
            String tableName, SchemaSaveMode schemaMode, DataSaveMode dataMode) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", "org.duckdb.DuckDBDriver");
        options.put("database", DATABASE_NAME);
        options.put("table", SCHEMA_NAME + "." + tableName);
        options.put("schema_save_mode", schemaMode);
        options.put("data_save_mode", dataMode);
        return options;
    }

    private CatalogTable saveModeInput() {
        return CatalogTable.of(
                TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, SCHEMA_NAME, "input"),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 10, false, null, null))
                        .build(),
                new HashMap<>(),
                Collections.emptyList(),
                null);
    }

    private void prepareSaveMode(Map<String, Object> options) throws Exception {
        JdbcSink sink =
                (JdbcSink)
                        new JdbcSinkFactory()
                                .createSink(
                                        new TableSinkFactoryContext(
                                                saveModeInput(),
                                                ReadonlyConfig.fromMap(options),
                                                getClass().getClassLoader()))
                                .createSink();
        try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
            handler.open();
            handler.handleSaveMode();
        }
    }

    private void dropSaveModeTable(String tableName) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS main." + tableName);
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
