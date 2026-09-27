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
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;
import org.apache.seatunnel.connectors.seatunnel.source.SourceFlowTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import lombok.SneakyThrows;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
    public void testUnsignedIntegerRanges() throws Exception {
        String definition = " (a UTINYINT, b USMALLINT, c UINTEGER, d UBIGINT)";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE unsigned_source" + definition);
            statement.execute("CREATE TABLE unsigned_sink" + definition);
            statement.execute(
                    "INSERT INTO unsigned_source VALUES "
                            + "(255,65535,4294967295,18446744073709551615),"
                            + "(128,32768,2147483648,9223372036854775808),"
                            + "(0,0,0,0),(NULL,NULL,NULL,NULL)");
        }
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", "org.duckdb.DuckDBDriver");
        options.put("table_path", "main.unsigned_source");
        List<SeaTunnelRow> rows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(options), new JdbcSourceFactory());
        Object[][] expected = {
            {(short) 255, 65535, 4294967295L, new BigDecimal("18446744073709551615")},
            {(short) 128, 32768, 2147483648L, new BigDecimal("9223372036854775808")},
            {(short) 0, 0, 0L, BigDecimal.ZERO},
            {null, null, null, null}
        };
        Assertions.assertEquals(expected.length, rows.size());
        for (Object[] fields : expected) {
            Assertions.assertTrue(
                    rows.stream().anyMatch(row -> Arrays.deepEquals(fields, row.getFields())),
                    "Missing unsigned row: " + Arrays.toString(fields));
        }
        DuckDBCatalog catalog =
                new DuckDBCatalog(CATALOG_NAME, DuckDBURLParser.parse(jdbcUrl), SCHEMA_NAME);
        catalog.open();
        CatalogTable table;
        try {
            table = catalog.getTable(TablePath.of(DATABASE_NAME, SCHEMA_NAME, "unsigned_source"));
        } finally {
            catalog.close();
        }
        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("url", jdbcUrl);
        sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
        sinkOptions.put("database", SCHEMA_NAME);
        sinkOptions.put("table", "unsigned_sink");
        sinkOptions.put("generate_sink_sql", true);
        sinkOptions.put("schema_save_mode", SchemaSaveMode.IGNORE);
        sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                table, ReadonlyConfig.fromMap(sinkOptions), new JdbcSinkFactory(), rows);
        catalog.open();
        try {
            catalog.createTable(
                    TablePath.of(DATABASE_NAME, SCHEMA_NAME, "unsigned_auto_sink"), table, false);
        } finally {
            catalog.close();
        }
        sinkOptions.put("table", "unsigned_auto_sink");
        sinkOptions.put("schema_save_mode", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                table, ReadonlyConfig.fromMap(sinkOptions), new JdbcSinkFactory(), rows);
        options.put("table_path", "main.unsigned_auto_sink");
        List<SeaTunnelRow> automatic =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(options), new JdbcSourceFactory());
        Assertions.assertEquals(expected.length, automatic.size());
        for (Object[] fields : expected) {
            Assertions.assertTrue(
                    automatic.stream().anyMatch(row -> Arrays.deepEquals(fields, row.getFields())));
        }
        options.put("table_path", "main.unsigned_sink");
        List<SeaTunnelRow> written =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(options), new JdbcSourceFactory());
        Assertions.assertEquals(expected.length, written.size());
        for (Object[] fields : expected) {
            Assertions.assertTrue(
                    written.stream().anyMatch(row -> Arrays.deepEquals(fields, row.getFields())));
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
