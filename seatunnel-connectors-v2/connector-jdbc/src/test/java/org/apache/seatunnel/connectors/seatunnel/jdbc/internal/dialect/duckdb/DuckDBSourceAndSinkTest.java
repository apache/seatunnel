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
import org.apache.seatunnel.api.table.type.LocalTimeType;
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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.mockito.Mockito;

import lombok.SneakyThrows;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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

    private List<SeaTunnelRow> readTablePath(String tablePath) throws Exception {
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("url", jdbcUrl);
        sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
        sourceOptions.put("table_path", tablePath);
        return SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testTimestampAliasesAcrossTimeZones() throws Exception {
        String tablePath = SCHEMA_NAME + ".ts_alias";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"ts_alias\" (c_ts TIMESTAMP, c_ts_s TIMESTAMP_S, "
                                    + "c_ts_ms TIMESTAMP_MS, c_ts_ns TIMESTAMP_NS, c_ts_null TIMESTAMP_NS)",
                            SCHEMA_NAME));
            statement.execute(
                    String.format(
                            "INSERT INTO \"%s\".\"ts_alias\" VALUES ("
                                    + "TIMESTAMP '2024-01-01 12:34:56.123456', "
                                    + "TIMESTAMP_S '2024-01-01 12:34:56', "
                                    + "TIMESTAMP_MS '2024-01-01 12:34:56.123', "
                                    + "TIMESTAMP_NS '2024-01-01 12:34:56.123456789', NULL)",
                            SCHEMA_NAME));
        }
        // Explicit wall-clock expectations; not derived from the read path under test.
        LocalDateTime[] expected = {
            LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123456000),
            LocalDateTime.of(2024, 1, 1, 12, 34, 56),
            LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123000000),
            LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123456789)
        };
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                List<SeaTunnelRow> rows = readTablePath(tablePath);
                Assertions.assertEquals(1, rows.size());
                SeaTunnelRow row = rows.get(0);
                for (int index = 0; index < expected.length; index++) {
                    Assertions.assertEquals(expected[index], row.getField(index), zone);
                }
                Assertions.assertNull(row.getField(4), zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testCatalogResolvesTimestampAliases() throws Exception {
        String table = "ts_alias_catalog";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (c_ts TIMESTAMP, c_ts_s TIMESTAMP_S, "
                                    + "c_ts_ms TIMESTAMP_MS, c_ts_ns TIMESTAMP_NS)",
                            SCHEMA_NAME, table));
        }
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(jdbcUrl);
        CatalogTable catalogTable;
        try (DuckDBCatalog catalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME)) {
            catalog.open();
            catalogTable = catalog.getTable(TablePath.of(DATABASE_NAME, SCHEMA_NAME, table));
        }
        for (String name : new String[] {"c_ts", "c_ts_s", "c_ts_ms", "c_ts_ns"}) {
            Assertions.assertEquals(
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    catalogTable.getTableSchema().getColumn(name).getDataType(),
                    name);
        }
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testQueryTimestampAliasesAcrossTimeZones() throws Exception {
        String query =
                "SELECT CAST('2024-01-01 12:34:56' AS TIMESTAMP_S) AS s, "
                        + "CAST('2024-01-01 12:34:56.123' AS TIMESTAMP_MS) AS ms, "
                        + "CAST('2024-01-01 12:34:56.123456789' AS TIMESTAMP_NS) AS ns, "
                        + "CAST(NULL AS TIMESTAMP_NS) AS n";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("url", jdbcUrl);
        sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
        sourceOptions.put("query", query);

        LocalDateTime expectedSec = LocalDateTime.of(2024, 1, 1, 12, 34, 56, 0);
        LocalDateTime expectedMs = LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123_000_000);
        LocalDateTime expectedNs = LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123_456_789);

        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                List<SeaTunnelRow> rows =
                        SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
                Assertions.assertEquals(1, rows.size(), zone);
                SeaTunnelRow row = rows.get(0);
                Assertions.assertEquals(expectedSec, row.getField(0), zone);
                Assertions.assertEquals(expectedMs, row.getField(1), zone);
                Assertions.assertEquals(expectedNs, row.getField(2), zone);
                Assertions.assertNull(row.getField(3), zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testTimestampGetterFallbackIsScopedToResultSetColumn() throws Exception {
        DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
        ResultSet first = Mockito.mock(ResultSet.class);
        LocalDateTime expected = LocalDateTime.of(2024, 1, 1, 12, 34, 56, 123000000);
        Mockito.when(first.getObject(1, LocalDateTime.class))
                .thenReturn(null)
                .thenThrow(new SQLException("Unsupported timestamp alias"));
        Mockito.when(first.getTimestamp(1)).thenReturn(Timestamp.valueOf(expected));
        Mockito.when(first.getObject(2, LocalDateTime.class)).thenReturn(expected);

        Assertions.assertNull(converter.readTimestamp(first, 1));
        for (int row = 0; row < 3; row++) {
            Assertions.assertEquals(expected, converter.readTimestamp(first, 1));
            Assertions.assertEquals(expected, converter.readTimestamp(first, 2));
        }
        Mockito.verify(first, Mockito.times(2)).getObject(1, LocalDateTime.class);
        Mockito.verify(first, Mockito.times(3)).getTimestamp(1);
        Mockito.verify(first, Mockito.times(3)).getObject(2, LocalDateTime.class);
        Mockito.verify(first, Mockito.never()).getTimestamp(2);

        ResultSet second = Mockito.mock(ResultSet.class);
        Mockito.when(second.getObject(1, LocalDateTime.class)).thenReturn(expected);
        Assertions.assertEquals(expected, converter.readTimestamp(second, 1));
        Mockito.verify(second).getObject(1, LocalDateTime.class);
        Mockito.verify(second, Mockito.never()).getTimestamp(1);
    }

    @Test
    public void testUnsupportedTimestampGetterPreservesNulls() throws Exception {
        DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1, LocalDateTime.class))
                .thenThrow(new UnsupportedOperationException("Typed getter unavailable"));
        Assertions.assertNull(converter.readTimestamp(resultSet, 1));
        Assertions.assertNull(converter.readTimestamp(resultSet, 1));
        Mockito.verify(resultSet).getObject(1, LocalDateTime.class);
        Mockito.verify(resultSet, Mockito.times(2)).getTimestamp(1);
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testTimestampGapAndGregorianCutoverAcrossTimeZones() throws Exception {
        String tablePath = SCHEMA_NAME + ".ts_wall_clock";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE main.ts_wall_clock (id INTEGER, value TIMESTAMP)");
            statement.execute(
                    "INSERT INTO main.ts_wall_clock VALUES "
                            + "(1, TIMESTAMP '2024-03-10 02:30:00.123456'), "
                            + "(2, TIMESTAMP '2024-11-03 01:30:00.123456'), "
                            + "(3, TIMESTAMP '1582-10-10 12:34:56'), "
                            + "(4, NULL)");
        }
        LocalDateTime[] expected = {
            LocalDateTime.of(2024, 3, 10, 2, 30, 0, 123456000),
            LocalDateTime.of(2024, 11, 3, 1, 30, 0, 123456000),
            LocalDateTime.of(1582, 10, 10, 12, 34, 56),
            null
        };
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (boolean query : new boolean[] {false, true}) {
                    Map<String, Object> sourceOptions = new HashMap<>();
                    sourceOptions.put("url", jdbcUrl);
                    sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
                    sourceOptions.put(
                            query ? "query" : "table_path",
                            query ? "SELECT id, value FROM main.ts_wall_clock" : tablePath);
                    List<SeaTunnelRow> rows =
                            SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                    ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
                    Assertions.assertEquals(expected.length, rows.size(), zone);
                    for (SeaTunnelRow row : rows) {
                        int id = (Integer) row.getField(0);
                        Assertions.assertEquals(
                                expected[id - 1], row.getField(1), zone + ", query=" + query);
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
