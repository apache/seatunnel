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
import org.junit.jupiter.api.parallel.ResourceLock;

import lombok.SneakyThrows;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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

    @Test
    public void testQuerySourceNativeValues() {
        String query =
                "SELECT CAST(12345678901234567890 AS DECIMAL(20,0)) AS amount, "
                        + "TIMESTAMPTZ '2024-01-01 12:34:56.123456+08' AS tz, "
                        + "UUID '550e8400-e29b-41d4-a716-446655440000' AS uuid, "
                        + "JSON '{\"key\":1}' AS json, INTERVAL '1 day' AS interval_value, "
                        + "12345678901234567890::HUGEINT AS huge, "
                        + "[1, 2] AS list_value, {'key': 1} AS struct_value, MAP(['key'], [1]) AS map_value";
        List<SeaTunnelRow> rows = readQuery(query);
        Assertions.assertEquals(1, rows.size());
        Object[] fields = rows.get(0).getFields();
        Assertions.assertEquals(new BigDecimal("12345678901234567890"), fields[0]);
        Assertions.assertEquals(
                OffsetDateTime.parse("2024-01-01T04:34:56.123456Z").toInstant(),
                ((OffsetDateTime) fields[1]).toInstant());
        Assertions.assertEquals("550e8400-e29b-41d4-a716-446655440000", fields[2]);
        Assertions.assertEquals("{\"key\":1}", fields[3]);
        Assertions.assertEquals("1 day", fields[4]);
        Assertions.assertEquals(new BigDecimal("12345678901234567890"), fields[5]);
        Assertions.assertEquals("[1, 2]", fields[6]);
        Assertions.assertEquals("{key=1}", fields[7]);
        Assertions.assertEquals("{key=1}", fields[8]);
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testQuerySourceTimestampAndUnsignedBoundariesInUtc() {
        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            // Local-value timezone handling is independent of metadata discovery (#12592).
            // Validate the existing timestamp alias contract in a deterministic UTC JVM.
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            List<SeaTunnelRow> rows =
                    readQuery(
                            "SELECT CAST('2024-01-01 12:34:56' AS TIMESTAMP_S) AS seconds, "
                                    + "CAST('2024-01-01 12:34:56.123' AS TIMESTAMP_MS) AS millis, "
                                    + "CAST('2024-01-01 12:34:56.123456789' AS TIMESTAMP_NS) AS nanos, "
                                    + "255::UTINYINT AS u8, 65535::USMALLINT AS u16, "
                                    + "4294967295::UINTEGER AS u32, 18446744073709551615::UBIGINT AS u64, "
                                    + "340282366920938463463374607431768211455::UHUGEINT AS u128");
            Assertions.assertEquals(1, rows.size());
            Object[] fields = rows.get(0).getFields();
            Assertions.assertEquals(LocalDateTime.parse("2024-01-01T12:34:56"), fields[0]);
            Assertions.assertEquals(LocalDateTime.parse("2024-01-01T12:34:56.123"), fields[1]);
            Assertions.assertEquals(
                    LocalDateTime.parse("2024-01-01T12:34:56.123456789"), fields[2]);
            Assertions.assertEquals((short) 255, fields[3]);
            Assertions.assertEquals(65535, fields[4]);
            Assertions.assertEquals(4294967295L, fields[5]);
            Assertions.assertEquals(new BigDecimal("18446744073709551615"), fields[6]);
            Assertions.assertEquals("340282366920938463463374607431768211455", fields[7]);
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test
    public void testQuerySourceUnsignedZeroAndNull() {
        Object[] zero =
                readQuery(
                                "SELECT 0::UTINYINT AS u8, 0::USMALLINT AS u16, "
                                        + "0::UINTEGER AS u32, 0::UBIGINT AS u64, 0::UHUGEINT AS u128")
                        .get(0)
                        .getFields();
        Assertions.assertArrayEquals(new Object[] {(short) 0, 0, 0L, BigDecimal.ZERO, "0"}, zero);
        Object[] nulls =
                readQuery(
                                "SELECT NULL::UTINYINT AS u8, NULL::USMALLINT AS u16, "
                                        + "NULL::UINTEGER AS u32, NULL::UBIGINT AS u64, NULL::UHUGEINT AS u128")
                        .get(0)
                        .getFields();
        Assertions.assertArrayEquals(new Object[5], nulls);
    }

    @SneakyThrows
    private List<SeaTunnelRow> readQuery(String query) {
        Map<String, Object> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("driver", "org.duckdb.DuckDBDriver");
        options.put("query", query);
        return SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                ReadonlyConfig.fromMap(options), new JdbcSourceFactory());
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
