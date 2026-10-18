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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode.JdbcSaveModeHandler;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
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

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testSinkAutoCreatedTimestampColumnsKeepNanos() throws Exception {
        String table = "ts_scale_sink";
        String tablePath = SCHEMA_NAME + "." + table;
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", SCHEMA_NAME, table));
        }

        // The upstream schema declares one column per native DuckDB fractional-second precision.
        // The declared scale is the only precision information the sink has when it creates the
        // target table, so the automatic DDL has to carry it over.
        CatalogTable upstreamTable =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, SCHEMA_NAME, table),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.INT_TYPE)
                                                .build())
                                .column(
                                        timestampColumn(
                                                "c_ts_s", DuckDBTypeConverter.TIMESTAMP_S_SCALE))
                                .column(
                                        timestampColumn(
                                                "c_ts_ms", DuckDBTypeConverter.TIMESTAMP_MS_SCALE))
                                .column(
                                        timestampColumn(
                                                "c_ts", DuckDBTypeConverter.TIMESTAMP_SCALE))
                                .column(
                                        timestampColumn(
                                                "c_ts_ns", DuckDBTypeConverter.TIMESTAMP_NS_SCALE))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);

        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("url", jdbcUrl);
        sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
        sinkOptions.put("database", DATABASE_NAME);
        sinkOptions.put("table", tablePath);
        sinkOptions.put("query", "");
        sinkOptions.put("schema_save_mode", SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
        sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
        ReadonlyConfig sinkConfig = ReadonlyConfig.fromMap(sinkOptions);

        // The target table does not exist yet, so the production schema save mode creates it.
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(jdbcUrl);
        DuckDBCatalog writeCatalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME);
        SaveModeHandler saveModeHandler =
                new JdbcSaveModeHandler(
                        SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                        DataSaveMode.APPEND_DATA,
                        writeCatalog,
                        TablePath.of(DATABASE_NAME, SCHEMA_NAME, table),
                        upstreamTable,
                        null,
                        false);
        try {
            saveModeHandler.open();
            saveModeHandler.handleSchemaSaveMode();
        } finally {
            saveModeHandler.close();
        }

        // 1. The created columns are the native DuckDB types, discovered from the database.
        String[] columnNames = {"c_ts_s", "c_ts_ms", "c_ts", "c_ts_ns"};
        String[] nativeTypes = {
            DuckDBTypeConverter.DUCKDB_TIMESTAMP_S,
            DuckDBTypeConverter.DUCKDB_TIMESTAMP_MS,
            DuckDBTypeConverter.DUCKDB_TIMESTAMP,
            DuckDBTypeConverter.DUCKDB_TIMESTAMP_NS
        };
        Integer[] nativeScales = {
            DuckDBTypeConverter.TIMESTAMP_S_SCALE,
            DuckDBTypeConverter.TIMESTAMP_MS_SCALE,
            DuckDBTypeConverter.TIMESTAMP_SCALE,
            DuckDBTypeConverter.TIMESTAMP_NS_SCALE
        };
        for (int index = 0; index < columnNames.length; index++) {
            Assertions.assertEquals(
                    nativeTypes[index],
                    readColumnDataType(table, columnNames[index]),
                    columnNames[index]);
        }
        DuckDBCatalog readCatalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME);
        CatalogTable created;
        try {
            readCatalog.open();
            created = readCatalog.getTable(TablePath.of(DATABASE_NAME, SCHEMA_NAME, table));
        } finally {
            readCatalog.close();
        }
        for (int index = 0; index < columnNames.length; index++) {
            String name = columnNames[index];
            Assertions.assertEquals(
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    created.getTableSchema().getColumn(name).getDataType(),
                    name);
            Assertions.assertEquals(
                    nativeScales[index], created.getTableSchema().getColumn(name).getScale(), name);
        }

        // 2. Write explicit values through the real sink: a normal value, a pre-epoch value, an
        // all-NULL row and one nanosecond after the epoch.
        LocalDateTime base = LocalDateTime.of(2024, 1, 1, 12, 34, 56);
        LocalDateTime beforeEpoch = LocalDateTime.of(1969, 12, 31, 23, 59, 59);
        LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        List<SeaTunnelRow> rows = new ArrayList<>();
        rows.add(timestampSinkRow(1, base, 123_000_000, 123_456_000, 123_456_789));
        rows.add(timestampSinkRow(2, beforeEpoch, 123_000_000, 123_456_000, 123_456_789));
        rows.add(timestampSinkRow(3, null, 0, 0, 0));
        rows.add(timestampSinkRow(4, epoch, 0, 0, 1));
        // Values wider than the destination column retain DuckDB's native narrowing behavior.
        rows.add(
                timestampSinkRow(
                        5, base.withNano(999_999_999), 999_999_999, 999_999_999, 999_999_999));
        rows.add(timestampSinkRow(6, base.withNano(1_500), 1_500, 1_500, 1_500));
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                upstreamTable, sinkConfig, new JdbcSinkFactory(), rows);
        Assertions.assertEquals(6, countRows(TablePath.of(DATABASE_NAME, SCHEMA_NAME, table)));

        // 3. Read the stored values back with independent SQL, outside the connector read path, so
        // a symmetric truncation cannot make the expectations below pass.
        LocalDateTime[][] expected = {
            {
                base,
                base.withNano(123_000_000),
                base.withNano(123_456_000),
                base.withNano(123_456_789)
            },
            {
                beforeEpoch,
                beforeEpoch.withNano(123_000_000),
                beforeEpoch.withNano(123_456_000),
                beforeEpoch.withNano(123_456_789)
            },
            {null, null, null, null}
        };
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT id, c_ts_s::VARCHAR, c_ts_ms::VARCHAR, c_ts::VARCHAR, "
                                        + "c_ts_ns::VARCHAR FROM main."
                                        + table
                                        + " WHERE id <= 3 ORDER BY id")) {
            for (LocalDateTime[] row : expected) {
                Assertions.assertTrue(result.next());
                for (int index = 0; index < columnNames.length; index++) {
                    String message = "id " + result.getInt(1) + ", " + columnNames[index];
                    String value = result.getString(index + 2);
                    if (row[index] == null) {
                        Assertions.assertNull(value, message);
                        continue;
                    }
                    // Parsed from DuckDB's own text rendering, so a value that lost fractional
                    // seconds on the way in cannot match.
                    Assertions.assertEquals(
                            row[index], LocalDateTime.parse(value.replace(' ', 'T')), message);
                }
            }
            Assertions.assertFalse(result.next());
        }

        // 4. The nanosecond column keeps all nine digits: DuckDB's own literals are used, so a
        // microsecond truncation (or the rounding a typed binding applies) fails these.
        assertNanosStored(table, 1, "2024-01-01 12:34:56.123456789");
        assertNanosStored(table, 2, "1969-12-31 23:59:59.123456789");
        assertNanosStored(table, 4, "1970-01-01 00:00:00.000000001");
        assertNanosStored(table, 5, "2024-01-01 12:34:56.999999999");
        assertNanosStored(table, 6, "2024-01-01 12:34:56.000001500");
        Assertions.assertEquals(
                1,
                countTimestampRows(
                        table,
                        "id = 5 AND c_ts_s = TIMESTAMP_S '2024-01-01 12:34:57'"
                                + " AND c_ts_ms = TIMESTAMP_MS '2024-01-01 12:34:57'"
                                + " AND c_ts = TIMESTAMP '2024-01-01 12:34:56.999999'"));
        Assertions.assertEquals(
                1,
                countTimestampRows(
                        table,
                        "id = 6 AND c_ts_s = TIMESTAMP_S '2024-01-01 12:34:56'"
                                + " AND c_ts_ms = TIMESTAMP_MS '2024-01-01 12:34:56'"
                                + " AND c_ts = TIMESTAMP '2024-01-01 12:34:56.000001'"));
        Assertions.assertEquals(
                0,
                countTimestampRows(
                        table, "id = 1 AND c_ts_ns = TIMESTAMP_NS '2024-01-01 12:34:56.123456'"));
        Assertions.assertEquals(
                0,
                countTimestampRows(
                        table, "id = 4 AND c_ts_ns = TIMESTAMP_NS '1970-01-01 00:00:00'"));
    }

    /**
     * Regression: the sink stores the wall clock of a {@code LocalDateTime} unchanged, in every JVM
     * default time zone.
     *
     * <p>The tested DuckDB JDBC driver (1.3.1.0) interprets a typed {@code setTimestamp} binding
     * through the JVM default zone. A local time that does not exist in that zone is therefore
     * moved forward by the DST transition, and the 1582 Julian-to-Gregorian cutover is normalized
     * even in UTC. Most rows are exact to the microsecond and exercise the typed binding used
     * before this fix. Rows 1 and 2 are the same logical clock: they differ only in the digits
     * below the microsecond, which must not decide whether the stored clock is correct. The stored
     * values are verified with DuckDB's own text rendering and its own timestamp literals instead
     * of the connector read path, so a matching defect on both sides cannot make this test pass.
     */
    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testSinkTimestampKeepsWallClockAcrossDstGapsAndCutover() throws Exception {
        String tableName = "ts_sink_wall_clock";
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", SCHEMA_NAME, tableName));
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, value TIMESTAMP)",
                            SCHEMA_NAME, tableName));
        }
        // JVM default zone, bound value and expected stored wall clock. Cases cover US and
        // Lord Howe DST gaps, the Gregorian cutover, a normal control and NULL.
        String[] zones = {
            "America/New_York",
            "America/New_York",
            "America/Los_Angeles",
            "Australia/Lord_Howe",
            "UTC",
            "UTC",
            "UTC"
        };
        String[] bound = {
            "2024-03-10 02:30:00.123456",
            "2024-03-10 02:30:00.123456789",
            "2024-03-10 02:30:00.123456",
            "2024-10-06 02:15:00.123456",
            "1582-10-10 12:34:56.123456",
            "2024-01-01 12:34:56.123456",
            null
        };
        // A TIMESTAMP column keeps six fractional digits, so row 2 is narrowed by DuckDB. The
        // narrowing is asserted, not the precision: row 1 and row 2 must still agree.
        String[] stored = {
            "2024-03-10 02:30:00.123456",
            "2024-03-10 02:30:00.123456",
            "2024-03-10 02:30:00.123456",
            "2024-10-06 02:15:00.123456",
            "1582-10-10 12:34:56.123456",
            "2024-01-01 12:34:56.123456",
            null
        };
        CatalogTable sinkTable =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, SCHEMA_NAME, tableName),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.INT_TYPE)
                                                .build())
                                .column(
                                        timestampColumn(
                                                "value", DuckDBTypeConverter.TIMESTAMP_SCALE))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);
        Map<String, Object> sinkOptions = new HashMap<>();
        sinkOptions.put("url", jdbcUrl);
        sinkOptions.put("driver", "org.duckdb.DuckDBDriver");
        sinkOptions.put(
                "query",
                String.format("INSERT INTO \"%s\".\"%s\" VALUES (?, ?)", SCHEMA_NAME, tableName));
        sinkOptions.put("schema_save_mode", SchemaSaveMode.IGNORE);
        sinkOptions.put("data_save_mode", DataSaveMode.APPEND_DATA);
        ReadonlyConfig sinkConfig = ReadonlyConfig.fromMap(sinkOptions);
        // 1. Write every row through the production sink while its own zone is the JVM default.
        TimeZone original = TimeZone.getDefault();
        try {
            for (int index = 0; index < zones.length; index++) {
                TimeZone.setDefault(TimeZone.getTimeZone(zones[index]));
                LocalDateTime value =
                        bound[index] == null
                                ? null
                                : LocalDateTime.parse(bound[index].replace(' ', 'T'));
                List<SeaTunnelRow> rows = new ArrayList<>();
                rows.add(new SeaTunnelRow(new Object[] {index + 1, value}));
                SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                        sinkTable, sinkConfig, new JdbcSinkFactory(), rows);
            }
        } finally {
            TimeZone.setDefault(original);
        }

        // 2. Verify against DuckDB's own literals. Reading the rows back needs no particular JVM
        // zone, and the readback only makes a failure readable - the expectation is the literal.
        List<String> mismatches = new ArrayList<>();
        for (int index = 0; index < zones.length; index++) {
            int id = index + 1;
            String where =
                    stored[index] == null
                            ? "id = " + id + " AND value IS NULL"
                            : "id = " + id + " AND value = TIMESTAMP '" + stored[index] + "'";
            if (countTimestampRows(tableName, where) != 1) {
                mismatches.add(
                        zones[index]
                                + ", id "
                                + id
                                + ", bound '"
                                + bound[index]
                                + "', expected '"
                                + stored[index]
                                + "' but stored '"
                                + readTimestampText(tableName, id)
                                + "'");
            }
        }
        Assertions.assertTrue(mismatches.isEmpty(), String.join("; ", mismatches));
    }

    /** Reads one stored timestamp through DuckDB's own text rendering. */
    private String readTimestampText(String table, int id) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                String.format(
                                        "SELECT value::VARCHAR FROM \"%s\".\"%s\" WHERE id = %d",
                                        SCHEMA_NAME, table, id))) {
            Assertions.assertTrue(result.next(), table + ", id " + id);
            String text = result.getString(1);
            Assertions.assertFalse(result.next(), table + ", id " + id + ", duplicate row");
            return text;
        }
    }

    /** A timestamp column whose declared precision is the only information the sink DDL gets. */
    private Column timestampColumn(String name, int scale) {
        return PhysicalColumn.builder()
                .name(name)
                .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                .nullable(true)
                .scale(scale)
                .build();
    }

    /**
     * Builds one sink row from wall clocks: the four fractional values of {@code wallClock} for the
     * second, millisecond, microsecond and nanosecond columns. A null wall clock is an all-NULL
     * row.
     */
    private SeaTunnelRow timestampSinkRow(
            int id, LocalDateTime wallClock, int milliseconds, int microseconds, int nanoseconds) {
        if (wallClock == null) {
            return new SeaTunnelRow(new Object[] {id, null, null, null, null});
        }
        return new SeaTunnelRow(
                new Object[] {
                    id,
                    wallClock,
                    wallClock.withNano(milliseconds),
                    wallClock.withNano(microseconds),
                    wallClock.withNano(nanoseconds)
                });
    }

    /** Reads the type DuckDB itself reports for a column of an existing table. */
    private String readColumnDataType(String table, String columnName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                String.format(
                                        "SELECT data_type FROM information_schema.columns WHERE"
                                                + " table_schema = '%s' AND table_name = '%s' AND"
                                                + " column_name = '%s'",
                                        SCHEMA_NAME, table, columnName))) {
            Assertions.assertTrue(result.next(), columnName);
            return result.getString(1);
        }
    }

    /** Asserts that the nanosecond column holds exactly the given DuckDB literal. */
    private void assertNanosStored(String table, int id, String literal) throws SQLException {
        String where = String.format("id = %d AND c_ts_ns = TIMESTAMP_NS '%s'", id, literal);
        Assertions.assertEquals(1, countTimestampRows(table, where), where);
    }

    /** Counts the rows of the timestamp table that match an independent SQL predicate. */
    private int countTimestampRows(String table, String where) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                String.format(
                                        "SELECT COUNT(*) FROM main.%s WHERE %s", table, where))) {
            result.next();
            return result.getInt(1);
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
        String[] names = {"c_ts", "c_ts_s", "c_ts_ms", "c_ts_ns"};
        // Native fractional-second precision reported by the real DuckDB catalog.
        Integer[] scales = {
            DuckDBTypeConverter.TIMESTAMP_SCALE,
            DuckDBTypeConverter.TIMESTAMP_S_SCALE,
            DuckDBTypeConverter.TIMESTAMP_MS_SCALE,
            DuckDBTypeConverter.TIMESTAMP_NS_SCALE
        };
        for (int index = 0; index < names.length; index++) {
            String name = names[index];
            Assertions.assertEquals(
                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                    catalogTable.getTableSchema().getColumn(name).getDataType(),
                    name);
            Assertions.assertEquals(
                    scales[index], catalogTable.getTableSchema().getColumn(name).getScale(), name);
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
    public void testGenericSqlExceptionOnTypedTimestampReadPropagates() throws Exception {
        DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(metadata);
        // Column 1 is a real standard TIMESTAMP, so a driver failure on the typed read is a data
        // error and not a missing typed-getter capability.
        Mockito.when(metadata.getColumnTypeName(1)).thenReturn("TIMESTAMP");
        Mockito.when(metadata.getColumnType(1)).thenReturn(Types.TIMESTAMP);

        SQLException failure = new SQLException("boom", "HY000", 1234);
        Mockito.when(resultSet.getObject(1, LocalDateTime.class)).thenThrow(failure);
        LocalDateTime other = LocalDateTime.of(2024, 1, 1, 12, 34, 56);
        Mockito.when(resultSet.getObject(2, LocalDateTime.class)).thenReturn(other);

        SQLException thrown =
                Assertions.assertThrows(
                        SQLException.class, () -> converter.readTimestamp(resultSet, 1));
        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals("boom", thrown.getMessage());
        Assertions.assertEquals("HY000", thrown.getSQLState());
        Assertions.assertEquals(1234, thrown.getErrorCode());
        // A failed typed read must not silently degrade into the lossy Timestamp fallback.
        Mockito.verify(resultSet, Mockito.never()).getTimestamp(1);

        // The failure must not be remembered: column 1 is still read through the typed getter.
        Assertions.assertThrows(SQLException.class, () -> converter.readTimestamp(resultSet, 1));
        Mockito.verify(resultSet, Mockito.times(2)).getObject(1, LocalDateTime.class);
        Mockito.verify(resultSet, Mockito.never()).getTimestamp(1);

        // Other columns are unaffected by the failure.
        Assertions.assertEquals(other, converter.readTimestamp(resultSet, 2));
        Mockito.verify(resultSet).getObject(2, LocalDateTime.class);
        Mockito.verify(resultSet, Mockito.never()).getTimestamp(2);
    }

    @Test
    public void testTypedTimestampReadPreservesNulls() throws Exception {
        DuckDBJdbcRowConverter converter = new DuckDBJdbcRowConverter();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1, LocalDateTime.class)).thenReturn(null);

        Assertions.assertNull(converter.readTimestamp(resultSet, 1));
        Assertions.assertNull(converter.readTimestamp(resultSet, 1));

        // A SQL NULL is a value, not a capability failure: no fallback and no per-column state.
        Mockito.verify(resultSet, Mockito.times(2)).getObject(1, LocalDateTime.class);
        Mockito.verify(resultSet, Mockito.never()).getTimestamp(1);
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

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testTimestampAliasWallClockAcrossTimeZones() throws Exception {
        String table = "ts_alias_wall_clock";
        String tablePath = SCHEMA_NAME + "." + table;
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, c_ts TIMESTAMP, "
                                    + "c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, "
                                    + "c_ts_ns TIMESTAMP_NS)",
                            SCHEMA_NAME, table));
            statement.execute(
                    String.format(
                            "INSERT INTO \"%s\".\"%s\" VALUES "
                                    + "(1, TIMESTAMP '2024-03-10 02:30:00.123456', "
                                    + "TIMESTAMP_S '2024-03-10 02:30:00', "
                                    + "TIMESTAMP_MS '2024-03-10 02:30:00.123', "
                                    + "TIMESTAMP_NS '2024-03-10 02:30:00.123456789'), "
                                    + "(2, TIMESTAMP '2024-11-03 01:30:00.123456', "
                                    + "TIMESTAMP_S '2024-11-03 01:30:00', "
                                    + "TIMESTAMP_MS '2024-11-03 01:30:00.123', "
                                    + "TIMESTAMP_NS '2024-11-03 01:30:00.123456789'), "
                                    + "(3, TIMESTAMP '2024-06-15 12:34:56.123456', "
                                    + "TIMESTAMP_S '2024-06-15 12:34:56', "
                                    + "TIMESTAMP_MS '2024-06-15 12:34:56.123', "
                                    + "TIMESTAMP_NS '2024-06-15 12:34:56.123456789'), "
                                    + "(4, NULL, NULL, NULL, NULL), "
                                    + "(5, TIMESTAMP '1600-06-15 02:30:00', "
                                    + "TIMESTAMP_S '1600-06-15 02:30:00', "
                                    + "TIMESTAMP_MS '1600-06-15 02:30:00.123', NULL), "
                                    + "(6, TIMESTAMP '1800-06-15 02:30:00', "
                                    + "TIMESTAMP_S '1800-06-15 02:30:00', "
                                    + "TIMESTAMP_MS '1800-06-15 02:30:00.123', NULL), "
                                    + "(7, TIMESTAMP '1970-01-01 00:00:00', "
                                    + "TIMESTAMP_S '1970-01-01 00:00:00', "
                                    + "TIMESTAMP_MS '1970-01-01 00:00:00.000', "
                                    + "TIMESTAMP_NS '1970-01-01 00:00:00.000000000'), "
                                    + "(8, TIMESTAMP '1970-01-01 00:00:00.000001', "
                                    + "TIMESTAMP_S '1970-01-01 00:00:00', "
                                    + "TIMESTAMP_MS '1970-01-01 00:00:00.001', "
                                    + "TIMESTAMP_NS '1970-01-01 00:00:00.000000001'), "
                                    + "(9, TIMESTAMP '1969-12-31 23:59:59', "
                                    + "TIMESTAMP_S '1969-12-31 23:59:59', "
                                    + "TIMESTAMP_MS '1969-12-31 23:59:59.123', NULL)",
                            SCHEMA_NAME, table));
        }
        // Literal wall clocks stored in DuckDB: a US DST gap (row 1), a US DST overlap (row 2),
        // a normal value (row 3), all-NULL (row 4), two pre-1970 values (rows 5-6) and the epoch
        // boundary (rows 7-9). Field 1 is the plain TIMESTAMP control, so the alias columns cannot
        // be made to pass by regressing the typed read path. Rows 5-6 must keep the previous
        // plain-getter wall clock outside UTC: the UTC-instant route adds a local-mean-time offset
        // for pre-epoch values.
        // Rows 5-6 leave the `TIMESTAMP_NS` column NULL: 1600 is outside the range of that type,
        // and DuckDB JDBC 1.3.1.0 reads pre-epoch fractional `TIMESTAMP` and `TIMESTAMP_NS` values
        // one second late. Their whole-second `TIMESTAMP` controls plus the NULL `TIMESTAMP_NS`
        // column isolate the `TIMESTAMP_S`/`TIMESTAMP_MS` compatibility asserted here. Row 9 (one
        // second before the epoch) keeps the same NULL `TIMESTAMP_NS`, because its negative
        // fractional nanoseconds hit that same read defect; rows 7-8 cover the exact epoch and a
        // positive fractional nanosecond value.
        LocalDateTime[][] expected = {
            {
                LocalDateTime.of(2024, 3, 10, 2, 30, 0, 123_456_000),
                LocalDateTime.of(2024, 3, 10, 2, 30, 0),
                LocalDateTime.of(2024, 3, 10, 2, 30, 0, 123_000_000),
                LocalDateTime.of(2024, 3, 10, 2, 30, 0, 123_456_789)
            },
            {
                LocalDateTime.of(2024, 11, 3, 1, 30, 0, 123_456_000),
                LocalDateTime.of(2024, 11, 3, 1, 30, 0),
                LocalDateTime.of(2024, 11, 3, 1, 30, 0, 123_000_000),
                LocalDateTime.of(2024, 11, 3, 1, 30, 0, 123_456_789)
            },
            {
                LocalDateTime.of(2024, 6, 15, 12, 34, 56, 123_456_000),
                LocalDateTime.of(2024, 6, 15, 12, 34, 56),
                LocalDateTime.of(2024, 6, 15, 12, 34, 56, 123_000_000),
                LocalDateTime.of(2024, 6, 15, 12, 34, 56, 123_456_789)
            },
            {null, null, null, null},
            {
                LocalDateTime.of(1600, 6, 15, 2, 30, 0),
                LocalDateTime.of(1600, 6, 15, 2, 30, 0),
                LocalDateTime.of(1600, 6, 15, 2, 30, 0, 123_000_000),
                null
            },
            {
                LocalDateTime.of(1800, 6, 15, 2, 30, 0),
                LocalDateTime.of(1800, 6, 15, 2, 30, 0),
                LocalDateTime.of(1800, 6, 15, 2, 30, 0, 123_000_000),
                null
            },
            {
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0)
            },
            {
                LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1_000),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1_000_000),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1)
            },
            {
                LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_000_000),
                null
            }
        };
        String query =
                "SELECT id, c_ts, c_ts_s, c_ts_ms, c_ts_ns FROM main." + table + " ORDER BY id";
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (boolean useQuery : new boolean[] {false, true}) {
                    String mode = useQuery ? "query" : "table_path";
                    Map<String, Object> sourceOptions = new HashMap<>();
                    sourceOptions.put("url", jdbcUrl);
                    sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
                    sourceOptions.put(
                            useQuery ? "query" : "table_path", useQuery ? query : tablePath);
                    List<SeaTunnelRow> rows =
                            SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                    ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
                    Assertions.assertEquals(expected.length, rows.size(), zone + ", " + mode);
                    // `table_path` reads do not guarantee row order, so index the rows by their
                    // `id` column and assert the count and uniqueness of the ids.
                    Map<Integer, SeaTunnelRow> rowsById = new HashMap<>();
                    for (SeaTunnelRow row : rows) {
                        Integer id = (Integer) row.getField(0);
                        Assertions.assertNotNull(id, zone + ", " + mode);
                        Assertions.assertNull(
                                rowsById.put(id, row), zone + ", " + mode + ", duplicate id " + id);
                    }
                    Assertions.assertEquals(expected.length, rowsById.size(), zone + ", " + mode);
                    for (int id = 1; id <= expected.length; id++) {
                        SeaTunnelRow row = rowsById.get(id);
                        String message = zone + ", " + mode + ", id " + id;
                        Assertions.assertNotNull(row, message);
                        for (int column = 0; column < expected[id - 1].length; column++) {
                            Assertions.assertEquals(
                                    expected[id - 1][column],
                                    row.getField(column + 1),
                                    message + ", column " + column);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testTimestampAliasAroundFallbackAcrossTimeZones() throws Exception {
        // Regression: reading DuckDB TIMESTAMP columns must not reinterpret the stored wall clock.
        // Each zone stores a base wall clock plus that same value shifted by its DST offset, so a
        // read that adds or drops one shift is visible: one hour for America/Los_Angeles
        // (fallback 2024-11-03 02:00 -> 01:00) and Europe/Paris (2024-10-27 03:00 -> 02:00),
        // thirty minutes for Australia/Lord_Howe (2024-04-07 02:00 -> 01:30).
        String table = "ts_alias_around_fallback";
        String tablePath = SCHEMA_NAME + "." + table;
        // Fixed DuckDB literals, independent of the read path under test.
        String[] wallClocks = {
            "2024-11-03 08:30:00", // America/Los_Angeles: base
            "2024-11-03 07:30:00", // America/Los_Angeles: one hour before the base
            "2024-11-03 09:30:00", // America/Los_Angeles: one hour after the base
            "2024-10-27 00:30:00", // Europe/Paris: base
            "2024-10-26 23:30:00", // Europe/Paris: one hour before the base
            "2024-10-27 01:30:00", // Europe/Paris: one hour after the base
            "2024-04-06 14:30:00", // Australia/Lord_Howe: base
            "2024-04-06 14:00:00", // Australia/Lord_Howe: thirty minutes before the base
            "2024-04-06 15:00:00" // Australia/Lord_Howe: thirty minutes after the base
        };
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, c_ts TIMESTAMP, "
                                    + "c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, "
                                    + "c_ts_ns TIMESTAMP_NS)",
                            SCHEMA_NAME, table));
            StringBuilder insert =
                    new StringBuilder(
                            String.format("INSERT INTO \"%s\".\"%s\" VALUES ", SCHEMA_NAME, table));
            for (int i = 0; i < wallClocks.length; i++) {
                if (i > 0) {
                    insert.append(", ");
                }
                insert.append('(')
                        .append(i + 1)
                        .append(", TIMESTAMP '")
                        .append(wallClocks[i])
                        .append(".123456', TIMESTAMP_S '")
                        .append(wallClocks[i])
                        .append("', TIMESTAMP_MS '")
                        .append(wallClocks[i])
                        .append(".123', TIMESTAMP_NS '")
                        .append(wallClocks[i])
                        .append(".123456789')");
            }
            insert.append(", (").append(wallClocks.length + 1).append(", NULL, NULL, NULL, NULL)");
            statement.execute(insert.toString());
        }
        // Expected values are parsed back from the stored literals above, independently of the
        // read path under test: the plain TIMESTAMP control keeps its .123456 microseconds and the
        // typed columns keep their 0 / .123 / .123456789 fractions.
        LocalDateTime[][] expected = new LocalDateTime[wallClocks.length + 1][];
        for (int i = 0; i < wallClocks.length; i++) {
            LocalDateTime wallClock = LocalDateTime.parse(wallClocks[i].replace(' ', 'T'));
            expected[i] =
                    new LocalDateTime[] {
                        wallClock.withNano(123_456_000),
                        wallClock,
                        wallClock.withNano(123_000_000),
                        wallClock.withNano(123_456_789)
                    };
        }
        expected[wallClocks.length] = new LocalDateTime[] {null, null, null, null};
        String query =
                "SELECT id, c_ts, c_ts_s, c_ts_ms, c_ts_ns FROM main." + table + " ORDER BY id";
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone :
                    new String[] {
                        "UTC", "America/Los_Angeles", "Europe/Paris", "Australia/Lord_Howe"
                    }) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (boolean useQuery : new boolean[] {false, true}) {
                    String mode = useQuery ? "query" : "table_path";
                    Map<String, Object> sourceOptions = new HashMap<>();
                    sourceOptions.put("url", jdbcUrl);
                    sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
                    sourceOptions.put(
                            useQuery ? "query" : "table_path", useQuery ? query : tablePath);
                    List<SeaTunnelRow> rows =
                            SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                    ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
                    Assertions.assertEquals(expected.length, rows.size(), zone + ", " + mode);
                    // `table_path` reads do not guarantee row order, so index the rows by their
                    // `id` column and assert the count and uniqueness of the ids.
                    Map<Integer, SeaTunnelRow> rowsById = new HashMap<>();
                    for (SeaTunnelRow row : rows) {
                        Integer id = (Integer) row.getField(0);
                        Assertions.assertNotNull(id, zone + ", " + mode);
                        Assertions.assertNull(
                                rowsById.put(id, row), zone + ", " + mode + ", duplicate id " + id);
                    }
                    Assertions.assertEquals(expected.length, rowsById.size(), zone + ", " + mode);
                    for (int id = 1; id <= expected.length; id++) {
                        SeaTunnelRow row = rowsById.get(id);
                        String message = zone + ", " + mode + ", id " + id;
                        Assertions.assertNotNull(row, message);
                        for (int column = 0; column < expected[id - 1].length; column++) {
                            Assertions.assertEquals(
                                    expected[id - 1][column],
                                    row.getField(column + 1),
                                    message + ", column " + column);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @ResourceLock("java.util.TimeZone.default")
    @Test
    public void testPreEpochAliasReadsKeepTheLegacyGetterForDstGapAndCutover() throws Exception {
        // Regression pin for the read path that the sink-side wall-clock fix does not change: alias
        // values before the epoch still go through the legacy plain timestamp getter, whose
        // historical-offset and DST-gap normalization stays a known, deferred limitation.
        String table = "ts_alias_pre_epoch_legacy";
        String tablePath = SCHEMA_NAME + "." + table;
        // Stored SQL literals, independent of the read path under test. Row 1 falls inside the
        // 1918-03-31 02:00 -> 03:00 spring-forward gap of the US zones, row 2 is the same era
        // outside that gap, and row 3 falls inside the 1582 Gregorian cutover. The
        // `TIMESTAMP_NS` column is NULL for the cutover row because 1582 is outside the range of
        // that type.
        String[][] literals = {
            {
                "1918-03-31 02:30:00",
                "1918-03-31 02:30:00",
                "1918-03-31 02:30:00.123",
                "1918-03-31 02:30:00"
            },
            {
                "1918-06-15 12:00:00",
                "1918-06-15 12:00:00",
                "1918-06-15 12:00:00.123",
                "1918-06-15 12:00:00"
            },
            {"1582-10-10 12:34:56", "1582-10-10 12:34:56", "1582-10-10 12:34:56.123", null}
        };
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "CREATE TABLE \"%s\".\"%s\" (id INTEGER, c_ts TIMESTAMP, "
                                    + "c_ts_s TIMESTAMP_S, c_ts_ms TIMESTAMP_MS, "
                                    + "c_ts_ns TIMESTAMP_NS)",
                            SCHEMA_NAME, table));
            for (int i = 0; i < literals.length; i++) {
                statement.execute(
                        String.format(
                                "INSERT INTO \"%s\".\"%s\" VALUES (%d, TIMESTAMP '%s', "
                                        + "TIMESTAMP_S '%s', TIMESTAMP_MS '%s', %s)",
                                SCHEMA_NAME,
                                table,
                                i + 1,
                                literals[i][0],
                                literals[i][1],
                                literals[i][2],
                                literals[i][3] == null
                                        ? "NULL"
                                        : "TIMESTAMP_NS '" + literals[i][3] + "'"));
            }
            // Independent oracle: the wall clocks that actually reached the file, read back with
            // plain SQL instead of through the converter under test.
            try (ResultSet stored =
                    statement.executeQuery(
                            String.format(
                                    "SELECT id, CAST(c_ts AS VARCHAR), CAST(c_ts_s AS VARCHAR), "
                                            + "CAST(c_ts_ms AS VARCHAR), CAST(c_ts_ns AS VARCHAR) "
                                            + "FROM \"%s\".\"%s\" ORDER BY id",
                                    SCHEMA_NAME, table))) {
                int storedRows = 0;
                while (stored.next()) {
                    storedRows++;
                    Assertions.assertEquals(storedRows, stored.getInt(1));
                    for (int column = 0; column < 4; column++) {
                        Assertions.assertEquals(
                                literals[storedRows - 1][column],
                                stored.getString(column + 2),
                                "stored value, id " + storedRows + ", column " + column);
                    }
                }
                Assertions.assertEquals(literals.length, storedRows);
            }
        }
        // Expected read results, one entry per zone, pinned to what the tested driver
        // (duckdb_jdbc 1.3.1.0 on JDK 17) reports today. The plain `TIMESTAMP` column is the
        // control: its typed getter keeps every stored wall clock (02:30, 12:00, 1582-10-10). The
        // alias columns go through the legacy getter before the epoch: UTC and Asia/Shanghai have
        // no spring-forward gap in 1918, so they keep 02:30, while the US zones move it forward to
        // 03:30 by that gap; the pre-cutover date comes back ten days later in every zone, the
        // Julian reading of the 1582-10-10 literal. Both alias deviations are pinned here as
        // deferred; the sink-side wall-clock fix does not address them.
        LocalDateTime gapStoredValue = LocalDateTime.of(1918, 3, 31, 2, 30);
        LocalDateTime gapAfterSpringForward = LocalDateTime.of(1918, 3, 31, 3, 30);
        LocalDateTime cutoverStoredValue = LocalDateTime.of(1582, 10, 10, 12, 34, 56);
        LocalDateTime cutoverAliasValue = LocalDateTime.of(1582, 10, 20, 12, 34, 56);
        LocalDateTime cutoverMsAliasValue = LocalDateTime.of(1582, 10, 20, 12, 34, 56, 123_000_000);
        Map<String, LocalDateTime[][]> expectedByZone = new HashMap<>();
        for (String zone :
                new String[] {"UTC", "Asia/Shanghai", "America/New_York", "America/Los_Angeles"}) {
            // The JDK time-zone database decides whether the stored wall clock is inside that
            // zone's 1918 gap, independently of the read path under test.
            boolean inSpringForwardGap =
                    ZoneId.of(zone).getRules().getValidOffsets(gapStoredValue).isEmpty();
            LocalDateTime gapAliasValue =
                    inSpringForwardGap ? gapAfterSpringForward : gapStoredValue;
            expectedByZone.put(
                    zone,
                    new LocalDateTime[][] {
                        {
                            gapStoredValue,
                            gapAliasValue,
                            gapAliasValue.withNano(123_000_000),
                            gapAliasValue
                        },
                        {
                            LocalDateTime.of(1918, 6, 15, 12, 0),
                            LocalDateTime.of(1918, 6, 15, 12, 0),
                            LocalDateTime.of(1918, 6, 15, 12, 0, 0, 123_000_000),
                            LocalDateTime.of(1918, 6, 15, 12, 0)
                        },
                        {cutoverStoredValue, cutoverAliasValue, cutoverMsAliasValue, null}
                    });
        }
        String query =
                "SELECT id, c_ts, c_ts_s, c_ts_ms, c_ts_ns FROM main." + table + " ORDER BY id";
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone :
                    new String[] {
                        "UTC", "Asia/Shanghai", "America/New_York", "America/Los_Angeles"
                    }) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime[][] expected = expectedByZone.get(zone);
                for (boolean useQuery : new boolean[] {false, true}) {
                    String mode = useQuery ? "query" : "table_path";
                    Map<String, Object> sourceOptions = new HashMap<>();
                    sourceOptions.put("url", jdbcUrl);
                    sourceOptions.put("driver", "org.duckdb.DuckDBDriver");
                    sourceOptions.put(
                            useQuery ? "query" : "table_path", useQuery ? query : tablePath);
                    List<SeaTunnelRow> rows =
                            SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                                    ReadonlyConfig.fromMap(sourceOptions), new JdbcSourceFactory());
                    Assertions.assertEquals(expected.length, rows.size(), zone + ", " + mode);
                    // `table_path` reads do not guarantee row order, so index the rows by their
                    // `id` column and assert the count and uniqueness of the ids.
                    Map<Integer, SeaTunnelRow> rowsById = new HashMap<>();
                    for (SeaTunnelRow row : rows) {
                        Integer id = (Integer) row.getField(0);
                        Assertions.assertNotNull(id, zone + ", " + mode);
                        Assertions.assertNull(
                                rowsById.put(id, row), zone + ", " + mode + ", duplicate id " + id);
                    }
                    Assertions.assertEquals(expected.length, rowsById.size(), zone + ", " + mode);
                    for (int id = 1; id <= expected.length; id++) {
                        SeaTunnelRow row = rowsById.get(id);
                        String message = zone + ", " + mode + ", id " + id;
                        Assertions.assertNotNull(row, message);
                        for (int column = 0; column < expected[id - 1].length; column++) {
                            Assertions.assertEquals(
                                    expected[id - 1][column],
                                    row.getField(column + 1),
                                    message + ", column " + column);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
