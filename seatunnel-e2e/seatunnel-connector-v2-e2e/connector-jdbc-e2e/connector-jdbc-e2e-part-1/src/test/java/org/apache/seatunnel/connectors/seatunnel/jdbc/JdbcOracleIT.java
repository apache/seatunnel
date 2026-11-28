/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcOracleIT extends AbstractJdbcIT {

    private static final String ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int ORACLE_PORT = 1521;
    private static final String ORACLE_URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "TESTUSER";
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "XE";
    private static final String SCHEMA = USERNAME;
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE";
    private static final String SINK_TABLE = "E2E_TABLE_SINK";
    private static final String CATALOG_TABLE = "E2E_TABLE_CATALOG";
    private static final String VIEW_SOURCE = "E2E_VIEW_SOURCE";
    private static final String VIEW_SINK = "E2E_VIEW_SINK";
    private static final List<String> VIEW_COLUMNS =
            Arrays.asList("VARCHAR_10_COL", "INTEGER_COL", "DATE_COL");
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList(
                    "/jdbc_oracle_source_to_sink.conf",
                    "/jdbc_oracle_source_to_sink_use_select1.conf",
                    "/jdbc_oracle_source_to_sink_use_select2.conf",
                    "/jdbc_oracle_source_to_sink_use_select3.conf");

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    BLOB_COL                      blob,\n"
                    + "    NUMBER_1             number(1),\n"
                    + "    NUMBER_6             number(6),\n"
                    + "    NUMBER_10             number(10),\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    NUMBER_7_SF_N2_DP             number(7, -2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    XML_TYPE_COL                  \"SYS\".\"XMLTYPE\",\n"
                    + "    INTERVAL_COL           interval day(2) to second(6),\n"
                    + "    constraint PK_T_COL primary key (INTEGER_COL)"
                    + ")";

    private static final String SINK_CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    VARCHAR_10_COL                varchar2(10),\n"
                    + "    CHAR_10_COL                   char(10),\n"
                    + "    CLOB_COL                      clob,\n"
                    + "    BLOB_COL                      blob,\n"
                    + "    NUMBER_1             number(1),\n"
                    + "    NUMBER_6             number(6),\n"
                    + "    NUMBER_10             number(10),\n"
                    + "    NUMBER_3_SF_2_DP              number(3, 2),\n"
                    + "    NUMBER_7_SF_N2_DP             number(7, -2),\n"
                    + "    INTEGER_COL                   integer,\n"
                    + "    FLOAT_COL                     float(10),\n"
                    + "    REAL_COL                      real,\n"
                    + "    BINARY_FLOAT_COL              binary_float,\n"
                    + "    BINARY_DOUBLE_COL             binary_double,\n"
                    + "    DATE_COL                      date,\n"
                    + "    TIMESTAMP_WITH_3_FRAC_SEC_COL timestamp(3),\n"
                    + "    TIMESTAMP_WITH_LOCAL_TZ       timestamp with local time zone,\n"
                    + "    XML_TYPE_COL                  \"SYS\".\"XMLTYPE\",\n"
                    + "    INTERVAL_COL           interval day(2) to second(6)\n"
                    + ")";

    private static final String[] fieldNames =
            new String[] {
                "VARCHAR_10_COL",
                "CHAR_10_COL",
                "CLOB_COL",
                "BLOB_COL",
                "NUMBER_1",
                "NUMBER_6",
                "NUMBER_10",
                "NUMBER_3_SF_2_DP",
                "NUMBER_7_SF_N2_DP",
                "INTEGER_COL",
                "FLOAT_COL",
                "REAL_COL",
                "BINARY_FLOAT_COL",
                "BINARY_DOUBLE_COL",
                "DATE_COL",
                "TIMESTAMP_WITH_3_FRAC_SEC_COL",
                "TIMESTAMP_WITH_LOCAL_TZ",
                "XML_TYPE_COL",
                "INTERVAL_COL"
            };

    @Test
    public void testSampleDataFromColumnSuccess() throws Exception {
        JdbcDialect dialect = new OracleDialect();
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(null, SCHEMA, SOURCE_TABLE))
                        .build();
        dialect.sampleDataFromColumn(connection, table, "INTEGER_COL", 1, 1024);

        table =
                JdbcSourceTable.builder()
                        .tablePath(TablePath.of(null, SCHEMA, SOURCE_TABLE))
                        .query(
                                "select * from "
                                        + quoteIdentifier(SOURCE_TABLE)
                                        + " where INTEGER_COL = 1")
                        .build();
        dialect.sampleDataFromColumn(connection, table, "INTEGER_COL", 1, 1024);
    }

    /**
     * Disabled on Spark: TIMESTAMP WITH LOCAL TIME ZONE is now mapped to TIMESTAMP_TZ
     * (OffsetDateTime). Spark encodes TIMESTAMP_TZ as DecimalType(18, 5) internally, causing
     * byte-level mismatch on Oracle round-trip. See JdbcMysqlTimestampIT for the same limitation.
     */
    @Override
    protected boolean isDisabledOnContainer(TestContainer container) {
        return container.identifier().getEngineType() == EngineType.SPARK;
    }

    @TestTemplate
    public void testOracleWithoutDecimalTypeNarrowing(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob(
                        "/jdbc_oracle_source_to_sink_without_decimal_type_narrowing.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testOracleWithBlobAsString(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_oracle_source_to_sink_with_blob_as_string.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testOracleLobWithFakeSource(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_oracle_fake_source_to_sink_with_lob.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    /** Real-driver coverage for generated statements across short/long LOB and null transitions. */
    @Test
    public void testGeneratedDmlWithLobNullTransitions() throws Exception {
        String table = "E2E_LOB_NULL_TRANSITIONS";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE " + table + " (ID NUMBER PRIMARY KEY, C CLOB, N NCLOB, B BLOB)");
        }
        try {
            for (String mode : Arrays.asList("insert", "merge", "fallback")) {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("TRUNCATE TABLE " + table);
                }
                writeAndCheckLobNullRows(table, mode, 0);
                if (!"insert".equals(mode)) {
                    writeAndCheckLobNullRows(table, mode, 1);
                }
            }
        } finally {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE " + table);
            }
        }
    }

    /** Detects driver batch fragmentation without depending on wall-clock timing. */
    @Test
    public void testSparseLobNullsKeepRoundTripsBatchScaled() throws Exception {
        String table = "E2E_LOB_BATCH_ROUNDTRIPS";
        int rows = 1000;
        int batchSize = 100;
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE " + table + " (ID NUMBER PRIMARY KEY, C CLOB, N NCLOB, B BLOB)");
        }
        try {
            JdbcConnectionConfig connectionConfig = lobNullConnectionConfig(batchSize);
            JdbcSinkConfig config =
                    JdbcSinkConfig.builder()
                            .jdbcConnectionConfig(connectionConfig)
                            .table(table)
                            .build();
            SimpleJdbcConnectionProvider provider =
                    new SimpleJdbcConnectionProvider(connectionConfig);
            TableSchema schema = lobNullSchema();
            JdbcOutputFormat format =
                    new JdbcOutputFormatBuilder(
                                    new OracleDialect(), provider, config, schema, schema)
                            .commitOnFlush(true)
                            .build();
            format.open();
            try {
                long before = oracleRoundTrips(provider.getConnection());
                for (int id = 1; id <= rows; id++) {
                    boolean isNull = id % 17 == 0;
                    format.writeRecord(
                            new SeaTunnelRow(
                                    new Object[] {
                                        (long) id,
                                        isNull ? null : "row-\u4e2d\uD83D\uDE00",
                                        isNull ? null : "row-\u4e2d\uD83D\uDE00",
                                        isNull ? null : new byte[] {1, 2, (byte) id}
                                    }));
                }
                format.flush();
                long roundTrips = oracleRoundTrips(provider.getConnection()) - before;
                int batches = (rows + batchSize - 1) / batchSize;
                // Allow execution, commit and diagnostic overhead, but not per-null batch splits.
                Assertions.assertTrue(
                        roundTrips <= 4L * batches + 20,
                        "LOB NULLs fragmented driver batches: "
                                + roundTrips
                                + " round trips for "
                                + batches
                                + " batches");
            } finally {
                format.close();
            }
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals(rows, result.getLong(1));
            }
        } finally {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE " + table);
            }
        }
    }

    private static long oracleRoundTrips(Connection writerConnection) throws Exception {
        // The existing Oracle IT initialization grants DBA to its isolated TESTUSER.
        try (Statement statement = writerConnection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT m.value FROM v$mystat m JOIN v$statname n ON n.statistic#=m.statistic# "
                                        + "WHERE n.name='SQL*Net roundtrips to/from client'")) {
            Assertions.assertTrue(result.next());
            return result.getLong(1);
        }
    }

    /** A typed CLOB null must still select the CLOB overload in unrestricted custom SQL. */
    @Test
    public void testCustomSqlPreservesLobNullOverload() throws Exception {
        String table = "E2E_LOB_CUSTOM_NULL";
        String dispatch = "E2E_LOB_NULL_DISPATCH";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE " + table + " (ID NUMBER PRIMARY KEY, C CLOB, N NCLOB, B BLOB)");
            statement.executeUpdate(
                    "CREATE OR REPLACE PACKAGE "
                            + dispatch
                            + " AS FUNCTION PICK(P VARCHAR2) RETURN VARCHAR2; FUNCTION PICK(P CLOB) RETURN VARCHAR2; END;");
            statement.executeUpdate(
                    "CREATE OR REPLACE PACKAGE BODY "
                            + dispatch
                            + " AS FUNCTION PICK(P VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN 'VARCHAR'; END;"
                            + " FUNCTION PICK(P CLOB) RETURN VARCHAR2 IS BEGIN RETURN 'CLOB'; END; END;");
        }
        try {
            JdbcConnectionConfig connectionConfig = lobNullConnectionConfig(17);
            JdbcSinkConfig config =
                    JdbcSinkConfig.builder()
                            .jdbcConnectionConfig(connectionConfig)
                            .table(table)
                            .simpleSql(
                                    "INSERT INTO "
                                            + table
                                            + " (ID,C,N,B) VALUES (:ID,"
                                            + dispatch
                                            + ".PICK(:C),:N,:B)")
                            .build();
            TableSchema schema = lobNullSchema();
            JdbcOutputFormat format =
                    new JdbcOutputFormatBuilder(
                                    new OracleDialect(),
                                    new SimpleJdbcConnectionProvider(connectionConfig),
                                    config,
                                    schema,
                                    schema)
                            .commitOnFlush(true)
                            .build();
            format.open();
            try {
                format.writeRecord(new SeaTunnelRow(new Object[] {1L, null, null, null}));
                format.flush();
            } finally {
                format.close();
            }
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT C,N,B FROM " + table)) {
                Assertions.assertTrue(result.next());
                Assertions.assertEquals("CLOB", result.getString(1));
                Assertions.assertNull(result.getString(2));
                Assertions.assertNull(result.getBytes(3));
                Assertions.assertFalse(result.next());
            }
        } finally {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE " + table);
                statement.executeUpdate("DROP PACKAGE " + dispatch);
            }
        }
    }

    private void writeAndCheckLobNullRows(String table, String mode, int phase) throws Exception {
        TableSchema schema = lobNullSchema();
        JdbcConnectionConfig connectionConfig = lobNullConnectionConfig(17);
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .table(table)
                        .primaryKeys("insert".equals(mode) ? null : Collections.singletonList("ID"))
                        .enableUpsert("merge".equals(mode))
                        .build();
        JdbcOutputFormat format =
                new JdbcOutputFormatBuilder(
                                new OracleDialect(),
                                new SimpleJdbcConnectionProvider(connectionConfig),
                                sinkConfig,
                                schema,
                                schema)
                        .commitOnFlush(true)
                        .build();
        format.open();
        try {
            for (int id = 1; id <= 144; id++) {
                SeaTunnelRow row =
                        new SeaTunnelRow(lobNullValues(id, phase, !"merge".equals(mode)));
                if (phase == 1) {
                    row.setRowKind(RowKind.UPDATE_AFTER);
                }
                format.writeRecord(row);
            }
            format.flush();
        } finally {
            format.close();
        }
        try (Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery("SELECT ID,C,N,B FROM " + table + " ORDER BY ID")) {
            for (int id = 1; id <= 144; id++) {
                Object[] expected = lobNullValues(id, phase, !"merge".equals(mode));
                Assertions.assertTrue(result.next(), "Missing LOB row " + id);
                Assertions.assertEquals(id, result.getLong(1));
                String text = (String) expected[1];
                // Oracle maps zero-length character/binary stream binds to SQL NULL.
                String expectedText = text != null && text.isEmpty() ? null : text;
                byte[] binary = (byte[]) expected[3];
                byte[] expectedBinary = binary != null && binary.length == 0 ? null : binary;
                Assertions.assertEquals(expectedText, result.getString(2), "CLOB row " + id);
                Assertions.assertEquals(expectedText, result.getString(3), "NCLOB row " + id);
                Assertions.assertArrayEquals(expectedBinary, result.getBytes(4), "BLOB row " + id);
            }
            Assertions.assertFalse(result.next(), "Unexpected extra LOB row");
        }
    }

    private static TableSchema lobNullSchema() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.builder()
                                .name("ID")
                                .dataType(BasicType.LONG_TYPE)
                                .nullable(false)
                                .sourceType("NUMBER")
                                .build())
                .column(
                        PhysicalColumn.builder()
                                .name("C")
                                .dataType(BasicType.STRING_TYPE)
                                .sourceType("CLOB")
                                .build())
                .column(
                        PhysicalColumn.builder()
                                .name("N")
                                .dataType(BasicType.STRING_TYPE)
                                .sourceType("NCLOB")
                                .build())
                .column(
                        PhysicalColumn.builder()
                                .name("B")
                                .dataType(PrimitiveByteArrayType.INSTANCE)
                                .sourceType("BLOB")
                                .build())
                .build();
    }

    private JdbcConnectionConfig lobNullConnectionConfig(int batchSize) {
        return JdbcConnectionConfig.builder()
                .url(jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost()))
                .driverName(DRIVER_CLASS)
                .username(jdbcCase.getUserName())
                .password(jdbcCase.getPassword())
                .autoCommit(false)
                .batchSize(batchSize)
                .batchIntervalMs(0)
                .maxRetries(0)
                .build();
    }

    private static Object[] lobNullValues(int id, int phase, boolean longValues) {
        // Older Oracle/driver versions cannot MERGE long LOB binds through SELECT FROM DUAL.
        // Exercise long values with INSERT and UPDATE, and the supported short MERGE path.
        int[] lengths =
                longValues
                        ? new int[] {
                            -1, 0, 1, 1333, 1334, 2000, 2001, 4000, 4001, 32767, 32768, 65536
                        }
                        : new int[] {-1, 0, 1, 128};
        int length = lengths[(id + phase) % lengths.length];
        // Start with a complete null-only batch, including NCLOB's national-character form.
        if (id <= 17 || length < 0) {
            return new Object[] {(long) id, null, null, null};
        }
        String text = ("\u4e2d\uD83D\uDE00\u00e9".repeat((length + 3) / 4)).substring(0, length);
        if (!text.isEmpty() && Character.isHighSurrogate(text.charAt(text.length() - 1))) {
            text = text.substring(0, text.length() - 1) + "x";
        }
        byte[] binary = new byte[length];
        for (int i = 0; i < length; i++) {
            binary[i] = (byte) (i + id + phase);
        }
        return new Object[] {(long) id, text, text, binary};
    }

    @TestTemplate
    public void testOracleAppendValuesInsertMode(TestContainer container) throws Exception {
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_oracle_source_to_sink_append_values.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "SELECT COUNT(*) FROM "
                                            + buildTableInfoWithSchema(SCHEMA, SINK_TABLE))) {
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(100, resultSet.getInt(1));
            }
        } finally {
            clearTable(jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSinkTable());
        }
    }

    /** A source {@code table_path} may name a view, not only a table. */
    @TestTemplate
    public void testViewAsSourceTablePath(TestContainer container) throws Exception {
        assertViewReadThroughTablePath(
                container,
                "/jdbc_oracle_view_source_to_sink.conf",
                createViewSql(),
                buildTableInfoWithSchema(SCHEMA, VIEW_SOURCE),
                buildTableInfoWithSchema(SCHEMA, VIEW_SINK));
    }

    @Test
    public void testCatalogResolvesView() throws Exception {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        assertCatalogResolvesView(
                new OracleCatalog(
                        "oracle",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA,
                        null),
                createViewSql(),
                buildTableInfoWithSchema(SCHEMA, VIEW_SOURCE),
                TablePath.of(DATABASE, SCHEMA, VIEW_SOURCE),
                VIEW_COLUMNS);
    }

    private String createViewSql() {
        return "CREATE OR REPLACE VIEW "
                + buildTableInfoWithSchema(SCHEMA, VIEW_SOURCE)
                + " AS SELECT "
                + String.join(", ", VIEW_COLUMNS)
                + " FROM "
                + buildTableInfoWithSchema(SCHEMA, SOURCE_TABLE);
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("ORACLE_PASSWORD", PASSWORD);
        containerEnv.put("APP_USER", USERNAME);
        containerEnv.put("APP_USER_PASSWORD", PASSWORD);
        String jdbcUrl = String.format(ORACLE_URL, ORACLE_PORT, SCHEMA);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(SCHEMA, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(ORACLE_IMAGE)
                .networkAliases(ORACLE_NETWORK_ALIASES)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(ORACLE_PORT)
                .localPort(ORACLE_PORT)
                .jdbcTemplate(ORACLE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(DATABASE)
                .schema(SCHEMA)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .catalogDatabase(DATABASE)
                .catalogSchema(SCHEMA)
                .catalogTable(CATALOG_TABLE)
                .createSql(CREATE_SQL)
                .sinkCreateSql(SINK_CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                // oracle jdbc not support getTables/getCatalog/getSchema , is empty
                .tablePathFullName(TablePath.DEFAULT.getFullName())
                .build();
    }

    @Override
    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, fieldNames, "INTEGER_COL");
    }

    @Override
    protected List<String> driverDependencyClassNames() {
        return Arrays.asList(
                "oracle.jdbc.OracleDriver", "oracle.xdb.XMLType", "oracle.xml.parser.v2.XMLParser");
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                String.format("f%s", i),
                                String.format("f%s", i),
                                String.format("f%s", i),
                                // set value bytes more than 4000bytes
                                IntStream.range(0, 4000)
                                        .mapToObj(d -> d + "")
                                        .collect(Collectors.joining(","))
                                        .getBytes(StandardCharsets.UTF_8),
                                1,
                                i * 10,
                                i * 1000,
                                BigDecimal.valueOf(1.1),
                                BigDecimal.valueOf(2400),
                                i,
                                Float.parseFloat("2.2"),
                                Float.parseFloat("2.2"),
                                Float.parseFloat("22.2"),
                                Double.parseDouble("2.2"),
                                Date.valueOf(LocalDate.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                Timestamp.valueOf(LocalDateTime.now()),
                                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\"><name>SeaTunnel : E2E : Connector V2 : Oracle XMLType</name></project>",
                                Duration.ofHours(25)
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        // gvenzl/oracle-free is a multi-arch (amd64/arm64) substitute for gvenzl/oracle-xe
        DockerImageName imageName =
                DockerImageName.parse(ORACLE_IMAGE).asCompatibleSubstituteFor("gvenzl/oracle-xe");

        GenericContainer<?> container =
                new OracleContainer(imageName)
                        .withDatabaseName(SCHEMA)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("sql/oracle_init.sql"),
                                "/container-entrypoint-startdb.d/init.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    @Override
    protected void initCatalog() {
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        catalog =
                new OracleCatalog(
                        "oracle",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        OracleURLParser.parse(jdbcUrl),
                        SCHEMA,
                        null);
        catalog.open();
    }

    @BeforeAll
    @Override
    public void startUp() {
        super.startUp();
        // analyzeTable before execute job
        String analyzeTable =
                String.format(
                        "analyze table "
                                + quoteIdentifier(SOURCE_TABLE)
                                + " compute statistics for table");
        log.info("analyze table {}", analyzeTable);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(analyzeTable);
        } catch (Exception e) {
            log.error("Error when analyze table", e);
        }
    }
}
