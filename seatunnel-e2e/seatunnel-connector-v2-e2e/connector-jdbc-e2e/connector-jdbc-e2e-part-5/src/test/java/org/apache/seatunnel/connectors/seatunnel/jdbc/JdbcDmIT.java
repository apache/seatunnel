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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbJdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcDmIT extends AbstractJdbcIT {

    private static final String DM_IMAGE = "laglangyue/dmdb8";
    private static final String DM_CONTAINER_HOST = "e2e_dmdb";

    private static final String DM_DATABASE = "SYSDBA";
    private static final String DM_SOURCE = "e2e_table_source";
    private static final String DM_SINK = "e2e_table_sink";
    private static final String DM_USERNAME = "SYSDBA";
    private static final String DM_PASSWORD = "SYSDBA";
    private static final int DM_PORT = 5336;
    private static final int DM_CONTAINER_PORT = 5236;
    private static final String DM_URL = "jdbc:dm://" + HOST + ":%s";

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_dm_source_and_sink.conf");
    private static final String CREATE_SQL =
            "create table if not exists %s"
                    + "(\n"
                    + "    DM_BIT              BIT,\n"
                    + "    DM_INT              INT,\n"
                    + "    DM_INTEGER          INTEGER,\n"
                    + "    DM_PLS_INTEGER      PLS_INTEGER,\n"
                    + "    DM_TINYINT          TINYINT,\n"
                    + "\n"
                    + "    DM_BYTE             BYTE,\n"
                    + "    DM_SMALLINT         SMALLINT,\n"
                    + "    DM_BIGINT           BIGINT,\n"
                    + "\n"
                    + "    DM_NUMERIC          NUMERIC,\n"
                    + "    DM_NUMBER           NUMBER,\n"
                    + "    DM_DECIMAL          DECIMAL,\n"
                    + "    DM_DEC              DEC,\n"
                    + "\n"
                    + "    DM_REAL             REAL,\n"
                    + "    DM_FLOAT            FLOAT,\n"
                    + "    DM_DOUBLE_PRECISION DOUBLE PRECISION,\n"
                    + "    DM_DOUBLE           DOUBLE,\n"
                    + "\n"
                    + "    DM_CHAR             CHAR,\n"
                    + "    DM_CHARACTER        CHARACTER,\n"
                    + "    DM_NCHAR            NCHAR(50),\n"
                    + "    DM_VARCHAR          VARCHAR,\n"
                    + "    DM_VARCHAR2         VARCHAR2,\n"
                    + "    DM_NVARCHAR2        NVARCHAR2(50),\n"
                    + "    DM_TEXT             TEXT,\n"
                    + "    DM_LONG             LONG,\n"
                    + "    DM_LONGVARCHAR      LONGVARCHAR,\n"
                    + "    DM_CLOB             CLOB,\n"
                    + "\n"
                    + "    DM_TIMESTAMP        TIMESTAMP,\n"
                    + "    DM_DATETIME         DATETIME,\n"
                    + "    DM_DATE             DATE,\n"
                    + "\n"
                    + "    DM_BLOB             BLOB,\n"
                    + "    DM_BINARY           BINARY,\n"
                    + "    DM_VARBINARY        VARBINARY,\n"
                    + "    DM_LONGVARBINARY    LONGVARBINARY,\n"
                    + "    DM_IMAGE            IMAGE,\n"
                    + "    DM_BFILE            BFILE\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(DM_URL, DM_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DM_DATABASE, DM_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DM_IMAGE)
                .networkAliases(DM_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DM_CONTAINER_PORT)
                .localPort(DM_PORT)
                .jdbcTemplate(DM_URL)
                .jdbcUrl(jdbcUrl)
                .userName(DM_USERNAME)
                .password(DM_PASSWORD)
                .database(DM_DATABASE)
                .sourceTable(DM_SOURCE)
                .sinkTable(DM_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .tablePathFullName(String.format("%s.%s", DM_DATABASE, DM_SOURCE))
                .build();
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {
                    "DM_BIT",
                    "DM_INT",
                    "DM_INTEGER",
                    "DM_PLS_INTEGER",
                    "DM_TINYINT",
                    "DM_BYTE",
                    "DM_SMALLINT",
                    "DM_BIGINT",
                    "DM_NUMERIC",
                    "DM_NUMBER",
                    "DM_DECIMAL",
                    "DM_DEC",
                    "DM_REAL",
                    "DM_FLOAT",
                    "DM_DOUBLE_PRECISION",
                    "DM_DOUBLE",
                    "DM_CHAR",
                    "DM_CHARACTER",
                    "DM_NCHAR",
                    "DM_VARCHAR",
                    "DM_VARCHAR2",
                    "DM_NVARCHAR2",
                    "DM_TEXT",
                    "DM_LONG",
                    "DM_LONGVARCHAR",
                    "DM_CLOB",
                    "DM_TIMESTAMP",
                    "DM_DATETIME",
                    "DM_DATE",
                    "DM_BLOB",
                    "DM_BINARY",
                    "DM_VARBINARY",
                    "DM_LONGVARBINARY",
                    "DM_IMAGE",
                    "DM_BFILE"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? (byte) 1 : (byte) 0,
                                i,
                                i,
                                i,
                                Short.valueOf("1"),
                                Byte.valueOf("1"),
                                i,
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                'f',
                                'f',
                                // DM_NCHAR: multi-byte content exercises the national
                                // character path
                                String.format("达梦_%s", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                // DM_NVARCHAR2: multi-byte content exercises the national
                                // character path
                                String.format("达梦_%s", i),
                                String.format("f1_%s", i),
                                String.format("{\"aa\":\"bb_%s\"}", i),
                                String.format("f1_%s", i),
                                String.format("f1_%s", i),
                                Timestamp.valueOf(LocalDateTime.now()),
                                new Timestamp(System.currentTimeMillis()),
                                Date.valueOf(LocalDate.now()),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            });
            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Test
    public void testLocalTimeZoneAndIntervalTypeConversion() throws Exception {
        String tableName = "E2E_DM_LOCAL_TZ_INTERVAL";
        String tableInfo = DM_USERNAME + "." + tableName;
        // Build a local catalog scoped to this test so we don't inadvertently
        // enable the inherited testCreateIndex (which fires whenever catalog != null).
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        DamengCatalog localCatalog =
                new DamengCatalog(
                        "dameng",
                        jdbcCase.getUserName(),
                        jdbcCase.getPassword(),
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        DM_USERNAME,
                        DRIVER_CLASS);
        localCatalog.open();
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE "
                                + tableInfo
                                + " (LTZ_VALUE TIMESTAMP(6) WITH LOCAL TIME ZONE,"
                                + " IYM_VALUE INTERVAL YEAR TO MONTH,"
                                + " IDS_VALUE INTERVAL DAY TO SECOND)");
                statement.execute(
                        "INSERT INTO "
                                + tableInfo
                                + " VALUES (TIMESTAMP '2024-05-06 07:08:09.123456 +08:00',"
                                + " INTERVAL '2-3' YEAR TO MONTH,"
                                + " INTERVAL '4 05:06:07.123456' DAY TO SECOND)");
                connection.commit();

                CatalogTable catalogTable =
                        localCatalog.getTable(TablePath.of(DM_DATABASE, DM_USERNAME, tableName));
                Assertions.assertEquals(
                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                        catalogTable.getTableSchema().getColumn("LTZ_VALUE").getDataType());
                Assertions.assertEquals(
                        "TIMESTAMP WITH LOCAL TIME ZONE",
                        catalogTable.getTableSchema().getColumn("LTZ_VALUE").getSourceType());
                Assertions.assertEquals(
                        BasicType.STRING_TYPE,
                        catalogTable.getTableSchema().getColumn("IYM_VALUE").getDataType());
                Assertions.assertEquals(
                        BasicType.STRING_TYPE,
                        catalogTable.getTableSchema().getColumn("IDS_VALUE").getDataType());

                try (ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT LTZ_VALUE, IYM_VALUE, IDS_VALUE FROM " + tableInfo)) {
                    Assertions.assertTrue(resultSet.next());
                    String iymDriverString = resultSet.getString("IYM_VALUE");
                    String idsDriverString = resultSet.getString("IDS_VALUE");
                    SeaTunnelRow row =
                            new DmdbJdbcRowConverter()
                                    .toInternal(resultSet, catalogTable.getTableSchema());
                    OffsetDateTime convertedTimestamp = (OffsetDateTime) row.getField(0);
                    Assertions.assertEquals(
                            OffsetDateTime.parse("2024-05-06T07:08:09.123456+08:00").toInstant(),
                            convertedTimestamp.toInstant());
                    Assertions.assertEquals(iymDriverString, row.getField(1));
                    Assertions.assertEquals(idsDriverString, row.getField(2));
                    Assertions.assertFalse(resultSet.next());
                }
            } finally {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE " + tableInfo);
                    connection.commit();
                }
            }
        } finally {
            localCatalog.close();
        }
    }

    /**
     * Starts the image with a fresh database initialized with LENGTH_IN_CHAR=1 and CHARSET=1
     * (UTF-8), the opposite of the image's default database on both settings.
     */
    private static final String INIT_LENGTH_IN_CHAR_UTF8 =
            "cd /opt/dmdbms/bin"
                    + " && su dmdba -c \"./dminit PATH=/opt/dmdbms/st DB_NAME=ST INSTANCE_NAME=ST"
                    + " PORT_NUM=5236 LENGTH_IN_CHAR=1 CHARSET=1 PAGE_SIZE=8 CASE_SENSITIVE=0"
                    + " SYSDBA_PWD=SYSDBA001\""
                    + " && exec su dmdba -c \"./dmserver path=/opt/dmdbms/st/ST/dm.ini -noconsole\"";

    @Test
    public void testNationalCharLengthFromCatalog() throws Exception {
        // The shared container runs the image's default database: LENGTH_IN_CHAR=0, GB18030.
        String jdbcUrl = jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost());
        assertNationalCharLengths(jdbcUrl, DM_PASSWORD, 0, 0, true);

        try (GenericContainer<?> lengthInCharUtf8 =
                new GenericContainer<>(DM_IMAGE)
                        .withNetwork(NETWORK)
                        .withCommand("bash", "-c", INIT_LENGTH_IN_CHAR_UTF8)
                        .withExposedPorts(DM_CONTAINER_PORT)
                        .waitingFor(
                                Wait.forLogMessage(".*SYSTEM IS READY.*", 1)
                                        .withStartupTimeout(Duration.ofMinutes(5)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DM_IMAGE)))) {
            lengthInCharUtf8.start();
            String url =
                    String.format(
                            "jdbc:dm://%s:%d",
                            lengthInCharUtf8.getHost(),
                            lengthInCharUtf8.getMappedPort(DM_CONTAINER_PORT));
            assertNationalCharLengths(url, "SYSDBA001", 1, 1, false);
        }
    }

    /**
     * Checks that the catalog reports the declared length of NVARCHAR and NVARCHAR2, which is also
     * the number of characters they hold, and leaves CHAR, VARCHAR and NCHAR as before.
     */
    private void assertNationalCharLengths(
            String jdbcUrl,
            String password,
            int lengthInChar,
            int unicodeFlag,
            boolean checkReadOnlyUser)
            throws Exception {
        String table = "E2E_DM_NATIONAL_CHAR";
        String tableInfo = DM_USERNAME + "." + table;
        TablePath tablePath = TablePath.of(DM_DATABASE, DM_USERNAME, table);
        String reader = "E2E_DM_READER";
        try (Connection conn = connect(jdbcUrl, DM_USERNAME, password);
                Statement statement = conn.createStatement()) {
            try (ResultSet rs =
                    statement.executeQuery(
                            "SELECT SF_GET_LENGTH_IN_CHAR(), SF_GET_UNICODE_FLAG() FROM DUAL")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(lengthInChar, rs.getInt(1));
                Assertions.assertEquals(unicodeFlag, rs.getInt(2));
            }
            statement.execute(
                    "CREATE TABLE "
                            + tableInfo
                            + " (V_NVARCHAR NVARCHAR(10), V_NVARCHAR2 NVARCHAR2(10),"
                            + " V_NVARCHAR_100 NVARCHAR(100), V_NVARCHAR2_100 NVARCHAR2(100),"
                            + " V_NCHAR NCHAR(10), V_CHAR CHAR(10), V_VARCHAR VARCHAR(10),"
                            + " V_VARCHAR2 VARCHAR2(10))");
            try {
                // NVARCHAR(10) and NVARCHAR2(10) hold 10 characters, multi-byte or not.
                String tenChinese = repeat("\u8fbe", 10);
                statement.execute(
                        "INSERT INTO "
                                + tableInfo
                                + " (V_NVARCHAR, V_NVARCHAR2) VALUES ('"
                                + tenChinese
                                + "', '"
                                + tenChinese
                                + "')");
                Assertions.assertThrows(
                        SQLException.class,
                        () ->
                                statement.execute(
                                        "INSERT INTO "
                                                + tableInfo
                                                + " (V_NVARCHAR2) VALUES ('"
                                                + repeat("a", 11)
                                                + "')"));

                assertCatalogLengths(jdbcUrl, DM_USERNAME, password, tablePath);
                if (checkReadOnlyUser) {
                    // A user that can only read the table must get the same lengths.
                    statement.execute("CREATE USER " + reader + " IDENTIFIED BY \"Reader12345\"");
                    statement.execute("GRANT SELECT ON " + tableInfo + " TO " + reader);
                    assertCatalogLengths(jdbcUrl, reader, "Reader12345", tablePath);
                }
            } finally {
                statement.execute("DROP TABLE " + tableInfo);
                if (checkReadOnlyUser) {
                    statement.execute("DROP USER IF EXISTS " + reader + " CASCADE");
                }
            }
        }
    }

    private void assertCatalogLengths(
            String jdbcUrl, String user, String password, TablePath tablePath) {
        DamengCatalog catalog =
                new DamengCatalog(
                        "dameng",
                        user,
                        password,
                        JdbcUrlUtil.getUrlInfo(jdbcUrl),
                        DM_USERNAME,
                        DRIVER_CLASS);
        catalog.open();
        try {
            CatalogTable catalogTable = catalog.getTable(tablePath);
            Map<String, Pair<String, Long>> expected = new HashMap<>();
            expected.put("V_NVARCHAR", Pair.of("NVARCHAR(10)", 40L));
            expected.put("V_NVARCHAR2", Pair.of("NVARCHAR2(10)", 40L));
            expected.put("V_NVARCHAR_100", Pair.of("NVARCHAR(100)", 400L));
            expected.put("V_NVARCHAR2_100", Pair.of("NVARCHAR2(100)", 400L));
            expected.put("V_NCHAR", Pair.of("NCHAR(10)", 40L));
            expected.put("V_CHAR", Pair.of("CHAR(10)", 40L));
            expected.put("V_VARCHAR", Pair.of("VARCHAR2(10)", 40L));
            expected.put("V_VARCHAR2", Pair.of("VARCHAR2(10)", 40L));
            expected.forEach(
                    (name, type) -> {
                        Column column = catalogTable.getTableSchema().getColumn(name);
                        Assertions.assertEquals(type.getLeft(), column.getSourceType(), name);
                        Assertions.assertEquals(type.getRight(), column.getColumnLength(), name);
                    });
        } finally {
            catalog.close();
        }
    }

    private Connection connect(String jdbcUrl, String user, String password) throws Exception {
        Driver driver = (Driver) loadDriverClass().newInstance();
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        Connection conn = driver.connect(jdbcUrl, props);
        conn.setAutoCommit(true);
        return conn;
    }

    private static String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(schema, table);
    }

    protected void clearTable(String database, String schema, String table) {
        clearTable(schema, table);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DM_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DM_CONTAINER_HOST)
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DM_IMAGE)));
        container.addExposedPort(DM_CONTAINER_PORT);

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }
}
