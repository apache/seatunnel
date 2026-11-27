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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase.KingbaseCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingbaseDialect;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end regression for the explicit {@code compatible_mode = "mysql"} path of the Kingbase
 * connector: type mapping, row conversion, generated upsert SQL, {@code field_ide} handling and the
 * MySQL catalog selected for table discovery.
 *
 * <p>This test needs a KingbaseES server that was initialised in MySQL compatibility mode and a KM
 * license, so it is opt-in, like {@link JdbcKingbaseIT}. To run it:
 *
 * <ol>
 *   <li>Obtain a trial KM license from <a href="https://www.kingbase.com.cn/sqwjxz/index.htm">the
 *       KingbaseES download page</a>.
 *   <li>Provide an image (or replace {@link #KINGBASE_IMAGE}) whose cluster is initialised in MySQL
 *       mode, and point {@code -Dkingbase.mysql.image=<image>} at it. The default image used by
 *       {@link JdbcKingbaseIT} runs in the native mode, where this test is expected to fail.
 *   <li>Point {@code -Dkingbase.km.license=<host path>} at the downloaded license, then remove the
 *       {@link Disabled} annotation.
 * </ol>
 *
 * <p>What it exercises against the real server: the source job reads rows through the MySQL type
 * mapper and row converter; the sink job generates its insert/upsert SQL from {@code
 * generate_sink_sql} with the same explicit mode and {@code ON DUPLICATE KEY UPDATE} replaces the
 * native {@code ON CONFLICT}; the catalog is built by {@link KingbaseCatalogFactory} and must be a
 * {@link MySqlCatalog} that opens and reads real metadata; {@link
 * #testMySqlModeUpsertKeepsSingleRow} executes the generated upsert twice and checks that the
 * duplicate key updates the row instead of inserting a second one.
 */
@Disabled(
        "Needs a KingbaseES cluster initialised in MySQL compatibility mode and a KM license; see the class javadoc")
public class JdbcKingbaseMySqlModeIT extends AbstractJdbcIT {

    private static final String KINGBASE_IMAGE =
            System.getProperty("kingbase.mysql.image", "seatunnelhub/kingbase:v8r6");
    private static final String KINGBASE_CONTAINER_HOST = "e2e_KINGBASEDb";
    private static final String KINGBASE_DATABASE = "test";
    private static final String KINGBASE_SOURCE = "e2e_mysql_mode_source";
    private static final String KINGBASE_SINK = "e2e_mysql_mode_sink";
    private static final String KINGBASE_CATALOG_TABLE = "e2e_mysql_mode_catalog";

    private static final String KINGBASE_USERNAME = "SYSTEM";
    private static final String KINGBASE_PASSWORD = "123456";
    private static final int KINGBASE_PORT = 54321;
    private static final String KINGBASE_URL = "jdbc:kingbase8://" + HOST + ":%s/test";
    private static final String DRIVER_CLASS = "com.kingbase8.Driver";
    private static final String KM_LICENSE_PROPERTY = "kingbase.km.license";

    private static final List<String> CONFIG_FILE =
            Arrays.asList("/jdbc_kingbase_mysql_mode_source_and_sink.conf");

    private static final String[] FIELD_NAMES = {
        "id",
        "c_tinyint",
        "c_smallint",
        "c_int",
        "c_bigint",
        "c_float",
        "c_double",
        "c_decimal",
        "c_varchar",
        "c_datetime",
        "c_date",
        "c_time"
    };

    /** MySQL-mode compatible column types; native Kingbase/PostgreSQL would reject most of them. */
    private static final String CREATE_SQL =
            "create table %s \n"
                    + "(\n"
                    + "    id BIGINT PRIMARY KEY,\n"
                    + "    c_tinyint TINYINT,\n"
                    + "    c_smallint SMALLINT,\n"
                    + "    c_int INT,\n"
                    + "    c_bigint BIGINT,\n"
                    + "    c_float FLOAT,\n"
                    + "    c_double DOUBLE,\n"
                    + "    c_decimal DECIMAL(10,2),\n"
                    + "    c_varchar VARCHAR(100),\n"
                    + "    c_datetime DATETIME,\n"
                    + "    c_date DATE,\n"
                    + "    c_time TIME\n"
                    + ");\n";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(KINGBASE_URL, KINGBASE_PORT);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();

        String insertSql = insertTable(null, KINGBASE_SOURCE, FIELD_NAMES);

        return JdbcCase.builder()
                .dockerImage(KINGBASE_IMAGE)
                .networkAliases(KINGBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(KINGBASE_PORT)
                .localPort(KINGBASE_PORT)
                .jdbcTemplate(KINGBASE_URL)
                .jdbcUrl(jdbcUrl)
                .userName(KINGBASE_USERNAME)
                .password(KINGBASE_PASSWORD)
                .database(KINGBASE_DATABASE)
                .sourceTable(KINGBASE_SOURCE)
                .sinkTable(KINGBASE_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(KINGBASE_DATABASE)
                .catalogTable(KINGBASE_CATALOG_TABLE)
                .build();
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                (long) i,
                                (byte) i,
                                (short) i,
                                i,
                                (long) i,
                                1.5f,
                                2.5d,
                                BigDecimal.valueOf(i, 2),
                                "row-" + i,
                                LocalDateTime.of(2024, 1, 1, 12, 0, 0).plusSeconds(i),
                                LocalDate.of(2024, 1, 1).plusDays(i),
                                LocalTime.of(12, 0, 0).plusSeconds(i)
                            });
            rows.add(row);
        }
        return Pair.of(FIELD_NAMES, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(KINGBASE_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(KINGBASE_CONTAINER_HOST)
                        .withEnv("KINGBASE_SYSTEM_PASSWORD", KINGBASE_PASSWORD)
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KINGBASE_IMAGE)));
        String licensePath = System.getProperty(KM_LICENSE_PROPERTY, "");
        if (!licensePath.isEmpty()) {
            container.withFileSystemBind(licensePath, "/home/kingbase/license.dat");
        }
        container.addExposedPort(KINGBASE_PORT);
        return container;
    }

    /**
     * Builds the catalog through the real factory rather than a stub, so the MySQL-mode catalog
     * path (including its metadata connection and version query) is exercised against the server.
     */
    @Override
    protected void initCatalog() {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put(
                JdbcCommonOptions.URL.key(),
                jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost()));
        catalogConfig.put(JdbcCommonOptions.USERNAME.key(), KINGBASE_USERNAME);
        catalogConfig.put(JdbcCommonOptions.PASSWORD.key(), KINGBASE_PASSWORD);
        catalogConfig.put(JdbcCommonOptions.DRIVER.key(), DRIVER_CLASS);
        catalogConfig.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), "mysql");

        Catalog createdCatalog =
                new KingbaseCatalogFactory()
                        .createCatalog(
                                "kingbase_mysql_mode", ReadonlyConfig.fromMap(catalogConfig));
        Assertions.assertTrue(
                createdCatalog instanceof MySqlCatalog,
                "explicit compatible_mode=mysql must build a MySqlCatalog, got "
                        + createdCatalog.getClass().getName());
        createdCatalog.open();
        this.catalog = createdCatalog;

        CatalogTable sourceTable =
                createdCatalog.getTable(new TablePath(KINGBASE_DATABASE, null, KINGBASE_SOURCE));
        Assertions.assertNotNull(sourceTable);
        String[] catalogFieldNames = sourceTable.getTableSchema().getFieldNames();
        Assertions.assertEquals(FIELD_NAMES.length, catalogFieldNames.length);
        Assertions.assertTrue(
                Arrays.asList(catalogFieldNames).containsAll(Arrays.asList(FIELD_NAMES)),
                "catalog must report the created MySQL-mode columns, got "
                        + Arrays.toString(catalogFieldNames));
        Assertions.assertNotNull(
                sourceTable.getTableSchema().getPrimaryKey(),
                "catalog must report the primary key used by the upsert path");
    }

    /** Real value comparison between source and sink tables, not just the job exit code. */
    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
        defaultCompare(executeKey, FIELD_NAMES, "id");
    }

    /**
     * Executes the generated MySQL-style upsert twice with the same key. The duplicate key must
     * update the existing row instead of inserting a second one, which is what {@code ON DUPLICATE
     * KEY UPDATE} guarantees and the native {@code ON CONFLICT} statement would not be able to run
     * on a MySQL-mode server.
     */
    @Test
    public void testMySqlModeUpsertKeepsSingleRow() throws SQLException {
        String table = "e2e_mysql_mode_upsert";
        String[] fields = {"ID", "NAME", "AMOUNT"};
        String[] primaryKeys = {"ID"};

        try {
            executeSql("DROP TABLE IF EXISTS " + quoteIdentifier(table));
            executeSql(
                    "CREATE TABLE "
                            + quoteIdentifier(table)
                            + " (`ID` BIGINT PRIMARY KEY, `NAME` VARCHAR(100), `AMOUNT` INT)");
            connection.commit();

            KingbaseDialect dialect =
                    new KingbaseDialect("mysql", FieldIdeEnum.UPPERCASE.getValue());
            String upsertSql =
                    dialect.getUpsertStatement(KINGBASE_DATABASE, table, fields, primaryKeys)
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "MySQL mode must generate an upsert statement"));
            Assertions.assertTrue(
                    upsertSql.contains("ON DUPLICATE KEY UPDATE"),
                    "MySQL mode must not fall back to ON CONFLICT: " + upsertSql);
            Assertions.assertTrue(
                    upsertSql.contains("`ID`"),
                    "field_ide must be propagated to the MySQL-style statement: " + upsertSql);

            executeUpsert(upsertSql, 1L, "first", 10);
            executeUpsert(upsertSql, 1L, "second", 20);
            connection.commit();

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "SELECT `ID`, `NAME`, `AMOUNT` FROM "
                                            + quoteIdentifier(table))) {
                Assertions.assertTrue(resultSet.next(), "the upsert must leave one row");
                Assertions.assertEquals(1L, resultSet.getLong(1));
                Assertions.assertEquals("second", resultSet.getString(2));
                Assertions.assertEquals(20, resultSet.getInt(3));
                Assertions.assertFalse(
                        resultSet.next(),
                        "the duplicate key must update the row instead of inserting a new one");
            }
        } finally {
            try {
                connection.rollback();
                executeSql("DROP TABLE IF EXISTS " + quoteIdentifier(table));
                connection.commit();
            } catch (SQLException ignored) {
                // Best-effort cleanup only.
            }
        }
    }

    private void executeUpsert(String upsertSql, long id, String name, int amount)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            statement.setLong(1, id);
            statement.setString(2, name);
            statement.setInt(3, amount);
            statement.executeUpdate();
        }
    }

    private void executeSql(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
