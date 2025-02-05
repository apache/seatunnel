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

package org.apache.seatunnel.e2e.connector.starrocks;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class StarRocksIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.2.1";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "starrocks_e2e";
    private static final int SR_DOCKER_PORT = 9030;
    private static final int SR_PORT = 9033;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String URL = "jdbc:mysql://%s:" + SR_PORT;
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String SR_DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String COLUMN_STRING =
            "BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL";

    private static final String DDL_SOURCE =
            "create table "
                    + DATABASE
                    + "."
                    + SOURCE_TABLE
                    + " (\n"
                    + "  BIGINT_COL     BIGINT,\n"
                    // add comment for test
                    + "  LARGEINT_COL   LARGEINT COMMENT '''N''-N',\n"
                    + "  SMALLINT_COL   SMALLINT COMMENT '\\N\\-N',\n"
                    + "  TINYINT_COL    TINYINT,\n"
                    + "  BOOLEAN_COL    BOOLEAN,\n"
                    + "  DECIMAL_COL    Decimal(12, 1),\n"
                    + "  DOUBLE_COL     DOUBLE,\n"
                    + "  FLOAT_COL      FLOAT,\n"
                    + "  INT_COL        INT,\n"
                    + "  CHAR_COL       CHAR,\n"
                    + "  VARCHAR_11_COL VARCHAR(11),\n"
                    + "  STRING_COL     STRING,\n"
                    + "  DATETIME_COL   DATETIME,\n"
                    + "  DATE_COL       DATE\n"
                    + ")ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`BIGINT_COL`)\n"
                    + "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"in_memory\" = \"false\","
                    + "\"storage_format\" = \"DEFAULT\""
                    + ")";

    private static final String DDL_FAKE_SINK_TABLE =
            "create table "
                    + DATABASE
                    + "."
                    + "fake_table_sink"
                    + " (\n"
                    + "  id     BIGINT,\n"
                    + "  c_string   STRING,\n"
                    + "  c_boolean    BOOLEAN,\n"
                    + "  c_tinyint    TINYINT,\n"
                    + "  c_int        INT,\n"
                    + "  c_bigint     BIGINT,\n"
                    + "  c_float      FLOAT,\n"
                    + "  c_double     DOUBLE,\n"
                    + "  c_decimal    Decimal(2, 1),\n"
                    + "  c_date       DATE\n"
                    + ")ENGINE=OLAP\n"
                    + "DUPLICATE KEY(`id`)\n"
                    + "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_num\" = \"1\",\n"
                    + "\"in_memory\" = \"false\","
                    + "\"storage_format\" = \"DEFAULT\""
                    + ")";

    private static final String INIT_DATA_SQL =
            "insert into "
                    + DATABASE
                    + "."
                    + SOURCE_TABLE
                    + " (\n"
                    + "  BIGINT_COL,\n"
                    + "  LARGEINT_COL,\n"
                    + "  SMALLINT_COL,\n"
                    + "  TINYINT_COL,\n"
                    + "  BOOLEAN_COL,\n"
                    + "  DECIMAL_COL,\n"
                    + "  DOUBLE_COL,\n"
                    + "  FLOAT_COL,\n"
                    + "  INT_COL,\n"
                    + "  CHAR_COL,\n"
                    + "  VARCHAR_11_COL,\n"
                    + "  STRING_COL,\n"
                    + "  DATETIME_COL,\n"
                    + "  DATE_COL\n"
                    + ")values(\n"
                    + "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?\n"
                    + ")";

    private Connection jdbcConnection;
    private GenericContainer<?> starRocksServer;
    private static final List<SeaTunnelRow> TEST_DATASET = generateTestDataSet();

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + SR_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        starRocksServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(new Slf4jLogConsumer(log));
        starRocksServer.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", SR_PORT, SR_DOCKER_PORT)));
        Startables.deepStart(Stream.of(starRocksServer)).join();
        log.info("StarRocks container started");
        // wait for starrocks fully start
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
        batchInsertData();
    }

    private static List<SeaTunnelRow> generateTestDataSet() {

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                Long.valueOf(i),
                                Long.valueOf(1123456),
                                Short.parseShort("1"),
                                Byte.parseByte("1"),
                                Boolean.FALSE,
                                BigDecimal.valueOf(12345, 1),
                                Double.parseDouble("2222243.2222243"),
                                Float.parseFloat("22.17"),
                                Integer.parseInt("1"),
                                "a",
                                "VARCHAR_COL",
                                "STRING_COL",
                                "2022-08-13 17:35:59",
                                "2022-08-13"
                            });
            rows.add(row);
        }
        return rows;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (starRocksServer != null) {
            starRocksServer.close();
        }
    }

    @TestTemplate
    public void testStarRocksSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/starrocks-thrift-to-starrocks-streamload.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try {
            assertHasData(SINK_TABLE);

            String sourceSql =
                    String.format(
                            "select * from %s.%s order by BIGINT_COL ", DATABASE, SOURCE_TABLE);
            String sinkSql =
                    String.format("select * from %s.%s order by BIGINT_COL ", DATABASE, SINK_TABLE);
            List<String> columnList =
                    Arrays.stream(COLUMN_STRING.split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
            Statement sourceStatement = jdbcConnection.createStatement();
            Statement sinkStatement = jdbcConnection.createStatement();
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            log.info(container.getServerLogs());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            Assertions.assertEquals(String.valueOf(source), String.valueOf(sink));
                        }
                    }
                }
            }
            Assertions.assertFalse(sinkResultSet.next());
            clearSinkTable();
        } catch (Exception e) {
            throw new RuntimeException("get starRocks connection error", e);
        }
    }

    @TestTemplate
    public void testSinkWithCatalogTableNameOnly(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake-to-starrocks.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private void initializeJdbcConnection()
            throws SQLException, ClassNotFoundException, MalformedURLException,
                    InstantiationException, IllegalAccessException {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(SR_DRIVER_JAR)}, StarRocksIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection = driver.connect(String.format(URL, starRocksServer.getHost()), props);
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute("create database test");
            // create source table
            statement.execute(DDL_SOURCE);
            // create sink table
            statement.execute(DDL_FAKE_SINK_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void batchInsertData() {
        List<SeaTunnelRow> rows = TEST_DATASET;
        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement =
                    jdbcConnection.prepareStatement(INIT_DATA_SQL)) {
                for (int i = 0; i < rows.size(); i++) {
                    for (int index = 0; index < rows.get(i).getFields().length; index++) {
                        preparedStatement.setObject(index + 1, rows.get(i).getFields()[index]);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            jdbcConnection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new RuntimeException("get connection error", exception);
        }
    }

    private void assertHasData(String table) {
        String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
        try (Statement statement = jdbcConnection.createStatement();
                ResultSet source = statement.executeQuery(sql)) {
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("test starrocks server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test starrocks server image error", e);
        }
    }

    @Test
    public void testCatalog() {
        TablePath tablePathStarRocksSource = TablePath.of("test", "e2e_table_source");
        TablePath tablePathStarRocksSink = TablePath.of("test", "e2e_table_source_2");
        StarRocksCatalog starRocksCatalog =
                new StarRocksCatalog(
                        "StarRocks",
                        "root",
                        PASSWORD,
                        String.format(URL, starRocksServer.getHost()),
                        "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n ${rowtype_fields}\n ) ENGINE=OLAP \n  DUPLICATE KEY(`BIGINT_COL`) \n COMMENT '${comment}' \n DISTRIBUTED BY HASH (BIGINT_COL) BUCKETS 1 \n PROPERTIES (\n   \"replication_num\" = \"1\", \n  \"in_memory\" = \"false\" , \n  \"storage_format\" = \"DEFAULT\"  \n )");
        starRocksCatalog.open();
        CatalogTable catalogTable = starRocksCatalog.getTable(tablePathStarRocksSource);
        catalogTable =
                CatalogTable.of(
                        catalogTable.getTableId(),
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        "test'1'");
        // sink tableExists ?
        starRocksCatalog.dropTable(tablePathStarRocksSink, true);
        boolean tableExistsBefore = starRocksCatalog.tableExists(tablePathStarRocksSink);
        Assertions.assertFalse(tableExistsBefore);
        // create table
        starRocksCatalog.createTable(tablePathStarRocksSink, catalogTable, true);
        boolean tableExistsAfter = starRocksCatalog.tableExists(tablePathStarRocksSink);
        Assertions.assertTrue(tableExistsAfter);
        // isExistsData ?
        boolean existsDataBefore = starRocksCatalog.isExistsData(tablePathStarRocksSink);
        Assertions.assertFalse(existsDataBefore);
        // insert one data
        String customSql =
                "insert into "
                        + DATABASE
                        + "."
                        + "e2e_table_source_2"
                        + " (\n"
                        + "  BIGINT_COL,\n"
                        + "  LARGEINT_COL,\n"
                        + "  SMALLINT_COL,\n"
                        + "  TINYINT_COL,\n"
                        + "  BOOLEAN_COL,\n"
                        + "  DECIMAL_COL,\n"
                        + "  DOUBLE_COL,\n"
                        + "  FLOAT_COL,\n"
                        + "  INT_COL,\n"
                        + "  CHAR_COL,\n"
                        + "  VARCHAR_11_COL,\n"
                        + "  STRING_COL,\n"
                        + "  DATETIME_COL,\n"
                        + "  DATE_COL\n"
                        + ")values(\n"
                        + "\t 999,12345,1,1,false,1.1,9.9,2.5,3,'A','ADC','ASEDF','2022-08-13 17:35:59','2022-08-13'\n"
                        + ")";
        starRocksCatalog.executeSql(tablePathStarRocksSink, customSql);
        boolean existsDataAfter = starRocksCatalog.isExistsData(tablePathStarRocksSink);
        Assertions.assertTrue(existsDataAfter);
        // truncateTable
        starRocksCatalog.truncateTable(tablePathStarRocksSink, true);
        Assertions.assertFalse(starRocksCatalog.isExistsData(tablePathStarRocksSink));
        // drop table
        starRocksCatalog.dropTable(tablePathStarRocksSink, true);
        Assertions.assertFalse(starRocksCatalog.tableExists(tablePathStarRocksSink));
        starRocksCatalog.close();
    }
}
