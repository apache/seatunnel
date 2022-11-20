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

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
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

@Slf4j
public class StarRocksIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.2.1";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "starrocks_e2e";
    private static final int SR_DOCKER_PORT = 9030;
    private static final int SR_PORT = 9033;

    private static final String URL = "jdbc:mysql://%s:" + SR_PORT;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String SR_DRIVER_JAR = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String COLUMN_STRING = "BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL";

    private static final String DDL_SOURCE = "create table " + DATABASE + "." + SOURCE_TABLE + " (\n" +
            "  BIGINT_COL     BIGINT,\n" +
            "  LARGEINT_COL   LARGEINT,\n" +
            "  SMALLINT_COL   SMALLINT,\n" +
            "  TINYINT_COL    TINYINT,\n" +
            "  BOOLEAN_COL    BOOLEAN,\n" +
            "  DECIMAL_COL    DECIMAL,\n" +
            "  DOUBLE_COL     DOUBLE,\n" +
            "  FLOAT_COL      FLOAT,\n" +
            "  INT_COL        INT,\n" +
            "  CHAR_COL       CHAR,\n" +
            "  VARCHAR_11_COL VARCHAR(11),\n" +
            "  STRING_COL     STRING,\n" +
            "  DATETIME_COL   DATETIME,\n" +
            "  DATE_COL       DATE\n" +
            ")ENGINE=OLAP\n" +
            "DUPLICATE KEY(`BIGINT_COL`)\n" +
            "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"," +
            "\"storage_format\" = \"DEFAULT\"" +
            ")";

    private static final String DDL_SINK = "create table " + DATABASE + "." + SINK_TABLE + " (\n" +
            "  BIGINT_COL     BIGINT,\n" +
            "  LARGEINT_COL   LARGEINT,\n" +
            "  SMALLINT_COL   SMALLINT,\n" +
            "  TINYINT_COL    TINYINT,\n" +
            "  BOOLEAN_COL    BOOLEAN,\n" +
            "  DECIMAL_COL    DECIMAL,\n" +
            "  DOUBLE_COL     DOUBLE,\n" +
            "  FLOAT_COL      FLOAT,\n" +
            "  INT_COL        INT,\n" +
            "  CHAR_COL       CHAR,\n" +
            "  VARCHAR_11_COL VARCHAR(11),\n" +
            "  STRING_COL     STRING,\n" +
            "  DATETIME_COL   DATETIME,\n" +
            "  DATE_COL       DATE\n" +
            ")ENGINE=OLAP\n" +
            "DUPLICATE KEY(`BIGINT_COL`)\n" +
            "DISTRIBUTED BY HASH(`BIGINT_COL`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\"," +
            "\"storage_format\" = \"DEFAULT\"" +
            ")";

    private static final String INIT_DATA_SQL = "insert into " + DATABASE + "." + SOURCE_TABLE + " (\n" +
            "  BIGINT_COL,\n" +
            "  LARGEINT_COL,\n" +
            "  SMALLINT_COL,\n" +
            "  TINYINT_COL,\n" +
            "  BOOLEAN_COL,\n" +
            "  DECIMAL_COL,\n" +
            "  DOUBLE_COL,\n" +
            "  FLOAT_COL,\n" +
            "  INT_COL,\n" +
            "  CHAR_COL,\n" +
            "  VARCHAR_11_COL,\n" +
            "  STRING_COL,\n" +
            "  DATETIME_COL,\n" +
            "  DATE_COL\n" +
            ")values(\n" +
            "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?\n" +
            ")";

    private Connection jdbcConnection;
    private GenericContainer<?> starRocksServer;
    private static final List<SeaTunnelRow> TEST_DATASET = generateTestDataSet();
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + SR_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        starRocksServer = new GenericContainer<>(DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        starRocksServer.setPortBindings(Lists.newArrayList(
                String.format("%s:%s", SR_PORT, SR_DOCKER_PORT)));
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
            SeaTunnelRow row = new SeaTunnelRow(
                new Object[]{
                    Long.valueOf(i),
                    Long.valueOf(1123456),
                    Short.parseShort("1"),
                    Byte.parseByte("1"),
                    Boolean.FALSE,
                    BigDecimal.valueOf(2222243, 1),
                    Double.parseDouble("2222243.2222243"),
                    Float.parseFloat("222224"),
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
    public void testStarRocksSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/starrocks-jdbc-to-starrocks.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try  {
            assertHasData(SINK_TABLE);

            String sourceSql = String.format("select * from %s.%s", DATABASE, SOURCE_TABLE);
            String sinkSql = String.format("select * from %s.%s", DATABASE, SINK_TABLE);
            List<String> columnList = Arrays.stream(COLUMN_STRING.split(",")).map(x -> x.trim()).collect(Collectors.toList());
            Statement sourceStatement = jdbcConnection.createStatement();
            Statement sinkStatement = jdbcConnection.createStatement();
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(sourceResultSet.getMetaData().getColumnCount(), sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : columnList) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue = IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue = IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                    }
                }
            }
            //Check the row numbers is equal
            sourceResultSet.last();
            sinkResultSet.last();
            Assertions.assertEquals(sourceResultSet.getRow(), sinkResultSet.getRow());
            clearSinkTable();
        } catch (Exception e) {
            throw new RuntimeException("get starRocks connection error", e);
        }
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException {
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new URL(SR_DRIVER_JAR)}, StarRocksIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection =  driver.connect(String.format(URL, starRocksServer.getHost()), props);
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute("create database test");
            // create source table
            statement.execute(DDL_SOURCE);
            // create sink table
            statement.execute(DDL_SINK);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void batchInsertData() {
        List<SeaTunnelRow> rows = TEST_DATASET;
        try  {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(INIT_DATA_SQL)) {
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
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
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
}
