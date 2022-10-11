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

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.connectors.seatunnel.jdbc.util.JdbcCompareUtil;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class JdbcStarRocksdbIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "d87904488/starrocks-starter:2.2.1";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "e2e_starRocksdb";
    private static final int SR_PORT = 9030;
    private static final String URL = "jdbc:mysql://%s:" + SR_PORT;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String SOURCE_TABLE = "e2e_table_source";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String SR_DRIVER_JAR = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String COLUMN_STRING = "BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL";

    private Connection jdbcConnection;
    private GenericContainer<?> starRocksServer;

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
                String.format("%s:%s", SR_PORT, SR_PORT)));
        Startables.deepStart(Stream.of(starRocksServer)).join();
        log.info("StarRocks container started");
        // wait for starrocks fully start
        Class.forName(DRIVER_CLASS);
        given().ignoreExceptions()
                .await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
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
    @DisplayName("JDBC-SR E2E test")
    public void testJdbcStarRocks(TestContainer container) throws IOException, InterruptedException, SQLException {
        assertHasData(SOURCE_TABLE);
        Container.ExecResult execResult = container.executeJob("/jdbc_starrocks_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        assertHasData(SINK_TABLE);
        JdbcCompareUtil.compare(jdbcConnection, String.format("select * from %s.%s limit 1", DATABASE, SOURCE_TABLE),
                String.format("select * from %s.%s limit 1", DATABASE, SINK_TABLE), COLUMN_STRING);
        clearSinkTable();
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(String.format(
                URL, starRocksServer.getHost()), USERNAME, PASSWORD);
    }

    private void assertHasData(String table) {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test starRocks image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("test starRocks image error", e);
        }
    }

    private void initializeJdbcTable() {
        File file = ContainerUtil.getResourcesFile("/init/starrocks_init.conf");
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath("table_source_ddl") && config.hasPath("DML") && config.hasPath("table_sink_ddl");
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute("create database test");
            // create source table
            String sourceTableDDL = config.getString("table_source_ddl");
            statement.execute(sourceTableDDL);
            //init testdata
            String insertSQL = config.getString("DML");
            statement.execute(insertSQL);
            // create sink table
            String sinkTableDDL = config.getString("table_sink_ddl");
            statement.execute(sinkTableDDL);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }
}
