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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class DorisCDCSinkIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "zykkk/doris:1.2.2.1-avx2-x86_84";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "doris_cdc_e2e";
    private static final int DORIS_DOCKER_PORT = 9030;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String SR_DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    private static final String DDL_SINK =
            "create table "
                    + DATABASE
                    + "."
                    + SINK_TABLE
                    + " (\n"
                    + "  uu_id          BIGINT,\n"
                    + "  name           VARCHAR(128),\n"
                    + "  score          INT\n"
                    + ")ENGINE=OLAP\n"
                    + "UNIQUE KEY(`uu_id`)\n"
                    + "DISTRIBUTED BY HASH(`uu_id`) BUCKETS 1\n"
                    + "PROPERTIES (\n"
                    + "\"replication_allocation\" = \"tag.location.default: 1\""
                    + ")";

    private Connection jdbcConnection;
    private GenericContainer<?> dorisServer;

    @BeforeAll
    @Override
    public void startUp() {
        dorisServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(DORIS_DOCKER_PORT)
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(dorisServer)).join();
        log.info("doris container started");
        // wait for doris fully start
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (dorisServer != null) {
            dorisServer.close();
        }
    }

    @TestTemplate
    public void testdorisSink(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/write-cdc-changelog-to-doris.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String sinkSql = String.format("select * from %s.%s", DATABASE, SINK_TABLE);
        Set<List<Object>> actual = new HashSet<>();
        try (Statement sinkStatement = jdbcConnection.createStatement()) {
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            while (sinkResultSet.next()) {
                List<Object> row =
                        Arrays.asList(
                                sinkResultSet.getLong("uu_id"),
                                sinkResultSet.getString("name"),
                                sinkResultSet.getInt("score"));
                actual.add(row);
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }

    private void initializeJdbcConnection() throws Exception {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(SR_DRIVER_JAR)}, DorisCDCSinkIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection =
                driver.connect(
                        String.format(
                                "jdbc:mysql://%s:%s",
                                dorisServer.getHost(), dorisServer.getFirstMappedPort()),
                        props);
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(
                    "ADMIN SET FRONTEND CONFIG (\"enable_batch_delete_by_default\" = \"true\");");
        }
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            // create databases
            statement.execute("create database test");
            // create sink table
            statement.execute(DDL_SINK);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }
}
