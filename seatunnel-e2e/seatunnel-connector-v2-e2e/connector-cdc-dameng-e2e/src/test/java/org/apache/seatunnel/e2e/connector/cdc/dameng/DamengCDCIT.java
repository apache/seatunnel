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

package org.apache.seatunnel.e2e.connector.cdc.dameng;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
public class DamengCDCIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "laglangyue/dmdb8";
    private static final String HOST = "dameng-server";
    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";
    private static final String USERNAME = "SYSDBA";
    private static final String PASSWORD = "SYSDBA";
    private GenericContainer<?> damengServer;
    private Connection jdbcConnection;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting containers...");
        damengServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(5236)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("/docker/dm.ini"),
                                "/opt/dmdbms/data/DAMENG/dm.ini")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("/docker/dmarch.ini"),
                                "/opt/dmdbms/data/DAMENG/dmarch.ini")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(damengServer)).join();
        // wait for Dmdb fully start
        Class.forName(DRIVER_CLASS);
        given().ignoreExceptions()
                .await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initializeDmanegSetting();
        initializeTable();
        insertSourceTable();
        log.info("Containers are started.");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        log.info("Stopping containers...");
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (damengServer != null) {
            damengServer.stop();
        }
        log.info("Containers are stopped.");
    }

    @TestTemplate
    public void testCDC(TestContainer container) throws Exception {
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/dameng_cdc_to_jdbc.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        // snapshot stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> checkSourceAndSinkTableDataEquals());

        // insert update delete
        updateSourceTable();

        // stream stage
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> checkSourceAndSinkTableDataEquals());
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:dm://%s:%s",
                                damengServer.getHost(), damengServer.getFirstMappedPort()),
                        USERNAME,
                        PASSWORD);
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.executeQuery("SELECT 1").next();
        }
    }

    private void initializeDmanegSetting() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            // Using config file:
            //  dm.ini
            //      ARCH_INI = 1
            //  dmarch.ini
            //      ARCHIVE_LOCAL...
            //
            // Using SQL config:
            // statement.execute("ALTER DATABASE MOUNT");
            // statement.execute("ALTER DATABASE ADD ARCHIVELOG
            // 'TYPE=LOCAL,DEST=/bak/archlog,FILE_SIZE=64,SPACE_LIMIT=1024'");
            // statement.execute("ALTER DATABASE ARCHIVELOG");
            // statement.execute("ALTER DATABASE OPEN");
            // statement.execute("ALTER SYSTEM SET 'RLOG_APPEND_LOGIC'=2 MEMORY");

            ResultSet resultSet =
                    statement.executeQuery(
                            "SELECT PARA_NAME, PARA_VALUE FROM v$dm_ini WHERE PARA_NAME IN ('ARCH_INI','RLOG_APPEND_LOGIC')");

            Integer rlogAppendLogic = null;
            Integer archIni = null;
            while (resultSet.next()) {
                String paraName = resultSet.getString("PARA_NAME");
                int paraValue = resultSet.getInt("PARA_VALUE");
                if (paraName.equalsIgnoreCase("RLOG_APPEND_LOGIC")) {
                    rlogAppendLogic = paraValue;
                } else if (paraName.equalsIgnoreCase("ARCH_INI")) {
                    archIni = paraValue;
                }
            }

            Assertions.assertEquals(1, archIni);
            Assertions.assertEquals(2, rlogAppendLogic);
        }
    }

    private void initializeTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("CREATE SCHEMA CDC_SCHEMA AUTHORIZATION SYSDBA");
            statement.execute(
                    "CREATE TABLE CDC_SCHEMA.SOURCE_TABLE(id INT PRIMARY KEY, name VARCHAR(128) NULL, score INT NULL, d1 DATE NULL, t1 TIME NULL, ts1 TIMESTAMP NULL, dt1 DATETIME NULL)");
            statement.execute(
                    "CREATE TABLE CDC_SCHEMA.SINK_TABLE(id INT PRIMARY KEY, name VARCHAR(128) NULL, score INT NULL, d1 DATE NULL, t1 TIME NULL, ts1 TIMESTAMP NULL, dt1 DATETIME NULL)");
        }
    }

    private void insertSourceTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(
                    "INSERT INTO CDC_SCHEMA.SOURCE_TABLE(id, name, score) VALUES(1, 'A', 100)");
            statement.execute(
                    "INSERT INTO CDC_SCHEMA.SOURCE_TABLE(id, name, score) VALUES(2, 'B', 100)");
            statement.execute(
                    "INSERT INTO CDC_SCHEMA.SOURCE_TABLE(id, name, score, d1, t1, ts1, dt1) VALUES(3, 'C', 100, '2022-12-12', '12:12:12', '2022-12-12 12:12:12.111', '2022-12-12 12:12:12.111')");
            jdbcConnection.commit();
        }
    }

    private void updateSourceTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(
                    "UPDATE CDC_SCHEMA.SOURCE_TABLE set score = 99, d1 = '2022-12-13', t1 = '13:12:12', ts1 = '2022-12-13 13:12:12.111', dt1 = '2022-12-13 13:12:12.111' where score = 100");
            statement.execute(
                    "INSERT INTO CDC_SCHEMA.SOURCE_TABLE(id, name, score) VALUES(4, 'D', 100)");
            jdbcConnection.commit();
        }
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("DELETE FROM CDC_SCHEMA.SOURCE_TABLE where id = 2");
            jdbcConnection.commit();
        }
    }

    private void checkSourceAndSinkTableDataEquals() {
        Assertions.assertIterableEquals(
                querySql("SELECT * FROM CDC_SCHEMA.SOURCE_TABLE ORDER BY ID"),
                querySql("SELECT * FROM CDC_SCHEMA.SINK_TABLE ORDER BY ID"));
    }

    private List<List<Object>> querySql(String sql) {
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
