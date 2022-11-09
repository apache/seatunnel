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

package org.apache.seatunnel.e2e.flink.v2.jdbc;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class JdbcGreenplumIT extends FlinkContainer {

    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_CONTAINER_HOST = "flink_e2e_greenplum";
    private static final int GREENPLUM_CONTAINER_PORT = 5432;
    private static final int GREENPLUM_PORT = 5435;
    private static final String GREENPLUM_USER = "tester";
    private static final String GREENPLUM_PASSWORD = "pivotal";
    private static final String GREENPLUM_DRIVER = "org.postgresql.Driver";
    private static final String GREENPLUM_JDBC_URL = "jdbc:postgresql://%s:%s/testdb";
    private static final List<List> TEST_DATASET = generateTestDataset();
    private static final String THIRD_PARTY_PLUGINS_URL = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";

    private GenericContainer<?> greenplumServer;
    private Connection jdbcConnection;

    @BeforeEach
    public void startGreenplumContainer() throws ClassNotFoundException, SQLException {
        greenplumServer = new GenericContainer<>(GREENPLUM_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(GREENPLUM_CONTAINER_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(GREENPLUM_IMAGE)));
        greenplumServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", GREENPLUM_PORT, GREENPLUM_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(greenplumServer)).join();
        log.info("Greenplum container started");
        // wait for Greenplum fully start
        Class.forName(GREENPLUM_DRIVER);
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(360, TimeUnit.SECONDS)
            .untilAsserted(() -> initializeJdbcConnection());
        initializeJdbcTable();
        batchInsertData();
    }

    @Test
    public void testJdbcGreenplumSourceAndSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_greenplum_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        String sql = "select age, name from sink order by age asc";
        List<List> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                    resultSet.getInt(1),
                    resultSet.getString(2)));
            }
        }
        Assertions.assertIterableEquals(TEST_DATASET, result);
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(String.format(
                GREENPLUM_JDBC_URL, greenplumServer.getHost(), GREENPLUM_PORT),
            GREENPLUM_USER, GREENPLUM_PASSWORD);
    }

    private void initializeJdbcTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            String createSource = "CREATE TABLE source (\n" +
                "age INT NOT NULL,\n" +
                "name VARCHAR(255) NOT NULL\n" +
                ")";
            String createSink = "CREATE TABLE sink (\n" +
                "age INT NOT NULL,\n" +
                "name VARCHAR(255) NOT NULL\n" +
                ")";
            statement.execute(createSource);
            statement.execute(createSink);
        }
    }

    private static List<List> generateTestDataset() {
        List<List> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Arrays.asList(i, String.format("test_%s", i)));
        }
        return rows;
    }

    private void batchInsertData() throws SQLException {
        String sql = "insert into source(age, name) values(?, ?)";

        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql)) {
                for (List row : TEST_DATASET) {
                    preparedStatement.setInt(1, (Integer) row.get(0));
                    preparedStatement.setString(2, (String) row.get(1));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            jdbcConnection.commit();
        } catch (SQLException e) {
            jdbcConnection.rollback();
            throw e;
        }
    }

    @AfterEach
    public void closeGreenplumContainer() throws SQLException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (greenplumServer != null) {
            greenplumServer.stop();
        }
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container) throws IOException, InterruptedException {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + THIRD_PARTY_PLUGINS_URL);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    }
}
