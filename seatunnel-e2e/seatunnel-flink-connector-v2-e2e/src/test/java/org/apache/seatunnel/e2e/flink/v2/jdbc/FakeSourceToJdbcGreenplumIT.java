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

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToJdbcGreenplumIT extends FlinkContainer {

    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_CONTAINER_HOST = "flink_e2e_greenplum_sink";
    private static final int GREENPLUM_CONTAINER_PORT = 5432;
    private static final String GREENPLUM_HOST = "localhost";
    private static final int GREENPLUM_PORT = 5436;
    private static final String GREENPLUM_USER = "tester";
    private static final String GREENPLUM_PASSWORD = "pivotal";
    private static final String GREENPLUM_DRIVER = "org.postgresql.Driver";
    private static final String GREENPLUM_JDBC_URL = String.format(
            "jdbc:postgresql://%s:%s/testdb", GREENPLUM_HOST, GREENPLUM_PORT);

    private GenericContainer<?> greenplumServer;
    private Connection jdbcConnection;

    @BeforeEach
    public void startGreenplumContainer() throws ClassNotFoundException, SQLException {
        greenplumServer = new GenericContainer<>(GREENPLUM_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(GREENPLUM_CONTAINER_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        greenplumServer.setPortBindings(Lists.newArrayList(
                String.format("%s:%s", GREENPLUM_PORT, GREENPLUM_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(greenplumServer)).join();
        log.info("Greenplum container started");
        // wait for Greenplum fully start
        Class.forName(GREENPLUM_DRIVER);
        given().ignoreExceptions()
                .await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(() -> initializeJdbcConnection());
        initializeJdbcTable();
    }

    @Test
    public void testFakeSourceToJdbcGreenplumSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/fakesource_to_jdbc_greenplum.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        String sql = "select age, name from test";
        List<Object> result = new ArrayList<>();
        try (Statement statement = jdbcConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                        resultSet.getInt("age"),
                        resultSet.getString("name")));
            }
        }
        Assertions.assertEquals(false, result.isEmpty());
    }

    private void initializeJdbcConnection() throws SQLException {
        jdbcConnection = DriverManager.getConnection(GREENPLUM_JDBC_URL,
                GREENPLUM_USER, GREENPLUM_PASSWORD);
    }

    private void initializeJdbcTable() throws SQLException {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = "CREATE TABLE test (\n" +
                    "age INT NOT NULL,\n" +
                    "name VARCHAR(255) NOT NULL\n" +
                    ")";
            statement.execute(sql);
        }
    }

    @AfterEach
    public void closeGreenplumContainer() throws SQLException {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
    }
}
