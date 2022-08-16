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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.e2e.flink.FlinkContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@SuppressWarnings("checkstyle:MagicNumber")
@Slf4j
public class JdbcGreenplumToConsoleIT extends FlinkContainer {

    private static final String GREENPLUM_IMAGE = "datagrip/greenplum:6.8";
    private static final String GREENPLUM_HOSTNAME = "flink_e2e_greenplum";
    private static final String GREENPLUM_DRIVER = "org.postgresql.Driver";
    private static final int GREENPLUM_PORT = 5432;
    private static final String GREENPLUM_USER = "tester";
    private static final String GREENPLUM_PASSWORD = "pivotal";
    private static final String GREENPLUM_JDBC_URL = String.format("jdbc:postgresql://localhost:%s/testdb", GREENPLUM_PORT);

    private GenericContainer<?> greenplumServer;
    private Connection jdbcConnection;

    @BeforeEach
    public void startGreenplumContainer() throws ClassNotFoundException, SQLException {
        greenplumServer = new GenericContainer<>(GREENPLUM_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(GREENPLUM_HOSTNAME)
                .withLogConsumer(new Slf4jLogConsumer(log));
        greenplumServer.setPortBindings(Lists.newArrayList(
                String.format("%s:5432", GREENPLUM_PORT)));
        Startables.deepStart(Stream.of(greenplumServer)).join();
        log.info("Greenplum container started");
        // wait for Greenplum fully start
        Class.forName(GREENPLUM_DRIVER);
        given().ignoreExceptions()
                .await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(() -> initializeJdbcConnection());
        initializeJdbcTable();
        batchInsertData();
    }

    @Test
    public void testJdbcGreenplumToConsoleSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_greenplum_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
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

    private void batchInsertData() throws SQLException {
        int batchSize = 100;
        String sql = "insert into test(age, name) values(?, ?)";

        try {
            jdbcConnection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = jdbcConnection.prepareStatement(sql)) {
                for (int i = 1; i <= batchSize; i++) {
                    preparedStatement.setInt(1, i);
                    preparedStatement.setString(2, String.format("test_%s", i));
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
    }
}
