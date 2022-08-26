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

package org.apache.seatunnel.e2e.spark.v2.jdbc;

import org.apache.seatunnel.e2e.spark.SparkContainer;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class JdbcPhoenixIT extends SparkContainer {

    private static final String PHOENIX_DOCKER_IMAGE = "iteblog/hbase-phoenix-docker:1.0";

    private static final String PHOENIX_CONTAINER_HOST = "spark_e2e_phoenix_sink";
    private static final String PHOENIX_HOST = "localhost";

    private static final int PHOENIX_PORT = 8763;
    private static final int PHOENIX_CONTAINER_PORT = 8765;

    private static final String PHOENIX_CONNECT_URL = String.format("jdbc:phoenix:thin:url=http://%s:%s;serialization=PROTOBUF", PHOENIX_HOST, PHOENIX_PORT);
    private static final String PHOENIX_JDBC_DRIVER = "org.apache.phoenix.queryserver.client.Driver";

    private GenericContainer<?> phoenixServer;

    private Connection connection;

    @BeforeEach
    public void startPhoenixContainer() throws ClassNotFoundException, SQLException {
        phoenixServer = new GenericContainer<>(PHOENIX_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(PHOENIX_CONTAINER_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        phoenixServer.setPortBindings(Lists.newArrayList(
                String.format("%s:%s", PHOENIX_PORT, PHOENIX_CONTAINER_PORT)));
        Startables.deepStart(Stream.of(phoenixServer)).join();
        initializeJdbcConnection();
        log.info("phoenix container started");
        initializePhoenixTable();
        batchInsertData();
    }

    @Test
    public void testJdbcPhoenixSourceAndSink() throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/jdbc/jdbc_phoenix_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        String sql = "select age, name from test.sink order by age asc";
        List<List> result = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(Arrays.asList(
                        resultSet.getInt(1),
                        resultSet.getString(2)));
            }
        }
        Assertions.assertIterableEquals(generateTestDataset(), result);
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException {
        Class.forName(PHOENIX_JDBC_DRIVER);
        connection = DriverManager.getConnection(PHOENIX_CONNECT_URL);
    }

    private void initializePhoenixTable() {
        try  {
            Statement statement = connection.createStatement();
            String createSource = "create table test.source(\n" +
                    " name VARCHAR PRIMARY KEY,\n" +
                    " age INTEGER)";
            String createSink = "create table test.sink(\n" +
                    " name VARCHAR PRIMARY KEY,\n" +
                    " age INTEGER)";
            statement.execute(createSource);
            statement.execute(createSink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing  table failed!", e);
        }
    }

    @AfterEach
    public void closePhoenixContainer() throws SQLException {
        if (phoenixServer != null) {
            phoenixServer.stop();
        }
    }

    private static List<List> generateTestDataset() {
        List<List> rows = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            rows.add(Arrays.asList(i, String.format("test_%s", i)));
        }
        return rows;
    }

    private void batchInsertData() throws SQLException, ClassNotFoundException {
        String sql = "upsert into test.source(age, name) values(?, ?)";

        try {
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (List row : generateTestDataset()) {
                    preparedStatement.setInt(1, (Integer) row.get(0));
                    preparedStatement.setString(2, (String) row.get(1));
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }
}
