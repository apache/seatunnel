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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcPostgresIT extends FlinkContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcPostgresIT.class);
    private PostgreSQLContainer<?> pg;

    @SuppressWarnings("checkstyle:MagicNumber")
    @BeforeEach
    public void startPostgreSqlContainer() throws InterruptedException, ClassNotFoundException, SQLException {
        pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:alpine3.16"))
            .withNetwork(NETWORK)
            .withNetworkAliases("postgresql")
            .withUrlParam("max_prepared_transactions", "10")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        Startables.deepStart(Stream.of(pg)).join();
        LOGGER.info("Postgres container started");
        Class.forName(pg.getDriverClassName());
        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> initializeJdbcTable());
        batchInsertData();
    }

    private void initializeJdbcTable() {
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            Statement statement = connection.createStatement();
            String source = "create table source(\n" +
                "user_id bigserial NOT NULL PRIMARY KEY,\n" +
                "name char(10),\n" +
                "age INT\n" +
                ")";
            String sink = "create table sink(\n" +
                "user_id bigserial NOT NULL PRIMARY KEY,\n" +
                "name char(10),\n" +
                "age INT\n" +
                ")";
            statement.execute(source);
            statement.execute(sink);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Mysql table failed!", e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void batchInsertData() throws SQLException {
        String sql = "insert into source(name, age) values(?,?)";
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (List row : generateTestDataset()) {
                preparedStatement.setString(1, (String) row.get(0));
                preparedStatement.setInt(2, (Integer) row.get(1));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        }
    }

    @Test
    public void testJdbcPostgresSourceAndSink() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_postgres_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertIterableEquals(generateTestDataset(), queryResult());
    }

    @Test
    public void testJdbcPostgresSourceAndSinkParallel() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/jdbc_postgres_source_and_sink_parallel.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        //Because it is parallel, it is out of order
        Assertions.assertThrows(org.opentest4j.AssertionFailedError.class,
            () -> Assertions.assertIterableEquals(generateTestDataset(), queryResult()));

        //Sorting is required, because it is read in parallel, so there will be out of order
        List<List> sortedResult = queryResult().stream().sorted(Comparator.comparing(list -> (Integer) list.get(1)))
            .collect(Collectors.toList());
        Assertions.assertIterableEquals(generateTestDataset(), sortedResult);
    }

    @Test
    public void testJdbcPostgresSourceAndSinkParallelUpperLower() throws SQLException, IOException, InterruptedException {
        Container.ExecResult execResult =
            executeSeaTunnelFlinkJob("/jdbc/jdbc_postgres_source_and_sink_parallel_upper_lower.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        //Because it is parallel, it is out of order
        Assertions.assertThrows(org.opentest4j.AssertionFailedError.class,
            () -> Assertions.assertIterableEquals(generateTestDataset().stream().limit(50).collect(Collectors.toList()), queryResult()));

        //Sorting is required, because it is read in parallel, so there will be out of order
        List<List> sortedResult = queryResult().stream().sorted(Comparator.comparing(list -> (Integer) list.get(1)))
            .collect(Collectors.toList());

        //lower=1 upper=50
        List<List> limit50 = generateTestDataset().stream().limit(50).collect(Collectors.toList());
        Assertions.assertIterableEquals(limit50, sortedResult);
    }

    private List<List> queryResult() {
        List<List> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            Statement statement = connection.createStatement();
            String sql = "select name , age from sink ";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(
                    Arrays.asList(
                        resultSet.getString(1).replace(" ", ""),
                        resultSet.getInt(2)
                    )
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query result data failed!", e);
        }
        return result;
    }

    private static List<List> generateTestDataset() {
        List<List> rows = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            rows.add(
                Arrays.asList(
                    String.format("user_%s", i),
                    i
                ));
        }
        return rows;
    }

    @AfterEach
    public void closePostgreSqlContainer() {
        if (pg != null) {
            pg.stop();
        }
    }
}
