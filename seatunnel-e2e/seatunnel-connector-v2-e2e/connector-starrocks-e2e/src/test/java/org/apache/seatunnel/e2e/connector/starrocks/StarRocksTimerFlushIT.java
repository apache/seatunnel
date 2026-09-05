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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
public class StarRocksTimerFlushIT extends TestSuiteBase implements TestResource {

    private static final String STARROCKS_IMAGE = "seatunnelhub/starrocks-starter:2.2.1";
    private static final String STARROCKS_HOST = "starrocks_timer_flush_e2e";
    private static final int STARROCKS_QUERY_PORT = 9030;
    private static final String MYSQL_HOST = "mysql_starrocks_timer_flush_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "shop";
    private static final String SINK_DATABASE = "timer_flush";
    private static final String SINK_TABLE = "starrocks_timer_flush";
    private static final String JDBC_PLUGIN_LIB = "/tmp/seatunnel/plugins/Jdbc/lib";
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase = new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);
    private GenericContainer<?> starRocksServer;
    private Connection starRocksConnection;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(com.mysql.cj.jdbc.Driver.class).copyTo(container, JDBC_PLUGIN_LIB);
                DependencyJar.of(com.mysql.cj.jdbc.Driver.class)
                        .copyTo(container, MYSQL_CDC_PLUGIN_LIB);
            };

    @BeforeAll
    @Override
    public void startUp() {
        starRocksServer =
                new GenericContainer<>(STARROCKS_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(STARROCKS_HOST)
                        .withExposedPorts(STARROCKS_QUERY_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(STARROCKS_IMAGE)));
        Startables.deepStart(Stream.of(starRocksServer, MYSQL_CONTAINER)).join();
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initializeStarRocksConnection);
        shopDatabase.createAndInitialize();
        initializeTimerFlushTable();
    }

    @TestTemplate
    public void testStarRocksTimerFlush(TestContainer testContainer) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return testContainer.executeJob(
                                        "/mysqlcdc_to_starrocks_timer_flush.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job terminated before reaching RUNNING");
                                Assertions.assertEquals(
                                        "RUNNING", testContainer.getJobStatus(jobId));
                            });

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the snapshot");
                                Assertions.assertEquals(9, tableCount());
                            });

            try (Connection connection = shopDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate(
                        "INSERT INTO products (id, name, description, weight) "
                                + "VALUES (110, 'timer-flush', 'timer-flush probe', 1.0)");
            }

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the binlog event");
                                Assertions.assertEquals(10, tableCount());
                            });
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = testContainer.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }

        Container.ExecResult jobResult = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
    }

    private void initializeStarRocksConnection() throws SQLException {
        starRocksConnection =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%s",
                                starRocksServer.getHost(),
                                starRocksServer.getMappedPort(STARROCKS_QUERY_PORT)),
                        "root",
                        "");
    }

    private void initializeTimerFlushTable() {
        String createTableSql =
                "CREATE TABLE IF NOT EXISTS "
                        + SINK_DATABASE
                        + "."
                        + SINK_TABLE
                        + " ("
                        + "id INT NOT NULL, "
                        + "name VARCHAR(255), "
                        + "description VARCHAR(512), "
                        + "weight FLOAT) "
                        + "ENGINE=OLAP DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1')";
        try (Statement statement = starRocksConnection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + SINK_DATABASE);
            statement.execute(createTableSql);
            statement.execute("TRUNCATE TABLE " + SINK_DATABASE + "." + SINK_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize StarRocks timer flush table", e);
        }
    }

    private int tableCount() {
        String sql = "SELECT COUNT(*) FROM " + SINK_DATABASE + "." + SINK_TABLE;
        try (Statement statement = starRocksConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertTrue(resultSet.next());
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows in StarRocks timer flush table", e);
        }
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (starRocksConnection != null) {
            starRocksConnection.close();
        }
        if (starRocksServer != null) {
            starRocksServer.close();
        }
        MYSQL_CONTAINER.close();
    }
}
