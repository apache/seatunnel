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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
public class DorisTimerFlushIT extends AbstractDorisIT {

    private static final String MYSQL_HOST = "mysql_doris_timer_flush_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";
    private static final String SINK_DATABASE = "timer_flush";
    private static final String SINK_TABLE = "doris_timer_flush";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(com.mysql.cj.jdbc.Driver.class)
                            .copyTo(container, MYSQL_CDC_PLUGIN_LIB);

    @BeforeAll
    public void init() {
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        inventoryDatabase.createAndInitialize();
        initializeTimerFlushTable();
    }

    @TestTemplate
    public void testDorisTimerFlush(TestContainer testContainer) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return testContainer.executeJob(
                                        "/mysqlcdc_to_doris_timer_flush.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                if (jobFuture.isDone()) {
                                    Container.ExecResult jobResult = jobFuture.get();
                                    Assertions.fail(
                                            "The streaming job terminated before reaching RUNNING: "
                                                    + jobResult.getStderr());
                                }
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
                                Assertions.assertEquals(2, tableCount());
                            });

            try (Connection connection = inventoryDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate(
                        "INSERT INTO mysql_cdc_e2e_source_table (uuid, name, score) "
                                + "VALUES (3, 'timer-flush', 100)");
            }

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the binlog event");
                                Assertions.assertEquals(3, tableCount());
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

    private void initializeTimerFlushTable() {
        String createTableSql =
                "CREATE TABLE IF NOT EXISTS "
                        + SINK_DATABASE
                        + "."
                        + SINK_TABLE
                        + " ("
                        + "uuid BIGINT NOT NULL, "
                        + "name VARCHAR(128), "
                        + "score INT) "
                        + "DUPLICATE KEY(uuid) "
                        + "DISTRIBUTED BY HASH(uuid) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1')";
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + SINK_DATABASE);
            statement.execute(createTableSql);
            statement.execute("TRUNCATE TABLE " + SINK_DATABASE + "." + SINK_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize Doris timer flush table", e);
        }
    }

    private int tableCount() {
        String sql = "SELECT COUNT(*) FROM " + SINK_DATABASE + "." + SINK_TABLE;
        try (Statement statement = jdbcConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertTrue(resultSet.next());
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count rows in Doris timer flush table", e);
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
    public void close() {
        MYSQL_CONTAINER.close();
    }
}
