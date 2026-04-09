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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcSinkBatchIntervalIT extends TestSuiteBase implements TestResource {

    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @TestTemplate
    public void testBatchIntervalFlush(TestContainer container) throws SQLException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/jdbc_postgres_sink_batch_interval.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when timer flush is detected - "
                                            + "if the job already finished, the flush may have come from close()");
                            int rowCount = getSinkRowCount("sink_batch_interval_timer");
                            log.info(
                                    "Polling sink_batch_interval_timer during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "Timer flush should have written rows to the database "
                                            + "BEFORE job completion (batch_size=100000 is never reached)");
                        });

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_interval_timer");
    }

    @TestTemplate
    public void testBatchSizeFlush(TestContainer container) throws SQLException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob("/jdbc_postgres_sink_batch_size.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when batch_size flush is detected");
                            int rowCount = getSinkRowCount("sink_batch_size_only");
                            log.info(
                                    "Polling sink_batch_size_only during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "batch_size flush should have written rows to the database "
                                            + "BEFORE job completion");
                            Assertions.assertEquals(
                                    0,
                                    rowCount % 5,
                                    "Row count should be a multiple of batch_size(5), "
                                            + "actual: "
                                            + rowCount);
                        });

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_size_only");
    }

    @TestTemplate
    public void testBatchIntervalWithBatchSize1(TestContainer container)
            throws SQLException, InterruptedException {
        AtomicBoolean jobFinished = new AtomicBoolean(false);

        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/jdbc_postgres_sink_batch_interval_with_batch_size_1.conf");
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                jobFinished.set(true);
                            }
                        });

        given().ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    jobFinished.get(),
                                    "Job should still be running when batch_size=1 flush is detected");
                            int rowCount = getSinkRowCount("sink_batch_interval_bs1");
                            log.info(
                                    "Polling sink_batch_interval_bs1 during job execution: {} rows, jobFinished={}",
                                    rowCount,
                                    jobFinished.get());
                            Assertions.assertTrue(
                                    rowCount > 0,
                                    "batch_size=1 should flush each row immediately "
                                            + "BEFORE job completion");
                        });

        int firstCount = getSinkRowCount("sink_batch_interval_bs1");
        Thread.sleep(5000);
        Assertions.assertFalse(jobFinished.get(), "Job should still be running for second poll");
        int secondCount = getSinkRowCount("sink_batch_interval_bs1");
        log.info(
                "batch_size=1 incremental check: firstCount={}, secondCount={}",
                firstCount,
                secondCount);
        Assertions.assertTrue(
                secondCount > firstCount,
                "Row count should keep growing with batch_size=1 (per-row flush), "
                        + "firstCount="
                        + firstCount
                        + ", secondCount="
                        + secondCount);

        Container.ExecResult result = jobFuture.join();
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        truncateTable("sink_batch_interval_bs1");
    }

    private int getSinkRowCount(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("SELECT count(*) FROM " + tableName)) {
                Assertions.assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }
    }

    private void truncateTable(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE TABLE " + tableName);
            }
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
    }

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS %s(\n"
                    + "pk_id BIGINT NOT NULL PRIMARY KEY,\n"
                    + "name VARCHAR(255),\n"
                    + "score INT\n"
                    + ")";

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_timer"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_size_only"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_bs1"));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSQL table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
