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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

/** Exercises Zeta checkpoint-driven retirement of independent JDBC table writers. */
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason = "Only Zeta propagates the close-table event to the sink.")
public class JdbcPostgresEarlyTableCloseIT extends TestSuiteBase implements TestResource {

    private PostgreSQLContainer<?> postgres;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.postgresql.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() {
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("pg-early-close")
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test");
        postgres.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (postgres != null) {
            postgres.close();
        }
    }

    @TestTemplate
    public void testZetaRetiresCompletedJdbcTableBeforeSiblingFinishes(TestContainer container)
            throws Exception {
        String jobId = Long.toString(System.currentTimeMillis());
        String tableAClosed =
                "Closed sink writers for table test.close_table_source.a after close table event";
        String tableBClosed =
                "Closed sink writers for table test.close_table_source.b after close table event";
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Container.ExecResult> job = null;
        try {
            executeSQL(
                    "create schema close_table_source; "
                            + "create schema close_table_sink; "
                            + "create table close_table_source.a(id int primary key); "
                            + "create table close_table_source.b(id int primary key); "
                            + "create table close_table_sink.a(id int primary key); "
                            + "create table close_table_sink.b(id int primary key); "
                            + "insert into close_table_source.a values (11), (12); "
                            + "insert into close_table_source.b select generate_series(1001,2500)");
            List<Integer> expectedA = querySql("select id from close_table_source.a order by id");
            List<Integer> expectedB = querySql("select id from close_table_source.b order by id");
            Assertions.assertEquals(2, expectedA.size());
            Assertions.assertEquals(1500, expectedB.size());
            Future<Container.ExecResult> submittedJob =
                    executor.submit(
                            () ->
                                    container.executeJob(
                                            "/jdbc_postgres_early_table_close.conf", jobId));
            job = submittedJob;
            given().pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(40, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                String logs = container.getServerLogs();
                                Assertions.assertTrue(
                                        hasTableCloseForJob(logs, jobId, tableAClosed));
                                Assertions.assertFalse(
                                        hasTableCloseForJob(logs, jobId, tableBClosed));
                                Assertions.assertFalse(
                                        submittedJob.isDone(), "Job finished before early close");
                                Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                                Assertions.assertEquals(
                                        expectedA,
                                        querySql("select id from close_table_sink.a order by id"));
                                Assertions.assertTrue(
                                        querySql("select id from close_table_sink.b order by id")
                                                        .size()
                                                < expectedB.size(),
                                        "Table B must still have work when A is retired");
                            });
            Container.ExecResult result = submittedJob.get(70, TimeUnit.SECONDS);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            Assertions.assertEquals(
                    expectedA, querySql("select id from close_table_sink.a order by id"));
            Assertions.assertEquals(
                    expectedB, querySql("select id from close_table_sink.b order by id"));
        } finally {
            try {
                if (job != null && !job.isDone()) {
                    try {
                        container.cancelJob(jobId);
                    } finally {
                        job.cancel(true);
                    }
                }
            } finally {
                executor.shutdownNow();
                executeSQL(
                        "drop schema if exists close_table_sink cascade; "
                                + "drop schema if exists close_table_source cascade");
            }
        }
    }

    private static boolean hasTableCloseForJob(String logs, String jobId, String event) {
        return logs.lines()
                .anyMatch(line -> line.contains("[" + jobId + "]") && line.contains(event));
    }

    private List<Integer> querySql(String sql) throws SQLException {
        try (Connection connection = connect();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            List<Integer> rows = new ArrayList<>();
            while (result.next()) {
                rows.add(result.getInt(1));
            }
            return rows;
        }
    }

    private void executeSQL(String sql) throws SQLException {
        try (Connection connection = connect();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }
}
