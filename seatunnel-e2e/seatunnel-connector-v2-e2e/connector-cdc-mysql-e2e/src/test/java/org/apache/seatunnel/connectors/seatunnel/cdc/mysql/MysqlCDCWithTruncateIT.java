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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
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

/**
 * End-to-end coverage for MySQL-CDC {@code TRUNCATE TABLE} as a table-operation event. Only Zeta
 * plus JDBC sink apply the operation; Flink and Spark must fail fast if it is enabled.
 */
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Table-operation events such as TRUNCATE TABLE are only supported on the Zeta engine.")
public class MysqlCDCWithTruncateIT extends TestSuiteBase implements TestResource {

    private static final long ASSERT_TIMEOUT_MILLIS = 180_000L;
    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_with_truncate";
    private static final String SINK_TABLE_DISABLED =
            "mysql_cdc_e2e_sink_table_with_truncate_disabled";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String INCREMENTAL_READ_MARKER =
            "Start incremental read task for incremental split";
    private static final String COUNT_SQL = "select count(*) from %s.%s";
    private static final String ENABLED_JOB = "/mysqlcdc_to_mysql_with_truncate.conf";
    private static final String DISABLED_JOB = "/mysqlcdc_to_mysql_with_truncate_disabled.conf";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

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

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @BeforeAll
    @Override
    public void startUp() {
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        shopDatabase.createAndInitialize();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @Order(1)
    @TestTemplate
    public void testTruncateIsAppliedAndRestoreKeepsNewRows(TestContainer container)
            throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        executeSql("TRUNCATE TABLE " + MYSQL_DATABASE + "." + SINK_TABLE);
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(ENABLED_JOB, jobId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        awaitSourceAndSinkCountEquals(SOURCE_TABLE, SINK_TABLE);
        waitForIncrementalRead(container, MYSQL_DATABASE + "." + SOURCE_TABLE);

        executeSql("TRUNCATE TABLE " + MYSQL_DATABASE + "." + SOURCE_TABLE);
        awaitCount(SINK_TABLE, 0);

        executeSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (id, name, description, weight) VALUES (201, 'after-truncate', 'row after truncate', 1.0)");
        awaitCount(SINK_TABLE, 1);

        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.restoreJob(ENABLED_JOB, jobId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        executeSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (id, name, description, weight) VALUES (202, 'after-restore', 'row after restore', 2.0)");
        awaitCount(SINK_TABLE, 2);
        container.cancelJob(jobId);
    }

    @Order(2)
    @TestTemplate
    public void testTruncateIsIgnoredWhenDisabled(TestContainer container) throws Exception {
        shopDatabase.setTemplateName("shop").createAndInitialize();
        executeSql("TRUNCATE TABLE " + MYSQL_DATABASE + "." + SINK_TABLE_DISABLED);
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(DISABLED_JOB, jobId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        awaitSourceAndSinkCountEquals(SOURCE_TABLE, SINK_TABLE_DISABLED);
        long snapshotSinkCount = countRows(SINK_TABLE_DISABLED);
        Assertions.assertTrue(snapshotSinkCount > 0);

        waitForIncrementalRead(container, MYSQL_DATABASE + "." + SOURCE_TABLE);
        executeSql("TRUNCATE TABLE " + MYSQL_DATABASE + "." + SOURCE_TABLE);

        await().pollDelay(15, TimeUnit.SECONDS)
                .atMost(ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        snapshotSinkCount,
                                        countRows(SINK_TABLE_DISABLED),
                                        "TRUNCATE must not change the sink when table-operations.enabled is false"));
        container.cancelJob(jobId);
    }

    private void awaitSourceAndSinkCountEquals(String sourceTable, String sinkTable) {
        await().atMost(ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        countRows(sourceTable),
                                        countRows(sinkTable),
                                        "source and sink row counts must match"));
    }

    private void awaitCount(String table, long expected) {
        await().atMost(ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, countRows(table)));
    }

    private void waitForIncrementalRead(TestContainer container, String capturedTable) {
        await().atMost(ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String serverLogs = container.getServerLogs();
                            Assertions.assertTrue(
                                    serverLogs.contains(INCREMENTAL_READ_MARKER),
                                    "Incremental reader has not started yet");
                            Assertions.assertTrue(
                                    serverLogs.contains(capturedTable),
                                    "Incremental reader has not started for " + capturedTable);
                        });
    }

    private long countRows(String table) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(String.format(COUNT_SQL, MYSQL_DATABASE, table))) {
            Assertions.assertTrue(resultSet.next());
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            log.info(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }
}
