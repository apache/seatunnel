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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

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
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.SEATUNNEL},
        disabledReason = "This test validates Flink coordinated schema evolution.")
public class MysqlCDCWithFlinkCoordinatedSchemaEvolutionIT extends TestSuiteBase
        implements TestResource {

    private static final String MYSQL_HOST = "mysql_cdc_coordinated_schema_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String SOURCE_TABLE = "users_source";
    private static final String SINK_TABLE = "users_sink";
    private static final int SOURCE_PARALLELISM = 1;
    private static final int SINK_PARALLELISM = 4;
    private static final long ASSERT_TIMEOUT_MINUTES = 5L;

    private static final MySqlContainer MYSQL_CONTAINER =
            new MySqlContainer(MySqlVersion.V8_0)
                    .withConfigurationOverride("docker/server-gtids/my.cnf")
                    .withSetupSQL("docker/setup.sql")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(MYSQL_HOST)
                    .withDatabaseName("coordinated_schema")
                    .withUsername(MYSQL_USER_NAME)
                    .withPassword(MYSQL_USER_PASSWORD)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger(
                                            "mysql-coordinated-schema-evolution")));

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @Order(1)
    @TestTemplate
    @DisabledOnContainer(
            value = {
                TestContainerId.FLINK_1_13,
                TestContainerId.FLINK_1_15,
                TestContainerId.FLINK_1_18
            },
            disabledReason = "Run the parallel schema-refresh regression once on Flink 1.20.")
    public void testParallelSinkWritersRefreshSchema(AbstractTestFlinkContainer container)
            throws Exception {
        container.replaceTaskManagers(2, 2, extendedFactory);
        awaitRegisteredTaskManagers(container, 2);

        String database = databaseName("parallel", container);
        createTables(database);
        CompletableFuture<Container.ExecResult> job =
                executeJobAsync(
                        container,
                        "/mysqlcdc_to_mysql_with_flink_coordinated_schema_change_parallel.conf",
                        variables(database, serverId(container, 0)));

        awaitRowsEqual(database, 4, job);
        addEmailColumn(database);
        insertRowsWithEmail(database, 100, 64);

        awaitRowsEqual(database, 68, job);
        Assertions.assertEquals(
                64,
                queryCount(database, SINK_TABLE, "id >= 100 AND email IS NOT NULL"),
                "All post-DDL rows must be written with the refreshed schema");
    }

    @Order(2)
    @TestTemplate
    @Disabled(
            "Depends on the CDC checkpoint-schema restore fix from apache/seatunnel#11780; "
                    + "without it Flink restores a two-field source row after ADD COLUMN")
    @DisabledOnContainer(
            value = {
                TestContainerId.FLINK_1_13,
                TestContainerId.FLINK_1_15,
                TestContainerId.FLINK_1_18
            },
            disabledReason = "Run the TaskManager failover regression once on Flink 1.20.")
    public void testSchemaRefreshAfterCheckpointAndTaskManagerRestart(
            AbstractTestFlinkContainer container) {
        String database = databaseName("recovery", container);
        createTables(database);
        CompletableFuture<Container.ExecResult> job =
                executeJobAsync(
                        container,
                        "/mysqlcdc_to_mysql_with_flink_coordinated_schema_change_failover.conf",
                        variables(database, serverId(container, 1)));

        awaitRowsEqual(database, 4, job);
        String jobId = awaitRunningJob(container, "coordinated_schema_change_failover");
        addEmailColumn(database);
        insertRowsWithEmail(database, 200, 8);
        awaitRowsEqual(database, 12, job);

        long completedBeforeSchemaCheckpoint = completedCheckpointCount(container, jobId);
        await().atMost(ASSERT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completedCheckpointCount(container, jobId)
                                                > completedBeforeSchemaCheckpoint,
                                        "A checkpoint containing the evolved schema must complete"));

        container.restartTaskManager();
        awaitTaskManagerAndJobRecovery(container, jobId, job);

        insertRowsWithEmail(database, 999, 1);
        awaitRowsEqual(database, 13, job);
        Assertions.assertEquals(
                1,
                queryCount(database, SINK_TABLE, "id = 999 AND email = 'user-999@example.com'"),
                "The recovered writer must refresh before writing the first post-recovery row");
    }

    private CompletableFuture<Container.ExecResult> executeJobAsync(
            AbstractTestFlinkContainer container, String config, List<String> variables) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(config, variables);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private List<String> variables(String database, int serverId) {
        return Arrays.asList(
                "cse_database=" + database,
                "cse_source_table=" + SOURCE_TABLE,
                "cse_sink_table=" + SINK_TABLE,
                "cse_source_parallelism=" + SOURCE_PARALLELISM,
                "cse_sink_parallelism=" + SINK_PARALLELISM,
                "cse_server_id=" + serverId);
    }

    private int serverId(AbstractTestFlinkContainer container, int testOffset) {
        return 7000 + container.identifier().ordinal() * 10 + testOffset;
    }

    private String databaseName(String scenario, AbstractTestFlinkContainer container) {
        return ("coord_" + scenario + "_" + container.identifier().name()).toLowerCase(Locale.ROOT);
    }

    private void createTables(String database) {
        executeSql("CREATE DATABASE IF NOT EXISTS `" + database + "`");
        executeSql("DROP TABLE IF EXISTS `" + database + "`.`" + SOURCE_TABLE + "`");
        executeSql("DROP TABLE IF EXISTS `" + database + "`.`" + SINK_TABLE + "`");
        executeSql(
                "CREATE TABLE `"
                        + database
                        + "`.`"
                        + SOURCE_TABLE
                        + "` (id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        executeSql(
                "CREATE TABLE `"
                        + database
                        + "`.`"
                        + SINK_TABLE
                        + "` (id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        executeSql(
                "INSERT INTO `"
                        + database
                        + "`.`"
                        + SOURCE_TABLE
                        + "` VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
    }

    private void addEmailColumn(String database) {
        executeSql(
                "ALTER TABLE `"
                        + database
                        + "`.`"
                        + SOURCE_TABLE
                        + "` ADD COLUMN email VARCHAR(255) NULL");
    }

    private void insertRowsWithEmail(String database, int firstId, int count) {
        String sql =
                "INSERT INTO `"
                        + database
                        + "`.`"
                        + SOURCE_TABLE
                        + "` (id, name, email) VALUES (?, ?, ?)";
        try (Connection connection = getJdbcConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int id = firstId; id < firstId + count; id++) {
                statement.setInt(1, id);
                statement.setString(2, "user-" + id);
                statement.setString(3, "user-" + id + "@example.com");
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to insert post-schema-change rows", e);
        }
    }

    private void awaitRowsEqual(
            String database, int expectedCount, CompletableFuture<Container.ExecResult> job) {
        try {
            await().atMost(ASSERT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                assertJobActive(job);
                                Assertions.assertEquals(
                                        expectedCount, queryCount(database, SINK_TABLE, null));
                                Assertions.assertIterableEquals(
                                        queryRows(database, SOURCE_TABLE),
                                        queryRows(database, SINK_TABLE));
                            });
        } catch (ConditionTimeoutException e) {
            assertJobActive(job);
            throw e;
        }
    }

    private void assertJobActive(CompletableFuture<Container.ExecResult> job) {
        if (!job.isDone()) {
            return;
        }
        Container.ExecResult result = job.join();
        Assertions.fail(
                "The streaming job stopped unexpectedly. Exit code: "
                        + result.getExitCode()
                        + ", stderr: "
                        + result.getStderr());
    }

    private String awaitRunningJob(AbstractTestFlinkContainer container, String nameFragment) {
        final String[] jobId = new String[1];
        await().atMost(ASSERT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            JsonNode jobs = readFlinkJson(container, "/jobs/overview").get("jobs");
                            for (JsonNode job : jobs) {
                                if (job.get("name").asText().contains(nameFragment)
                                        && "RUNNING".equals(job.get("state").asText())) {
                                    jobId[0] = job.get("jid").asText();
                                    return;
                                }
                            }
                            Assertions.fail("The expected Flink job is not running yet");
                        });
        return jobId[0];
    }

    private long completedCheckpointCount(AbstractTestFlinkContainer container, String jobId) {
        return readFlinkJson(container, "/jobs/" + jobId + "/checkpoints")
                .get("counts")
                .get("completed")
                .asLong();
    }

    private void awaitTaskManagerAndJobRecovery(
            AbstractTestFlinkContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> job) {
        await().atMost(ASSERT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertJobActive(job);
                            Assertions.assertTrue(
                                    readFlinkJson(container, "/taskmanagers")
                                                    .get("taskmanagers")
                                                    .size()
                                            > 0,
                                    "The restarted TaskManager must register again");
                            Assertions.assertEquals(
                                    "RUNNING",
                                    readFlinkJson(container, "/jobs/" + jobId)
                                            .get("state")
                                            .asText());
                        });
    }

    private void awaitRegisteredTaskManagers(
            AbstractTestFlinkContainer container, int expectedTaskManagers) {
        await().atMost(ASSERT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedTaskManagers,
                                        readFlinkJson(container, "/taskmanagers")
                                                .get("taskmanagers")
                                                .size(),
                                        "The regression must run across multiple TaskManager JVMs"));
    }

    private ObjectNode readFlinkJson(AbstractTestFlinkContainer container, String path) {
        String endpoint =
                String.format(
                        "http://%s:%s%s",
                        container.getJobManagerHost(), container.getJobManagerRestPort(), path);
        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(new HttpGet(endpoint))) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IllegalStateException(
                        "Flink REST request failed: " + response.getStatusLine());
            }
            return JsonUtils.parseObject(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to query Flink REST endpoint " + endpoint, e);
        }
    }

    private int queryCount(String database, String table, String condition) {
        String sql = "SELECT COUNT(*) FROM `" + database + "`.`" + table + "`";
        if (condition != null) {
            sql += " WHERE " + condition;
        }
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows with query " + sql, e);
        }
    }

    private List<List<Object>> queryRows(String database, String table) {
        boolean emailExists = columnExists(database, table, "email");
        String sql =
                "SELECT id, name"
                        + (emailExists ? ", email" : "")
                        + " FROM `"
                        + database
                        + "`.`"
                        + table
                        + "` ORDER BY id";
        List<List<Object>> rows = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                rows.add(
                        emailExists
                                ? Arrays.asList(
                                        resultSet.getInt("id"),
                                        resultSet.getString("name"),
                                        resultSet.getString("email"))
                                : Arrays.asList(
                                        resultSet.getInt("id"), resultSet.getString("name")));
            }
            return rows;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to query rows with " + sql, e);
        }
    }

    private boolean columnExists(String database, String table, String column) {
        String sql =
                "SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_schema = ? AND table_name = ? AND column_name = ?";
        try (Connection connection = getJdbcConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, database);
            statement.setString(2, table);
            statement.setString(3, column);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1) == 1;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect table schema", e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute SQL: " + sql, e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    @BeforeAll
    @Override
    public void startUp() {
        Startables.deepStart(java.util.stream.Stream.of(MYSQL_CONTAINER)).join();
    }

    @AfterAll
    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
    }
}
