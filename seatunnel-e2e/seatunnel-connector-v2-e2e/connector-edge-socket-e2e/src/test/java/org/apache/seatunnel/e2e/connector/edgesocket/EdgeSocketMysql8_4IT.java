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

package org.apache.seatunnel.e2e.connector.edgesocket;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "EdgeSocket source currently supports SeaTunnel Zeta engine only.")
public class EdgeSocketMysql8_4IT extends AbstractEdgeSocketIT {
    private static final String MYSQL_VERSION = "8.4.0";
    private static final String MYSQL_IMAGE = "mysql:" + MYSQL_VERSION;
    private static final String MYSQL_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_CONTAINER_USER = "mysqluser";
    private static final String MYSQL_ADMIN_USER = "root";
    private static final String MYSQL_PASSWORD = "mysqlpw";
    private static final String SINK_TABLE = "edge_socket_sink";
    private static final String SINK_COLUMN = "value_text";
    private static final String QUERY_SINK_SQL_TEMPLATE = "SELECT %s FROM %s.%s ORDER BY %s";
    private static final String DDL_FILE_NAME = "edge_socket_mysql84_sink";

    private static final int RECORD_COUNT = 10;
    private EdgeSocketMySqlContainer mysqlContainer;
    private EdgeSocketUniqueDatabase inventoryDatabase;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl()
                                        + " --no-check-certificate");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @Override
    protected void startSinkDependencies() throws Exception {
        mysqlContainer =
                new EdgeSocketMySqlContainer(MYSQL_VERSION)
                        .withConfigurationOverride("docker/mysql/my8-4.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withUsername(MYSQL_CONTAINER_USER)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        mysqlContainer.start();

        Class.forName("com.mysql.cj.jdbc.Driver");
        inventoryDatabase =
                new EdgeSocketUniqueDatabase(
                        mysqlContainer,
                        MYSQL_DATABASE,
                        MYSQL_ADMIN_USER,
                        MYSQL_PASSWORD,
                        DDL_FILE_NAME);
        inventoryDatabase.setTemplateName(DDL_FILE_NAME).createAndInitialize();
        clearTable(inventoryDatabase.getDatabaseName(), SINK_TABLE);
    }

    @Override
    protected void stopSinkDependencies() {
        if (mysqlContainer != null) {
            mysqlContainer.close();
            mysqlContainer = null;
        }
    }

    @Override
    protected List<String> querySinkValues() throws Exception {
        List<List<Object>> rows =
                query(getSinkQuerySQL(inventoryDatabase.getDatabaseName(), SINK_TABLE));
        List<String> result = new ArrayList<>();
        for (List<Object> row : rows) {
            if (!row.isEmpty() && row.get(0) != null) {
                result.add(String.valueOf(row.get(0)));
            }
        }
        return result;
    }

    @TestTemplate
    public void testEdgeSocketSourceToMysql84(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/edge_socket_source_to_mysql84.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertJobRunningOrSubmissionFailed(container, jobId, jobFuture));
            ensureEdgeSocketForwarder();

            LinkedHashMap<String, String> schema = new LinkedHashMap<>();
            schema.put("id", "int");
            schema.put("name", "string");
            schema.put("score", "double");
            schema.put("active", "boolean");
            List<String> sourceMessages = buildSchemaPayloadJsonMessages(RECORD_COUNT, schema);
            List<String> expectedMessages = buildExpectedTransformedMessages(sourceMessages);

            sendRecordsThroughCollector(sourceMessages);
            awaitSinkContainsExpectedMessages(expectedMessages);
        } finally {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

            Container.ExecResult jobResult = waitForJobResult(jobFuture);
            Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
        }
    }

    @TestTemplate
    public void testEdgeSocketPacketModeToMysql84(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/edge_socket_source_packet_mode.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertJobRunningOrSubmissionFailed(container, jobId, jobFuture));
            ensureEdgeSocketForwarder();

            // Build plain-text messages; sendPacketRecords wraps them in PACKET envelopes
            List<String> sourceMessages = new ArrayList<>();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                sourceMessages.add("packet-value-" + i);
            }
            List<String> expectedMessages = buildExpectedTransformedMessages(sourceMessages);

            sendPacketRecords(sourceMessages);
            awaitSinkContainsExpectedMessages(expectedMessages);
        } finally {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

            Container.ExecResult jobResult = waitForJobResult(jobFuture);
            Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
        }
    }

    @TestTemplate
    public void testEdgeSocketEncryptedPacketModeToMysql84(TestContainer container)
            throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/edge_socket_source_packet_encrypted.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> assertJobRunningOrSubmissionFailed(container, jobId, jobFuture));
            ensureEdgeSocketForwarder();

            List<String> sourceMessages = new ArrayList<>();
            for (int i = 1; i <= RECORD_COUNT; i++) {
                sourceMessages.add("encrypted-value-" + i);
            }
            List<String> expectedMessages = buildExpectedTransformedMessages(sourceMessages);

            sendEncryptedPacketRecords(sourceMessages, E2E_SECRET_KEY);
            awaitSinkContainsExpectedMessages(expectedMessages);
        } finally {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

            Container.ExecResult jobResult = waitForJobResult(jobFuture);
            Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
        }
    }

    private void assertJobRunningOrSubmissionFailed(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            try {
                Container.ExecResult submitResult = jobFuture.get();
                Assertions.assertEquals(
                        0,
                        submitResult.getExitCode(),
                        "Submit job failed before reaching RUNNING: " + submitResult.getStderr());
            } catch (ExecutionException e) {
                throw new AssertionError("Submit job failed before reaching RUNNING", e.getCause());
            }
            Assertions.fail("Submit command exited before job reached RUNNING status");
        }
        Assertions.assertEquals(
                "RUNNING",
                container.getJobStatus(jobId),
                "EdgeSocket source job should be running before sending records");
    }

    private void executeSql(String sql) throws Exception {
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<List<Object>> query(String sql) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i));
                }
                result.add(row);
            }
            return result;
        }
    }

    private Connection getJdbcConnection() throws Exception {
        return inventoryDatabase.getJdbcConnection();
    }

    @SuppressWarnings("unused")
    private void initializeSinkData(List<String> seedValues) throws Exception {
        for (String seedValue : seedValues) {
            String escaped = seedValue.replace("'", "''");
            executeSql(
                    String.format(
                            "INSERT INTO `%s`.`%s`(`%s`) VALUES ('%s')",
                            MYSQL_DATABASE, SINK_TABLE, SINK_COLUMN, escaped));
        }
    }

    private void clearTable(String databaseName, String tableName) throws Exception {
        executeSql(String.format("TRUNCATE TABLE `%s`.`%s`", databaseName, tableName));
    }

    private String getSinkQuerySQL(String databaseName, String tableName) {
        return String.format(
                QUERY_SINK_SQL_TEMPLATE, SINK_COLUMN, databaseName, tableName, SINK_COLUMN);
    }
}
