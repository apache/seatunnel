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
import java.util.concurrent.TimeUnit;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "EdgeSocket source currently supports SeaTunnel Zeta engine only.")
public class EdgeSocketCheckpointRestoreIT extends AbstractEdgeSocketIT {
    private static final String MYSQL_VERSION = "8.4.0";
    private static final String MYSQL_IMAGE = "mysql:" + MYSQL_VERSION;
    private static final String MYSQL_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_CONTAINER_USER = "mysqluser";
    private static final String MYSQL_ADMIN_USER = "root";
    private static final String MYSQL_PASSWORD = "mysqlpw";
    private static final String SINK_TABLE = "edge_socket_sink";
    private static final String SINK_COLUMN = "value_text";
    private static final String DDL_FILE_NAME = "edge_socket_mysql84_sink";

    private static final int FIRST_BATCH_COUNT = 5;
    private static final int SECOND_BATCH_COUNT = 5;

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
    public void testEncryptedPacketCheckpointRestore(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        boolean jobCancelled = false;
        CompletableFuture<Container.ExecResult> restoredJobFuture = null;

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
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING",
                                            container.getJobStatus(jobId),
                                            "Job must reach RUNNING before sending first batch"));
            ensureEdgeSocketForwarder();

            List<String> firstBatch = new ArrayList<>();
            for (int i = 1; i <= FIRST_BATCH_COUNT; i++) {
                firstBatch.add("encrypted-value-" + i);
            }
            List<String> firstExpected = buildExpectedTransformedMessages(firstBatch);
            sendEncryptedPacketRecords(firstBatch, E2E_SECRET_KEY);
            awaitSinkContainsExpectedMessages(firstExpected);

            Container.ExecResult savepointResult = container.savepointJob(jobId);
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());

            Container.ExecResult firstJobResult = waitForJobResult(jobFuture);
            Assertions.assertEquals(0, firstJobResult.getExitCode(), firstJobResult.getStderr());

            restoredJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.restoreJob(
                                            "/edge_socket_source_packet_encrypted.conf", jobId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING",
                                            container.getJobStatus(jobId),
                                            "Restored job must reach RUNNING"));

            List<String> secondBatch = new ArrayList<>();
            for (int i = 1; i <= SECOND_BATCH_COUNT; i++) {
                secondBatch.add("encrypted-restored-" + i);
            }
            List<String> secondExpected = buildExpectedTransformedMessages(secondBatch);
            sendEncryptedPacketRecords(secondBatch, E2E_SECRET_KEY);

            List<String> allExpected = new ArrayList<>(firstExpected);
            allExpected.addAll(secondExpected);
            awaitSinkContainsExpectedMessages(allExpected);

            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            Container.ExecResult restoredJobResult = waitForJobResult(restoredJobFuture);
            Assertions.assertEquals(
                    0, restoredJobResult.getExitCode(), restoredJobResult.getStderr());
            jobCancelled = true;
        } finally {
            if (!jobCancelled) {
                container.cancelJob(jobId);
                if (!jobFuture.isDone()) {
                    waitForJobResult(jobFuture);
                }
                if (restoredJobFuture != null && !restoredJobFuture.isDone()) {
                    waitForJobResult(restoredJobFuture);
                }
            }
        }
    }

    @TestTemplate
    public void testCheckpointRestoreNoDataLoss(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        boolean jobCancelled = false;
        CompletableFuture<Container.ExecResult> restoredJobFuture = null;

        // Start the streaming job asynchronously
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/edge_socket_source_checkpoint_restore.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            // Wait for job to reach RUNNING state
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING",
                                            container.getJobStatus(jobId),
                                            "Job must reach RUNNING before sending first batch"));
            ensureEdgeSocketForwarder();

            // Send first batch and wait for checkpoint ACK
            LinkedHashMap<String, String> schema = new LinkedHashMap<>();
            schema.put("id", "int");
            schema.put("name", "string");
            schema.put("score", "double");
            schema.put("active", "boolean");
            List<String> firstBatch = buildSchemaPayloadJsonMessages(FIRST_BATCH_COUNT, schema);
            List<String> firstExpected = buildExpectedTransformedMessages(firstBatch);
            sendRecordsThroughCollector(firstBatch);
            awaitSinkContainsExpectedMessages(firstExpected);

            // Savepoint the job
            Container.ExecResult savepointResult = container.savepointJob(jobId);
            Assertions.assertEquals(0, savepointResult.getExitCode(), savepointResult.getStderr());

            // Wait for the original job to finish after savepoint
            Container.ExecResult firstJobResult = waitForJobResult(jobFuture);
            Assertions.assertEquals(0, firstJobResult.getExitCode(), firstJobResult.getStderr());

            // Restore the job from savepoint using the original jobId (CLI: -r <jobId>)
            restoredJobFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return container.restoreJob(
                                            "/edge_socket_source_checkpoint_restore.conf", jobId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING",
                                            container.getJobStatus(jobId),
                                            "Restored job must reach RUNNING before sending second batch"));

            List<String> secondBatch = buildSchemaPayloadJsonMessages(SECOND_BATCH_COUNT, schema);
            List<String> secondExpected = buildExpectedTransformedMessages(secondBatch);
            sendRecordsThroughCollector(secondBatch);

            List<String> allExpected = new ArrayList<>(firstExpected);
            allExpected.addAll(secondExpected);
            awaitSinkContainsExpectedMessages(allExpected);

            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            Container.ExecResult restoredJobResult = waitForJobResult(restoredJobFuture);
            Assertions.assertEquals(
                    0, restoredJobResult.getExitCode(), restoredJobResult.getStderr());
            jobCancelled = true;
        } finally {
            if (!jobCancelled) {
                container.cancelJob(jobId);
                if (!jobFuture.isDone()) {
                    waitForJobResult(jobFuture);
                }
                if (restoredJobFuture != null && !restoredJobFuture.isDone()) {
                    waitForJobResult(restoredJobFuture);
                }
            }
        }
    }

    private void clearTable(String databaseName, String tableName) throws Exception {
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE `%s`.`%s`", databaseName, tableName));
        }
    }

    private List<List<Object>> query(String sql) throws Exception {
        try (Connection connection = inventoryDatabase.getJdbcConnection();
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

    private String getSinkQuerySQL(String databaseName, String tableName) {
        return String.format(
                "SELECT %s FROM %s.%s ORDER BY %s",
                SINK_COLUMN, databaseName, tableName, SINK_COLUMN);
    }
}
