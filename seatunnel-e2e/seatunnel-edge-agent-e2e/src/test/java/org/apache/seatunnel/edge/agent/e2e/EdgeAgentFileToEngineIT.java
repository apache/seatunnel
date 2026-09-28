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

package org.apache.seatunnel.edge.agent.e2e;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.jdbc.Driver;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "EdgeSocket source currently supports SeaTunnel Zeta engine only.")
public class EdgeAgentFileToEngineIT extends AbstractEdgeAgentEngineIT {

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

    private EdgeAgentMySqlContainer mysqlContainer;
    private EdgeAgentUniqueDatabase inventoryDatabase;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @TempDir Path tempDir;

    @Override
    protected void startSinkDependencies() throws Exception {
        mysqlContainer =
                new EdgeAgentMySqlContainer(MYSQL_VERSION)
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
                new EdgeAgentUniqueDatabase(
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
    public void testAgentRawFileToMysql(TestContainer container) throws Exception {
        runAgentToMysql(
                container,
                "/e2e/edge_agent_file_to_mysql84.conf",
                "e2e/agent-docker-raw.yaml",
                buildRawSchemaLogContent(),
                buildRawSchemaExpectedSubstrings());
    }

    @TestTemplate
    public void testAgentPacketNoneToMysql(TestContainer container) throws Exception {
        runPacketAgentToMysql(
                container,
                "/e2e/edge_agent_packet_to_mysql84.conf",
                "e2e/agent-docker-packet-none.yaml");
    }

    @TestTemplate
    public void testAgentPacketGzipToMysql(TestContainer container) throws Exception {
        runPacketAgentToMysql(
                container,
                "/e2e/edge_agent_packet_to_mysql84.conf",
                "e2e/agent-docker-packet-gzip.yaml");
    }

    @TestTemplate
    public void testAgentPacketZlibToMysql(TestContainer container) throws Exception {
        runPacketAgentToMysql(
                container,
                "/e2e/edge_agent_packet_to_mysql84.conf",
                "e2e/agent-docker-packet-zlib.yaml");
    }

    @TestTemplate
    public void testAgentPacketDeflateToMysql(TestContainer container) throws Exception {
        runPacketAgentToMysql(
                container,
                "/e2e/edge_agent_packet_to_mysql84.conf",
                "e2e/agent-docker-packet-deflate.yaml");
    }

    @TestTemplate
    public void testAgentPacketAesGcmToMysql(TestContainer container) throws Exception {
        runPacketAgentToMysql(
                container,
                "/e2e/edge_agent_packet_encrypted_to_mysql84.conf",
                "e2e/agent-docker-packet-aes.yaml");
    }

    private void runPacketAgentToMysql(
            TestContainer container, String jobConfPath, String agentConfigClasspath)
            throws Exception {
        runAgentToMysql(
                container,
                jobConfPath,
                agentConfigClasspath,
                buildPacketLogContent(),
                buildPacketExpectedSubstrings());
    }

    private void runAgentToMysql(
            TestContainer container,
            String jobConfPath,
            String agentConfigClasspath,
            String logContent,
            List<String> expectedSubstrings)
            throws Exception {
        clearTable(inventoryDatabase.getDatabaseName(), SINK_TABLE);

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(jobConfPath, jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertJobRunningOrSubmissionFailed(container, jobId, jobFuture));

        Path workDir = Files.createTempDirectory(tempDir, "engine-run-");
        EdgeAgentDistPaths.copyAgentConfigFromClasspath(agentConfigClasspath, workDir);
        Path logFile = workDir.resolve("agent-events.log");
        Files.write(logFile, new byte[0]);

        EdgeAgentContainer agentContainer = new EdgeAgentContainer(NETWORK, workDir);
        agentContainer.start();
        try {
            Files.write(logFile, logContent.getBytes(StandardCharsets.UTF_8));
            awaitSinkContainsExpectedMessages(expectedSubstrings);
        } catch (Throwable failure) {
            agentContainer.copyLogsTo(
                    Paths.get("target", "edge-agent-logs", String.valueOf(System.nanoTime())));
            throw failure;
        } finally {
            try {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
                Container.ExecResult jobResult = waitForJobResult(jobFuture);
                Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
            } finally {
                agentContainer.stop();
            }
        }
    }

    private String buildRawSchemaLogContent() {
        LinkedHashMap<String, String> schema = new LinkedHashMap<>();
        schema.put("id", "int");
        schema.put("name", "string");
        schema.put("score", "double");
        schema.put("active", "boolean");
        return buildSchemaPayloadJsonMessages(RECORD_COUNT, schema).stream()
                .map(line -> line + "\n")
                .collect(Collectors.joining());
    }

    private String buildPacketLogContent() {
        return buildPlainTextMessages(RECORD_COUNT, "packet-value").stream()
                .map(line -> line + "\n")
                .collect(Collectors.joining());
    }

    private List<String> buildRawSchemaExpectedSubstrings() {
        List<String> substrings = new ArrayList<>();
        for (int i = 1; i <= RECORD_COUNT; i++) {
            // LineOutputFormatter wraps each line; payload JSON is escaped in value_text.
            substrings.add("\\\"name\\\":\\\"value-" + i + "\\\"");
        }
        return substrings;
    }

    private List<String> buildPacketExpectedSubstrings() {
        return buildPlainTextMessages(RECORD_COUNT, "packet-value");
    }

    private void clearTable(String databaseName, String tableName) throws Exception {
        executeSql(String.format("TRUNCATE TABLE `%s`.`%s`", databaseName, tableName));
    }

    private void executeSql(String sql) throws Exception {
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
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
                QUERY_SINK_SQL_TEMPLATE, SINK_COLUMN, databaseName, tableName, SINK_COLUMN);
    }
}
