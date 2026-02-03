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
 *    Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.file.local;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class LocalFileWithMetaLakeIT extends SeaTunnelContainer {

    private static final String GRAVITINO_IMAGE = "apache/gravitino:latest";
    private static final int GRAVITINO_PORT = 8090;

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;

    private GenericContainer<?> gravitinoContainer;
    private GenericContainer<?> mysqlContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                // Copy CSV data files from resources to container
                ContainerUtil.copyFileIntoContainers(
                        "/csv/data/table1.csv",
                        "/seatunnel/read/metalake/table1/data.csv",
                        container);
                ContainerUtil.copyFileIntoContainers(
                        "/csv/data/table2.csv",
                        "/seatunnel/read/metalake/table2/data.csv",
                        container);
            };

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        // Start MySQL container first as metadata storage
        startMySQLContainer();

        // Start Gravitino server with MySQL as backend
        startGravitinoServer();

        // Start SeaTunnel server with MetaLake enabled
        server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(buildStartCommand())
                        .withNetworkAliases("server")
                        .withExposedPorts()
                        .withFileSystemBind("/tmp", "/opt/hive")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));
        copySeaTunnelStarterToContainer(server);
        server.setPortBindings(Arrays.asList("5801:5801", "8080:8080"));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        server.start();

        // execute extra commands (including copying CSV files via extendedFactory)
        // This must be called after server.start() because copyFileToContainer requires a running
        // container
        executeExtraCommands(extendedFactory);
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
        if (gravitinoContainer != null) {
            gravitinoContainer.close();
        }
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
        super.tearDown();
    }

    private void startMySQLContainer() throws Exception {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);

        mysqlContainer =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .withImagePullPolicy(PullPolicy.alwaysPull())
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        mysqlContainer.setPortBindings(
                Collections.singletonList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        mysqlContainer.start();

        log.info("MySQL container started at {}", mysqlContainer.getHost());

        // Wait for MySQL to be fully ready
        Thread.sleep(10000);
    }

    private void startGravitinoServer() throws Exception {
        gravitinoContainer =
                new GenericContainer<>(GRAVITINO_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("gravitino")
                        .withExposedPorts(GRAVITINO_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "gravitino:" + GRAVITINO_IMAGE)));
        gravitinoContainer.setPortBindings(
                Collections.singletonList(String.format("%s:%s", GRAVITINO_PORT, GRAVITINO_PORT)));
        gravitinoContainer.start();

        // Wait for Gravitino to be ready by checking the API endpoint
        waitForGravitinoReady();

        log.info("Gravitino server started at {}", gravitinoContainer.getHost());

        // Create metalake and catalog using curl with MySQL as backend
        createMetalakeAndCatalog();
    }

    private void waitForGravitinoReady() throws Exception {
        int maxAttempts = 60;
        int attempt = 0;

        while (attempt < maxAttempts) {
            try {
                GenericContainer.ExecResult result =
                        gravitinoContainer.execInContainer(
                                "bash",
                                "-c",
                                "curl -s -f http://localhost:8090/api/metalakes || exit 1");
                if (result.getExitCode() == 0) {
                    log.info("Gravitino API is ready");
                    return;
                }
            } catch (Exception e) {
                log.debug("Gravitino not ready yet, attempt {}/{}", attempt + 1, maxAttempts);
            }
            attempt++;
            Thread.sleep(2000);
        }
        throw new RuntimeException(
                "Gravitino did not start within " + (maxAttempts * 2) + " seconds");
    }

    private void createMetalakeAndCatalog() throws Exception {
        // Create metalake
        GenericContainer.ExecResult createMetalakeResult =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"test_metalake\",\"comment\":\"for metalake test\",\"properties\":{}}'");
        log.info("Create metalake result: {}", createMetalakeResult.getStdout());
        Assertions.assertEquals(
                0, createMetalakeResult.getExitCode(), createMetalakeResult.getStderr());

        // Create catalog with MySQL as backend (jdbc-mysql provider)
        // This uses MySQL container as the metadata center
        GenericContainer.ExecResult createCatalogResult =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"test_catalog\",\"type\":\"relational\",\"provider\":\"jdbc-mysql\",\"comment\":\"for metalake test with MySQL backend\",\"properties\":{"
                                + "\"jdbc-driver\":\"com.mysql.cj.jdbc.Driver\","
                                + "\"jdbc-url\":\"jdbc:mysql://mysql-e2e:3306/seatunnel?useSSL=false\","
                                + "\"jdbc-user\":\"root\","
                                + "\"jdbc-password\":\"Abc!@#135_seatunnel\""
                                + "}}'");
        log.info("Create catalog result: {}", createCatalogResult.getStdout());
        Assertions.assertEquals(
                0, createCatalogResult.getExitCode(), createCatalogResult.getStderr());

        // Create schema through Gravitino API (this will also create the database in MySQL)
        GenericContainer.ExecResult createSchemaResult =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs/test_catalog/schemas' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"test_schema\"}'");
        log.info("Create schema via Gravitino result: {}", createSchemaResult.getStdout());
        Assertions.assertEquals(
                0, createSchemaResult.getExitCode(), createSchemaResult.getStderr());

        // Create table1 through Gravitino API
        GenericContainer.ExecResult createGravitinoTable1Result =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs/test_catalog/schemas/test_schema/tables' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"table1\",\"comment\":\"test table1\",\"columns\":["
                                + "{\"name\":\"c_string\",\"type\":\"string\",\"nullable\":true,\"comment\":\"string column\"},"
                                + "{\"name\":\"c_int\",\"type\":\"integer\",\"nullable\":true,\"comment\":\"int column\"},"
                                + "{\"name\":\"c_boolean\",\"type\":\"boolean\",\"nullable\":true,\"comment\":\"boolean column\"},"
                                + "{\"name\":\"c_double\",\"type\":\"double\",\"nullable\":true,\"comment\":\"double column\"}"
                                + "]}'");
        log.info("Create Gravitino table1 result: {}", createGravitinoTable1Result.getStdout());

        // Create table2 through Gravitino API
        GenericContainer.ExecResult createGravitinoTable2Result =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs/test_catalog/schemas/test_schema/tables' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"table2\",\"comment\":\"test table2\",\"columns\":["
                                + "{\"name\":\"c_string\",\"type\":\"string\",\"nullable\":true,\"comment\":\"string column\"},"
                                + "{\"name\":\"c_int\",\"type\":\"integer\",\"nullable\":true,\"comment\":\"int column\"},"
                                + "{\"name\":\"c_boolean\",\"type\":\"boolean\",\"nullable\":true,\"comment\":\"boolean column\"},"
                                + "{\"name\":\"c_double\",\"type\":\"double\",\"nullable\":true,\"comment\":\"double column\"}"
                                + "]}'");
        log.info("Create Gravitino table2 result: {}", createGravitinoTable2Result.getStdout());
    }

    @Test
    public void testLocalFileCsvToLocalFileCsvWithSchemaUrlAndFields() throws Exception {
        // Execute job with LocalFile source using fields and schema_url
        // CSV data files are copied via @TestContainerExtension
        GenericContainer.ExecResult execResult =
                executeJob("/csv/local_file_csv_to_local_file_csv_with_metalake.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify row count for table1 (should have 5 rows from source CSV file - excluding header)
        verifyCsvRowCount("/tmp/fake_empty/csv/table1", 5);

        // Verify row count for table2 (should have 10 rows from source CSV file - excluding header)
        verifyCsvRowCount("/tmp/fake_empty/csv/table2", 10);
    }

    private void verifyCsvRowCount(String path, int expectedRowCount) throws Exception {
        // Find all CSV files in the directory
        GenericContainer.ExecResult findResult =
                server.execInContainer("bash", "-c", "find " + path + " -type f -name '*.csv*'");

        if (findResult.getExitCode() != 0 || findResult.getStdout().trim().isEmpty()) {
            // Try with different pattern
            findResult = server.execInContainer("bash", "-c", "ls -1 " + path + " || true");
        }

        String[] files = findResult.getStdout().trim().split("\n");
        int totalRows = 0;

        for (String file : files) {
            if (file.trim().isEmpty()) continue;
            String filePath = file.trim();
            if (!filePath.startsWith("/")) {
                filePath = path + "/" + filePath;
            }

            // Count lines in CSV file (excluding header if present)
            GenericContainer.ExecResult countResult =
                    server.execInContainer("bash", "-c", "wc -l < " + filePath + " || echo 0");
            String countOutput = countResult.getStdout().trim();
            if (!countOutput.isEmpty()) {
                try {
                    int lineCount = Integer.parseInt(countOutput);
                    // Subtract 1 for header row
                    totalRows += Math.max(0, lineCount - 1);
                } catch (NumberFormatException e) {
                    log.warn("Failed to parse row count from: {}", countOutput);
                }
            }
        }

        log.info("Total data rows in {} (excluding headers): {}", path, totalRows);
        Assertions.assertEquals(
                expectedRowCount,
                totalRows,
                "Expected " + expectedRowCount + " rows in " + path + " but found " + totalRows);
    }
}
