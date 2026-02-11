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
        // Close containers in reverse order of creation
        if (server != null) {
            server.close();
        }
        if (gravitinoContainer != null) {
            gravitinoContainer.close();
        }
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
        // Note: Not calling super.tearDown() because:
        // 1. This test overrides startUp() and doesn't use CONTAINER_VOLUME_MOUNT_PATH
        // 2. Parent's tearDown tries to execInContainer on server which fails if already closed
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
        log.info("Gravitino server started at {}", gravitinoContainer.getHost());
        // Create metalake and catalog using curl with MySQL as backend
        createMetalakeAndCatalog();
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
        log.info("Verifying row count for path: {}, expected: {}", path, expectedRowCount);
        // Check if path exists
        GenericContainer.ExecResult checkResult =
                server.execInContainer(
                        "bash", "-c", "test -e " + path + " && echo 'exists' || echo 'not exists'");
        log.info("Path check result: {}", checkResult.getStdout().trim());
        if (checkResult.getStdout().trim().equals("not exists")) {
            log.warn("Path {} does not exist, skipping verification", path);
            return;
        }
        // Check if path is a file or directory
        GenericContainer.ExecResult typeResult =
                server.execInContainer(
                        "bash", "-c", "test -f " + path + " && echo 'file' || echo 'dir'");
        String pathType = typeResult.getStdout().trim();
        log.info("Path type: {}", pathType);
        int totalRows = 0;
        if ("file".equals(pathType)) {
            // Path is a file, count rows directly
            totalRows = countCsvRows(path);
        } else {
            // Path is a directory, list all files and count
            GenericContainer.ExecResult listResult =
                    server.execInContainer("bash", "-c", "ls -1 " + path + " 2>/dev/null || true");
            String[] files = listResult.getStdout().trim().split("\n");
            log.info("Found {} files in directory {}", files.length, path);
            for (String file : files) {
                if (file.trim().isEmpty()) continue;
                String filePath = path + "/" + file.trim();
                log.info("Processing file: {}", filePath);
                totalRows += countCsvRows(filePath);
            }
        }
        log.info("Total data rows in {} (excluding headers): {}", path, totalRows);
        Assertions.assertEquals(
                expectedRowCount,
                totalRows,
                "Expected " + expectedRowCount + " rows in " + path + " but found " + totalRows);
    }

    private int countCsvRows(String filePath) throws Exception {
        // Use wc -l to count lines (counts newline characters)
        GenericContainer.ExecResult wcResult =
                server.execInContainer(
                        "bash", "-c", "wc -l < " + filePath + " 2>/dev/null || echo 0");
        String wcOutput = wcResult.getStdout().trim();
        int lineCount = 0;
        try {
            lineCount = Integer.parseInt(wcOutput);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse wc output: {}", wcOutput);
        }

        // Check if file has content (wc -l might be 0 if last line has no newline)
        GenericContainer.ExecResult sizeResult =
                server.execInContainer(
                        "bash", "-c", "stat -c%s " + filePath + " 2>/dev/null || echo 0");
        int fileSize = Integer.parseInt(sizeResult.getStdout().trim());
        // If file has content but wc -l is 0, or if we need to check for last line without newline
        if (fileSize > 0 && lineCount == 0) {
            // File has content but no newlines, count as 1 line
            lineCount = 1;
        } else if (fileSize > 0) {
            // Check if last character is newline, if not add 1 to count
            GenericContainer.ExecResult lastCharResult =
                    server.execInContainer(
                            "bash", "-c", "tail -c 1 " + filePath + " | od -An -tx1 | head -1");
            String lastChar = lastCharResult.getStdout().trim();
            // If last character is not 0a (newline in hex), add 1
            if (!lastChar.equals("0a")) {
                lineCount++;
            }
        }
        // Read first line to check for header
        GenericContainer.ExecResult firstLineResult =
                server.execInContainer("bash", "-c", "head -1 " + filePath);
        String firstLine = firstLineResult.getStdout().trim().toLowerCase();
        // Check if first line is a header (contains column names)
        boolean hasHeader =
                firstLine.contains("c_string")
                        || firstLine.contains("c_int")
                        || firstLine.contains("c_boolean")
                        || firstLine.contains("c_double");
        int dataRows = hasHeader ? Math.max(0, lineCount - 1) : lineCount;
        log.info(
                "File: {}, Total lines: {}, Has header: {}, Data rows: {}",
                filePath,
                lineCount,
                hasHeader,
                dataRows);
        return dataRows;
    }
}
