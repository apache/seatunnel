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
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.file.local;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

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

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class LocalFileWithMetaLakeIT extends SeaTunnelContainer {

    private static final String GRAVITINO_IMAGE = "apache/gravitino:0.9.1";
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
                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
                container.execInContainer("mkdir", "-p", "/seatunnel/read/metalake/table1");
                container.execInContainer("mkdir", "-p", "/seatunnel/read/metalake/table2");

                // Copy CSV data files from resources to container
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(
                                PROJECT_ROOT_PATH
                                        + "/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-local-e2e/src/test/resources/csv/data/table1.csv"),
                        "/seatunnel/read/metalake/table1/data.csv");
                container.withCopyFileToContainer(
                        MountableFile.forHostPath(
                                PROJECT_ROOT_PATH
                                        + "/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-local-e2e/src/test/resources/csv/data/table2.csv"),
                        "/seatunnel/read/metalake/table2/data.csv");
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

        // execute extra commands
        executeExtraCommands(server);
        server.start();
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
                Arrays.asList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
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
                        .waitingFor(
                                Wait.forLogMessage(
                                        ".*Gravitino server has started successfully.*", 1))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "gravitino:" + GRAVITINO_IMAGE)));
        gravitinoContainer.start();

        log.info("Gravitino server started at {}", gravitinoContainer.getHost());

        // Wait for Gravitino to be fully ready
        Thread.sleep(10000);

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

        // Create schema in MySQL
        GenericContainer.ExecResult createSchemaResult =
                mysqlContainer.execInContainer(
                        "bash",
                        "-c",
                        "mysql -uroot -pAbc!@#135_seatunnel -e \"CREATE DATABASE IF NOT EXISTS test_schema;\"");
        log.info("Create schema result: {}", createSchemaResult.getStdout());
        Assertions.assertEquals(
                0, createSchemaResult.getExitCode(), createSchemaResult.getStderr());

        // Also create schema through Gravitino API
        createSchemaResult =
                gravitinoContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -L 'http://localhost:8090/api/metalakes/test_metalake/catalogs/test_catalog/schemas' "
                                + "-H 'Content-Type: application/json' "
                                + "-H 'Accept: application/vnd.gravitino.v1+json' "
                                + "-d '{\"name\":\"test_schema\",\"comment\":\"for metalake test\"}'");
        log.info("Create schema via Gravitino result: {}", createSchemaResult.getStdout());
        Assertions.assertEquals(
                0, createSchemaResult.getExitCode(), createSchemaResult.getStderr());

        // Create table1 with basic types in MySQL (this will be used as schema metadata)
        String createTable1Sql =
                "CREATE TABLE IF NOT EXISTS test_schema.table1 ("
                        + "c_string VARCHAR(255) DEFAULT NULL COMMENT 'string column',"
                        + "c_int INT DEFAULT NULL COMMENT 'int column',"
                        + "c_boolean TINYINT(1) DEFAULT NULL COMMENT 'boolean column',"
                        + "c_double DOUBLE DEFAULT NULL COMMENT 'double column',"
                        + "c_timestamp TIMESTAMP DEFAULT NULL COMMENT 'timestamp column'"
                        + ") COMMENT='test table1 with basic types'";
        GenericContainer.ExecResult createTable1Result =
                mysqlContainer.execInContainer(
                        "bash",
                        "-c",
                        "mysql -uroot -pAbc!@#135_seatunnel -e \"" + createTable1Sql + "\"");
        log.info("Create table1 result: {}", createTable1Result.getStdout());
        Assertions.assertEquals(
                0, createTable1Result.getExitCode(), createTable1Result.getStderr());

        // Create table2 with basic types in MySQL (using schema_url in config)
        String createTable2Sql =
                "CREATE TABLE IF NOT EXISTS test_schema.table2 ("
                        + "c_string VARCHAR(255) DEFAULT NULL COMMENT 'string column',"
                        + "c_int INT DEFAULT NULL COMMENT 'int column',"
                        + "c_boolean TINYINT(1) DEFAULT NULL COMMENT 'boolean column',"
                        + "c_double DOUBLE DEFAULT NULL COMMENT 'double column',"
                        + "c_timestamp TIMESTAMP DEFAULT NULL COMMENT 'timestamp column'"
                        + ") COMMENT='test table2 with basic types'";
        GenericContainer.ExecResult createTable2Result =
                mysqlContainer.execInContainer(
                        "bash",
                        "-c",
                        "mysql -uroot -pAbc!@#135_seatunnel -e \"" + createTable2Sql + "\"");
        log.info("Create table2 result: {}", createTable2Result.getStdout());
        Assertions.assertEquals(
                0, createTable2Result.getExitCode(), createTable2Result.getStderr());

        // Also create tables through Gravitino API for schema_url support
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
                                + "{\"name\":\"c_double\",\"type\":\"double\",\"nullable\":true,\"comment\":\"double column\"},"
                                + "{\"name\":\"c_timestamp\",\"type\":\"timestamp\",\"nullable\":true,\"comment\":\"timestamp column\"}"
                                + "]}'");
        log.info("Create Gravitino table1 result: {}", createGravitinoTable1Result.getStdout());

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
                                + "{\"name\":\"c_double\",\"type\":\"double\",\"nullable\":true,\"comment\":\"double column\"},"
                                + "{\"name\":\"c_timestamp\",\"type\":\"timestamp\",\"nullable\":true,\"comment\":\"timestamp column\"}"
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
        verifyCsvRowCount("/tmp/fake_empty/csv/db.table1", 5);

        // Verify row count for table2 (should have 10 rows from source CSV file - excluding header)
        verifyCsvRowCount("/tmp/fake_empty/csv/db.table2", 10);
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
