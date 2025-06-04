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

package org.apache.seatunnel.e2e.connector.databend;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class TestDatabendCase extends TestSuiteBase implements TestResource {
    private DatabendContainer databendContainer;
    private GenericContainer minioContainer;
    private static final String DATABEND_CONTAINER_HOST = "databend";
    private static final String DATABEND_DOCKER_IMAGE = "datafuselabs/databend:v1.2.71-nightly";
    private static final int PORT = 8000;
    private static final int LOCAL_PORT = 8000;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.minioContainer =
                new GenericContainer<>("minio/minio:latest")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("minio")
                        .withEnv("MINIO_ROOT_USER", "minioadmin")
                        .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
                        .withCommand("server", "/data")
                        .withExposedPorts(9000);

        this.minioContainer.setWaitStrategy(
                Wait.defaultWaitStrategy().withStartupTimeout(Duration.ofSeconds(60)));

        this.minioContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", 9000, 9000)));

        this.minioContainer.start();

        log.info("MinIO container starting，wait 5 secs ...");
        Thread.sleep(5000);

        boolean bucketCreated = DatabendTestUtils.createMinIOBucketWithAWSSDK("databend");
        if (!bucketCreated) {
            log.warn("can't make sure MinIO bucket create success，continue to start Databend");
        }
        this.databendContainer =
                new DatabendContainer(DATABEND_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DATABEND_CONTAINER_HOST)
                        .withUsername("root")
                        .withPassword("")
                        .withEnv("STORAGE_TYPE", "s3")
                        .withEnv("STORAGE_S3_ENDPOINT_URL", "http://minio:9000")
                        .withEnv("STORAGE_S3_ACCESS_KEY_ID", "minioadmin")
                        .withEnv("STORAGE_S3_SECRET_ACCESS_KEY", "minioadmin")
                        .withEnv("STORAGE_S3_BUCKET", "databend")
                        .withEnv("STORAGE_S3_REGION", "us-east-1")
                        .withEnv("STORAGE_S3_ENABLE_VIRTUAL_HOST_STYLE", "false")
                        .withEnv("STORAGE_S3_FORCE_PATH_STYLE", "true")
                        .withUrlParam("ssl", "false");

        this.databendContainer.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", LOCAL_PORT, PORT) // host 8000 map to container port 8000
                        ));

        Startables.deepStart(Stream.of(this.databendContainer)).join();
        log.info("Databend container started");

        log.info("Databend container started");

        // Create test table
        String createSourceTable =
                "CREATE TABLE IF NOT EXISTS source_table " + "(name STRING, age INT, score DOUBLE)";

        String createSinkTable =
                "CREATE TABLE IF NOT EXISTS sink_table " + "(name STRING, age INT, score DOUBLE)";

        String createSchemaEvolutionTable =
                "CREATE TABLE IF NOT EXISTS schema_evolution_table "
                        + "(id INT, name STRING, score DOUBLE)";

        try (Connection connection =
                        DriverManager.getConnection(
                                databendContainer.getJdbcUrl(),
                                databendContainer.getUsername(),
                                databendContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(createSourceTable);
            statement.execute(createSinkTable);
            statement.execute(createSchemaEvolutionTable);
            // Insert test data
            String insertSourceData =
                    "INSERT INTO source_table VALUES "
                            + "('Alice', 30, 95.5), "
                            + "('Bob', 25, 85.0), "
                            + "('Charlie', 35, 92.5)";
            statement.execute(insertSourceData);
        } catch (SQLException e) {
            log.error("Error creating test tables", e);
            throw new RuntimeException("Failed to create test tables", e);
        }
        log.info("Test tables and data initialized");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (databendContainer != null) {
            databendContainer.close();
        }
    }

    @TestTemplate
    public void testDatabendSink(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // Run the test job
        Container.ExecResult execResult = container.executeJob("/databend/databend_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify the sink results
        try (Connection connection =
                        DriverManager.getConnection(
                                databendContainer.getJdbcUrl(),
                                databendContainer.getUsername(),
                                databendContainer.getPassword());
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT * FROM sink_table ORDER BY name")) {

            List<List<Object>> expectedRecords =
                    Arrays.asList(
                            Arrays.asList("Alice", 30, 95.5),
                            Arrays.asList("Bob", 25, 85.0),
                            Arrays.asList("Charlie", 35, 92.5));

            List<List<Object>> actualRecords = new ArrayList<>();

            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                row.add(resultSet.getString("name"));
                row.add(resultSet.getInt("age"));
                row.add(resultSet.getDouble("score"));
                actualRecords.add(row);
            }

            Assertions.assertEquals(expectedRecords.size(), actualRecords.size());
            for (int i = 0; i < expectedRecords.size(); i++) {
                Assertions.assertEquals(expectedRecords.get(i), actualRecords.get(i));
            }
        }
        clearSinkTable();
    }

    private void clearSinkTable() throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                databendContainer.getJdbcUrl(),
                                databendContainer.getUsername(),
                                databendContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE sink_table");
        }
    }

    @TestTemplate
    public void testDatabendSource(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/databend/databend_source.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testSchemaEvolution(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // Run the schema evolution test job
        Container.ExecResult execResult =
                container.executeJob("/databend/databend_schema_evolution.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify the schema was evolved correctly
        try (Connection connection =
                        DriverManager.getConnection(
                                databendContainer.getJdbcUrl(),
                                databendContainer.getUsername(),
                                databendContainer.getPassword());
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("DESC schema_evolution_table")) {

            List<String> columnNames = new ArrayList<>();
            while (resultSet.next()) {
                columnNames.add(resultSet.getString("field"));
            }

            // Verify the new column exists
            Assertions.assertTrue(
                    columnNames.contains("email"),
                    "Table should have 'email' column after schema evolution");
        }
    }
}
