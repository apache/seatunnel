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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.databend.DatabendContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class DatabendCDCSinkIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendCDCSinkIT.class);
    private static final String DATABEND_DOCKER_IMAGE = "datafuselabs/databend:nightly";
    private static final String DATABEND_CONTAINER_HOST = "databend";
    private static final int PORT = 8000;
    private static final int LOCAL_PORT = 8000;
    private static final String DATABASE = "default";
    private static final String SINK_TABLE = "sink_table";
    private DatabendContainer container;
    private GenericContainer<?> minioContainer;
    private Connection connection;

    @TestTemplate
    public void testDatabendSinkCDC(TestContainer container) throws Exception {
        // Run the CDC test job
        Container.ExecResult execResult =
                container.executeJob("/databend/fake_to_databend_cdc.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            try (Statement stmt = connection.createStatement();
                                    ResultSet rs =
                                            stmt.executeQuery(
                                                    "SELECT COUNT(*) as count FROM sink_table")) {
                                if (rs.next()) {
                                    int count = rs.getInt("count");
                                    LOG.info(
                                            "Current record count in sink_table: {}, expecting 3",
                                            count);
                                    Assertions.assertEquals(
                                            3, count, "Expected 3 records in sink_table");
                                }
                            }
                        });

        // Verify the sink results
        try (Statement statement = connection.createStatement()) {

            // First check how many records we have
            try (ResultSet countRs =
                    statement.executeQuery("SELECT COUNT(*) as count FROM sink_table")) {
                if (countRs.next()) {
                    int count = countRs.getInt("count");
                    LOG.info("Found {} records in sink_table", count);
                }
            }

            // Then get all records for debugging
            try (ResultSet allRs = statement.executeQuery("SELECT * FROM sink_table ORDER BY id")) {
                LOG.info("All records in sink_table:");
                while (allRs.next()) {
                    LOG.info(
                            "Record: id={}, name={}, position={}, age={}, score={}",
                            allRs.getInt("id"),
                            allRs.getString("name"),
                            allRs.getString("position"),
                            allRs.getInt("age"),
                            allRs.getDouble("score"));
                }
            }

            // Finally check with expected results
            try (ResultSet resultSet =
                    statement.executeQuery("SELECT * FROM sink_table ORDER BY id")) {

                List<List<Object>> expectedRecords =
                        Arrays.asList(
                                Arrays.asList(1, "Alice", "Engineer", 30, 95.5),
                                Arrays.asList(3, "Charlie", "Engineer", 35, 92.5),
                                Arrays.asList(4, "David", "Designer", 28, 88.0));

                List<List<Object>> actualRecords = new ArrayList<>();

                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(resultSet.getInt("id"));
                    row.add(resultSet.getString("name"));
                    row.add(resultSet.getString("position"));
                    row.add(resultSet.getInt("age"));
                    row.add(resultSet.getDouble("score"));
                    actualRecords.add(row);
                }

                LOG.info("Expected records: {}", expectedRecords);
                LOG.info("Actual records: {}", actualRecords);

                Assertions.assertEquals(
                        expectedRecords.size(),
                        actualRecords.size(),
                        "Record count mismatch. Expected: "
                                + expectedRecords.size()
                                + ", Actual: "
                                + actualRecords.size());
                for (int i = 0; i < expectedRecords.size(); i++) {
                    Assertions.assertEquals(
                            expectedRecords.get(i),
                            actualRecords.get(i),
                            "Record at index " + i + " does not match");
                }
            }
        }
        clearSinkTable();
    }

    private void clearSinkTable() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE sink_table");
        }
    }

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

        LOG.info("MinIO container starting，wait 5 secs ...");
        Thread.sleep(5000);

        boolean bucketCreated = createMinIOBucketWithAWSSDK("databend");
        if (!bucketCreated) {
            LOG.warn("can't make sure MinIO bucket create success，continue to start Databend");
        }
        this.container =
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

        this.container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", LOCAL_PORT, PORT) // host 8000 map to container port 8000
                        ));

        Startables.deepStart(Stream.of(this.container)).join();
        LOG.info("Databend container started");
        Awaitility.given()
                .ignoreExceptions()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);

        this.initializeDatabendTable();
    }

    private void initializeDatabendTable() {
        try (Statement statement = connection.createStatement(); ) {
            // Create sink table
            String createTableSql =
                    "CREATE TABLE IF NOT EXISTS sink_table ("
                            + "  id INT, "
                            + "  name STRING, "
                            + "  position STRING, "
                            + "  age INT, "
                            + "  score DOUBLE"
                            + ")";
            statement.execute(createTableSql);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Databend table failed!", e);
        }
    }

    /**
     * using AWS SDK create MinIO bucket
     *
     * @param bucketName bucket
     * @return success or not
     */
    private boolean createMinIOBucketWithAWSSDK(String bucketName) {
        try {
            LOG.info("using AWS SDK to create MinIO bucket: {}", bucketName);

            AwsClientBuilder.EndpointConfiguration endpointConfig =
                    new AwsClientBuilder.EndpointConfiguration(
                            "http://localhost:9000", "us-east-1");

            AWSCredentials credentials = new BasicAWSCredentials("minioadmin", "minioadmin");
            AWSCredentialsProvider credentialsProvider =
                    new AWSStaticCredentialsProvider(credentials);

            AmazonS3 s3Client =
                    AmazonS3ClientBuilder.standard()
                            .withEndpointConfiguration(endpointConfig)
                            .withCredentials(credentialsProvider)
                            .withPathStyleAccessEnabled(true)
                            .disableChunkedEncoding()
                            .build();

            boolean bucketExists = s3Client.doesBucketExistV2(bucketName);
            if (bucketExists) {
                LOG.info("bucket {} exist，no need to create", bucketName);
                return true;
            }

            s3Client.createBucket(bucketName);
            LOG.info("create MinIO bucket success: {}", bucketName);
            return true;
        } catch (Exception e) {
            LOG.error("using AWS SDK to create MinIO failed", e);
            return false;
        }
    }

    private void initConnection()
            throws SQLException, ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        final Properties info = new Properties();
        info.put("user", "root"); // Default Databend user
        info.put("password", ""); // Default Databend password is empty
        System.out.println("maped port is: " + container.getMappedPort(8000));
        System.out.println("mapped host: is: " + container.getHost());

        String jdbcUrl =
                String.format(
                        "jdbc:databend://%s:%d/%s?ssl=false",
                        container.getHost(), container.getMappedPort(8000), DATABASE);

        this.connection = DriverManager.getConnection(jdbcUrl, info);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {

        if (this.connection != null) {
            try {
                this.connection.close();
                LOG.info("Database connection closed");

                this.connection = null;
            } catch (SQLException e) {
                LOG.error("Error closing database connection", e);
            }
        }

        if (minioContainer != null) {
            minioContainer.stop();
            LOG.info("Minio container stopped");
        }

        // Add a longer sleep to ensure all heartbeat threads are properly terminated
        Thread.sleep(10000);

        if (this.container != null) {
            this.container.stop();
            LOG.info("Container stopped");
        }

        if (this.minioContainer != null) {
            this.minioContainer.stop();
            LOG.info("MinIO container stopped");
        }
    }
}
