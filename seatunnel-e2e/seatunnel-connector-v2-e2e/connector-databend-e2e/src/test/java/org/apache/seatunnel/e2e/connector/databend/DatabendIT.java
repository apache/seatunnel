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
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
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
import java.util.Properties;
import java.util.stream.Stream;

public class DatabendIT extends TestSuiteBase implements TestResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendIT.class);
    private static final String DATABEND_DOCKER_IMAGE = "datafuselabs/databend:v1.2.71-nightly";
    private static final String DATABEND_CONTAINER_HOST = "databend";
    private static final int PORT = 8000;
    private static final int LOCAL_PORT = 8000;
    private static final String DRIVER_CLASS = "com.databend.jdbc.Driver";
    private static final String INIT_DATABEND_PATH = "/databend/databend_init.conf";
    private static final String DATABEND_JOB_CONFIG = "/databend/databend_to_databend.conf";
    private static final String DATABASE = "default";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_SQL = "insert_sql";
    private static final Config CONFIG = getInitDatabendConfig();
    private DatabendContainer container;
    private GenericContainer<?> minioContainer;
    private Connection connection;

    @TestTemplate
    public void testDatabendSink(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // Run the test job
        Container.ExecResult execResult = container.executeJob("/databend/databend_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // Verify the sink results
        try (Connection connection = getConnection();
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
        try (Connection connection = getConnection();
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
        try (Connection connection = getConnection();
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

    @TestTemplate
    public void testDatabend(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob(DATABEND_JOB_CONFIG);
        Assertions.assertEquals(0, execResult.getExitCode());

        try (Connection conn = getConnection()) {
            assertHasDataWithConnection(conn, SINK_TABLE);
            clearTableWithConnection(conn, SINK_TABLE);
        }
    }

    private void assertHasDataWithConnection(Connection conn, String table) {
        String sql = String.format("SELECT * FROM %s.%s LIMIT 1", DATABASE, table);
        try (Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertTrue(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to assert data exists", e);
        }
    }

    @TestTemplate
    public void testSourceToConsole(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/databend/databend_to_console.conf");
        System.out.println("execResult: " + execResult.getStdout());
        System.out.println("END.......");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testFakeToDatabend(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/databend/fake_to_databend.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try (Connection conn = getConnection()) {
            clearTableWithConnection(conn, SINK_TABLE);
        }
    }

    private synchronized Connection getConnection() throws SQLException {
        if (this.connection == null || this.connection.isClosed()) {
            LOG.info("Creating new database connection");
            final Properties info = new Properties();
            info.put("user", "root");
            info.put("password", "");

            String jdbcUrl =
                    String.format(
                            "jdbc:databend://%s:%d/%s?ssl=false",
                            container.getHost(), container.getMappedPort(8000), DATABASE);

            this.connection = DriverManager.getConnection(jdbcUrl, info);
        }
        return this.connection;
    }

    private int countDataWithConnection(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            String sql = "SELECT COUNT(1) FROM " + tableName;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    return -1;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count data", e);
        }
    }

    private void clearTableWithConnection(Connection conn, String tableName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("TRUNCATE TABLE %s.%s", DATABASE, tableName));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to clear table", e);
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
                .atMost(300, java.util.concurrent.TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);

        this.forTest();
        this.initializeDatabendTables();
        this.batchInsertDataWithoutPresign();
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

    private void batchInsertDataWithoutPresign() {
        try (Statement stmt = this.connection.createStatement()) {
            String sql1 = "INSERT INTO source_table (name, age, score) VALUES ('Alice', 30, 95.5)";
            stmt.execute(sql1);
            String sql2 = "INSERT INTO source_table (name, age, score) VALUES ('Bob', 25, 85.0)";
            stmt.execute(sql2);
            String sql3 =
                    "INSERT INTO source_table (name, age, score) VALUES ('Charlie', 35, 92.5)";
            stmt.execute(sql3);

            LOG.info("Successfully inserted 3 test records");
        } catch (SQLException e) {
            LOG.error("Failed to insert test data", e);
            throw new RuntimeException("Failed to insert test data", e);
        }
    }

    private void forTest() {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement(); ) {
            ResultSet resultSet = statement.executeQuery("SELECT 1");
            if (resultSet.next()) {
                int resultSetInt = resultSet.getInt(1);
                System.out.println("###########Result: " + resultSetInt);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Test Databend server image error", e);
        }
    }

    private void initializeDatabendTables() {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement(); ) {
            statement.execute(CONFIG.getString(SOURCE_TABLE));
            statement.execute(CONFIG.getString(SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Databend tables failed!", e);
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

    private static Config getInitDatabendConfig() {
        File file = ContainerUtil.getResourcesFile(INIT_DATABEND_PATH);
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath(SOURCE_TABLE)
                && config.hasPath(SINK_TABLE)
                && config.hasPath(INSERT_SQL);
        return config;
    }

    private void executeUpdateWithConnection(Connection conn, String sql) {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Execute SQL failed: " + sql, e);
        }
    }

    private void dropTableWithConnection(Connection conn, String tableName) {
        try (Statement statement = conn.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Drop table failed!", e);
        }
    }

    private void assertHasData(String table) {
        String sql = String.format("SELECT * FROM %s.%s LIMIT 1", DATABASE, table);
        try (Connection conn = getConnection();
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            Assertions.assertTrue(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to assert data exists", e);
        }
    }

    private int countData(String tableName) {
        try (Connection conn = getConnection()) {
            return countDataWithConnection(conn, tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get connection", e);
        }
    }

    private void clearTable(String tableName) {
        try (Connection conn = getConnection()) {
            clearTableWithConnection(conn, tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get connection", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.connection != null) {
            try {
                this.connection.close();
                LOG.info("Database connection and heartbeat thread closed");

                this.connection = null;
            } catch (SQLException e) {
                LOG.error("Error closing database connection", e);
            }
        }

        Thread.sleep(5000);

        if (this.container != null) {
            this.container.stop();
            LOG.info("Container stopped");
        }
    }
}
