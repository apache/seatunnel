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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import io.restassured.response.Response;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.JSON;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.copyConnectorJarToContainer;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_JOB_INFO;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_METADATA_DATASOURCE;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_METADATA_DATASOURCES;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_SUBMIT_JOB;

/**
 * E2E tests for DynamicMetadataProvider. Tests REST API CRUD operations, MySQL integration, and
 * JSON job submission.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamicMetadataProviderIT extends SeaTunnelContainer {

    protected GenericContainer<?> dbServer;
    protected Connection connection;
    private String baseUrl;

    // Test constants
    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        // Setup SeaTunnel container with dynamic metadata enabled
        server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(buildStartCommand())
                        .withNetworkAliases("server")
                        .withExposedPorts(5801, 8080)
                        .withFileSystemBind("/tmp", "/opt/hive")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));

        copySeaTunnelStarterToContainer(server);
        server.setPortBindings(Arrays.asList("5801:5801", "8080:8080"));

        // Copy base resources
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        // Copy dynamic metadata enabled seatunnel.yaml
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-7/src/test/resources/config/seatunnel_dynamic.yaml"),
                Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString());

        // Copy hadoop uber jar
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        executeExtraCommands(server);
        server.start();
        baseUrl = "http://" + server.getHost() + ":" + server.getMappedPort(8080);

        copyJdbcConnectorJarToContainer();

        // Download MySQL driver
        server.execInContainer(
                "bash",
                "-c",
                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && "
                        + "cd /tmp/seatunnel/plugins/Jdbc/lib && "
                        + "wget "
                        + driverUrl()
                        + " --no-check-certificate");

        // Setup MySQL container
        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());
        Startables.deepStart(Stream.of(dbServer)).join();

        // Wait for MySQL and initialize JDBC connection
        org.awaitility.Awaitility.await()
                .ignoreExceptions()
                .atMost(360, java.util.concurrent.TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                this.initializeJdbcConnection(
                                        "jdbc:mysql://localhost:"
                                                + dbServer.getMappedPort(MYSQL_PORT)
                                                + "/"
                                                + MYSQL_DATABASE));
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        // Cleanup any remaining datasources
        try {
            Response listResponse =
                    given().get(baseUrl + REST_URL_METADATA_DATASOURCES).thenReturn();
            List<Map<String, Object>> datasources = listResponse.jsonPath().getList("$");
            for (Map<String, Object> ds : datasources) {
                String id = (String) ds.get("metadataDatasourceId");
                if (id != null) {
                    given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + id);
                }
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        if (connection != null) {
            connection.close();
        }
        if (dbServer != null) {
            dbServer.close();
        }
        super.tearDown();
    }

    private void copyJdbcConnectorJarToContainer() {
        copyConnectorJarToContainer(
                server,
                "/jdbc_mysql_source_to_sink_with_dynamic_metadata.conf",
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
    }

    protected GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);
        MySQLContainer<?> container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        return container;
    }

    protected void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException,
                    ClassNotFoundException {
        Properties props = new Properties();
        props.put("user", MYSQL_USERNAME);
        props.put("password", MYSQL_PASSWORD);
        Driver driver = (Driver) Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        this.connection = driver.connect(jdbcUrl, props);
        connection.setAutoCommit(false);
    }

    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    private void createTestDatasource(String id, String url) {
        String body =
                String.format(
                        "{\"metadataDatasourceId\": \"%s\", \"connectorType\": \"Jdbc\", "
                                + "\"properties\": {\"url\": \"%s\", \"driver\": \"com.mysql.cj.jdbc.Driver\", "
                                + "\"user\": \"root\", \"password\": \"Abc!@#135_seatunnel\"}}",
                        id, url);
        given().body(body)
                .contentType(JSON)
                .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                .then()
                .statusCode(200);
    }

    private void createSourceTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS source");
            stmt.execute("DROP TABLE IF EXISTS sink");
            stmt.execute("CREATE TABLE source (id INT PRIMARY KEY, name VARCHAR(100), age INT)");
            stmt.execute("CREATE TABLE sink (id INT PRIMARY KEY, name VARCHAR(100), age INT)");
            connection.commit();
        }
    }

    private void insertTestData(int rowCount) throws SQLException {
        try (PreparedStatement pstmt =
                connection.prepareStatement(
                        "INSERT INTO source (id, name, age) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= rowCount; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "name_" + i);
                pstmt.setInt(3, 20 + i % 40);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            connection.commit();
        }
    }

    private void verifySinkTableData(int expectedCount) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sink")) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals(expectedCount, rs.getInt(1));
        }
    }

    private String getJobStatus(long jobId) {
        return given().get(baseUrl + REST_URL_JOB_INFO + "/" + jobId)
                .jsonPath()
                .getString("jobStatus");
    }

    private void waitForJobFinished(long jobId, int timeoutSeconds) {
        org.awaitility.Awaitility.await()
                .atMost(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)
                .until(
                        () -> {
                            String status = getJobStatus(jobId);
                            return "FINISHED".equals(status) || "FAILED".equals(status);
                        });
    }

    // ==================== REST API CRUD Tests ====================

    @Test
    void testCreateDatasource() throws Exception {
        String requestBody =
                "{"
                        + "\"metadataDatasourceId\": \"test_create_ds\","
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://mysql-e2e:3306/seatunnel\","
                        + "  \"driver\": \"com.mysql.cj.jdbc.Driver\","
                        + "  \"user\": \"root\","
                        + "  \"password\": \"Abc!@#135_seatunnel\""
                        + "}"
                        + "}";

        Response response =
                given().body(requestBody)
                        .contentType(JSON)
                        .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                        .then()
                        .statusCode(200)
                        .extract()
                        .response();

        Assertions.assertEquals("success", response.jsonPath().getString("status"));
        Assertions.assertEquals(
                "test_create_ds", response.jsonPath().getString("metadataDatasourceId"));

        // Verify datasource exists
        given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_create_ds")
                .then()
                .statusCode(200);
    }

    @Test
    void testCreateDuplicateDatasource() throws Exception {
        String requestBody =
                "{"
                        + "\"metadataDatasourceId\": \"test_duplicate_ds\","
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://mysql-e2e:3306/seatunnel\""
                        + "}"
                        + "}";

        // Create first datasource
        given().body(requestBody)
                .contentType(JSON)
                .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                .then()
                .statusCode(200);

        // Try to create duplicate - should fail
        Response response =
                given().body(requestBody)
                        .contentType(JSON)
                        .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response.jsonPath().getString("status"));
        Assertions.assertTrue(response.jsonPath().getString("message").contains("already exists"));
    }

    @Test
    void testCreateMissingFields() throws Exception {
        // Missing connectorType
        String requestBody1 =
                "{"
                        + "\"metadataDatasourceId\": \"test_missing_field\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://localhost:3306/test\""
                        + "}"
                        + "}";

        Response response1 =
                given().body(requestBody1)
                        .contentType(JSON)
                        .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response1.jsonPath().getString("status"));
        Assertions.assertTrue(response1.jsonPath().getString("message").contains("connectorType"));

        // Missing metadataDatasourceId
        String requestBody2 =
                "{"
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://localhost:3306/test\""
                        + "}"
                        + "}";

        Response response2 =
                given().body(requestBody2)
                        .contentType(JSON)
                        .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response2.jsonPath().getString("status"));
        Assertions.assertTrue(
                response2.jsonPath().getString("message").contains("metadataDatasourceId"));
    }

    @Test
    void testGetDatasource() throws Exception {
        // Create a datasource first
        createTestDatasource("test_get_ds", "jdbc:mysql://mysql-e2e:3306/seatunnel");

        // Get the datasource
        Response response =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_get_ds")
                        .then()
                        .statusCode(200)
                        .extract()
                        .response();

        Assertions.assertEquals(
                "test_get_ds", response.jsonPath().getString("metadataDatasourceId"));
        Assertions.assertEquals("Jdbc", response.jsonPath().getString("connectorType"));
        Assertions.assertNotNull(response.jsonPath().get("properties"));

        // Sensitive key should be masked
        Assertions.assertEquals("******", response.jsonPath().getString("properties.password"));

        // Non-sensitive keys should display actual values
        Assertions.assertEquals(
                "jdbc:mysql://mysql-e2e:3306/seatunnel",
                response.jsonPath().getString("properties.url"));
        Assertions.assertEquals("root", response.jsonPath().getString("properties.user"));
        Assertions.assertEquals(
                "com.mysql.cj.jdbc.Driver", response.jsonPath().getString("properties.driver"));
    }

    @Test
    void testGetNonExistentDatasource() throws Exception {
        Response response =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/non_existent_ds")
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response.jsonPath().getString("status"));
        Assertions.assertTrue(response.jsonPath().getString("message").contains("not found"));
    }

    @Test
    void testListDatasources() throws Exception {
        // Create multiple datasources
        createTestDatasource("test_list_ds1", "jdbc:mysql://mysql-e2e:3306/seatunnel");
        createTestDatasource("test_list_ds2", "jdbc:mysql://mysql-e2e:3306/seatunnel");

        // List all datasources
        Response response =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCES)
                        .then()
                        .statusCode(200)
                        .extract()
                        .response();

        List<Map<String, Object>> datasources = response.jsonPath().getList("$");
        Assertions.assertTrue(datasources.size() >= 2);

        // Verify our datasources are in the list
        boolean found1 =
                datasources.stream()
                        .anyMatch(ds -> "test_list_ds1".equals(ds.get("metadataDatasourceId")));
        boolean found2 =
                datasources.stream()
                        .anyMatch(ds -> "test_list_ds2".equals(ds.get("metadataDatasourceId")));
        Assertions.assertTrue(found1);
        Assertions.assertTrue(found2);
    }

    @Test
    void testUpdateDatasource() throws Exception {
        // Create a datasource
        createTestDatasource("test_update_ds", "jdbc:mysql://mysql-e2e:3306/seatunnel");

        // Update the datasource
        String updateBody =
                "{"
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://updated-host:3306/seatunnel\","
                        + "  \"driver\": \"com.mysql.cj.jdbc.Driver\","
                        + "  \"user\": \"updated_user\""
                        + "}"
                        + "}";

        Response response =
                given().body(updateBody)
                        .contentType(JSON)
                        .put(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_update_ds")
                        .then()
                        .statusCode(200)
                        .extract()
                        .response();

        Assertions.assertEquals("success", response.jsonPath().getString("status"));

        // Verify the update
        Response getResponse =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_update_ds")
                        .then()
                        .extract()
                        .response();

        Map<String, Object> properties = getResponse.jsonPath().getMap("properties");
        Assertions.assertTrue(properties.containsValue("updated_user"));
    }

    @Test
    void testUpdateNonExistentDatasource() throws Exception {
        String updateBody =
                "{"
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://localhost:3306/test\""
                        + "}"
                        + "}";

        Response response =
                given().body(updateBody)
                        .contentType(JSON)
                        .put(baseUrl + REST_URL_METADATA_DATASOURCE + "/non_existent_update_ds")
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response.jsonPath().getString("status"));
        Assertions.assertTrue(response.jsonPath().getString("message").contains("not found"));
    }

    @Test
    void testDeleteDatasource() throws Exception {
        // Create a datasource
        createTestDatasource("test_delete_ds", "jdbc:mysql://mysql-e2e:3306/seatunnel");

        // Delete the datasource
        Response response =
                given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_delete_ds")
                        .then()
                        .statusCode(200)
                        .extract()
                        .response();

        Assertions.assertEquals("success", response.jsonPath().getString("status"));
        Assertions.assertEquals(
                "test_delete_ds", response.jsonPath().getString("metadataDatasourceId"));

        // Verify it's deleted
        Response getResponse =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_delete_ds")
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", getResponse.jsonPath().getString("status"));
    }

    @Test
    void testDeleteNonExistentDatasource() throws Exception {
        Response response =
                given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/non_existent_delete_ds")
                        .then()
                        .extract()
                        .response();

        Assertions.assertEquals("error", response.jsonPath().getString("status"));
        Assertions.assertTrue(response.jsonPath().getString("message").contains("not found"));
    }

    @Test
    void testFullCrudLifecycle() throws Exception {
        String dsId = "test_lifecycle_ds";

        // CREATE
        createTestDatasource(dsId, "jdbc:mysql://mysql-e2e:3306/seatunnel");
        given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId).then().statusCode(200);

        // READ
        Response getResponse =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId)
                        .then()
                        .extract()
                        .response();
        Assertions.assertEquals(dsId, getResponse.jsonPath().getString("metadataDatasourceId"));

        // UPDATE
        String updateBody =
                "{\"properties\": {\"url\": \"jdbc:mysql://lifecycle:3306/seatunnel\"}}";
        given().body(updateBody)
                .put(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId)
                .then()
                .statusCode(200);

        // Verify update
        Response updatedResponse =
                given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId)
                        .then()
                        .extract()
                        .response();
        Assertions.assertTrue(
                updatedResponse.jsonPath().getString("properties.url").contains("lifecycle"));

        // DELETE
        given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId).then().statusCode(200);

        // Verify deleted
        given().get(baseUrl + REST_URL_METADATA_DATASOURCE + "/" + dsId).then().statusCode(400);
    }

    // ==================== Integration Tests ====================

    @Test
    void testDynamicMetadataProviderMysqlToMysql() throws Exception {
        // 1. Register source datasource via REST API
        String sourceDsBody =
                "{"
                        + "\"metadataDatasourceId\": \"test_datasource\","
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://mysql-e2e:3306/seatunnel\","
                        + "  \"driver\": \"com.mysql.cj.jdbc.Driver\","
                        + "  \"user\": \"root\","
                        + "  \"password\": \"Abc!@#135_seatunnel\""
                        + "}"
                        + "}";

        given().body(sourceDsBody)
                .contentType(JSON)
                .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                .then()
                .statusCode(200);

        // 2. Create source table and insert test data
        createSourceTable();
        insertTestData(100);

        // 3. Execute job using metadata_datasource_id
        Container.ExecResult execResult =
                executeJob("/jdbc_mysql_source_to_sink_with_dynamic_metadata.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // 4. Verify sink table contains expected data
        verifySinkTableData(100);

        // 5. Cleanup datasources
        given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/test_datasource");
    }

    // ==================== JSON Job Submission Tests ====================

    @Test
    void testJsonJobSubmissionWithDynamicMetadata() throws Exception {
        // 1. Register datasources
        String sourceDsBody =
                "{"
                        + "\"metadataDatasourceId\": \"json_test_datasource\","
                        + "\"connectorType\": \"Jdbc\","
                        + "\"properties\": {"
                        + "  \"url\": \"jdbc:mysql://mysql-e2e:3306/seatunnel\","
                        + "  \"driver\": \"com.mysql.cj.jdbc.Driver\","
                        + "  \"user\": \"root\","
                        + "  \"password\": \"Abc!@#135_seatunnel\""
                        + "}"
                        + "}";

        given().body(sourceDsBody)
                .contentType(JSON)
                .post(baseUrl + REST_URL_METADATA_DATASOURCE)
                .then()
                .statusCode(200);

        // 2. Create source table with test data
        createSourceTable();
        insertTestData(30);

        // 3. Submit job via REST API with JSON format
        String jobJson =
                "{"
                        + "    \"env\": {"
                        + "        \"job.mode\": \"BATCH\""
                        + "    },"
                        + "    \"source\": ["
                        + "        {"
                        + "            \"plugin_name\": \"Jdbc\","
                        + "            \"metadata_datasource_id\": \"json_test_datasource\","
                        + "            \"query\": \"select * from source\""
                        + "        }"
                        + "    ],"
                        + "    \"sink\": ["
                        + "        {"
                        + "            \"plugin_name\": \"Jdbc\","
                        + "            \"metadata_datasource_id\": \"json_test_datasource\","
                        + "            \"generate_sink_sql\": true,"
                        + "            \"database\": \"seatunnel\","
                        + "            \"table\": \"sink\""
                        + "        }"
                        + "    ]"
                        + "}";

        Response submitResponse =
                given().body(jobJson)
                        .queryParam("format", "json")
                        .contentType(JSON)
                        .post(baseUrl + REST_URL_SUBMIT_JOB)
                        .then()
                        .extract()
                        .response();

        // Verify job was submitted
        Assertions.assertNotNull(submitResponse.jsonPath().get("jobId"));

        Long jobId = submitResponse.jsonPath().getLong("jobId");

        // 4. Wait for job completion
        waitForJobFinished(jobId, 120);

        // 5. Verify job finished successfully
        String jobStatus = getJobStatus(jobId);
        Assertions.assertEquals("FINISHED", jobStatus, "Job should finish successfully");

        // 6. Verify data was written correctly
        verifySinkTableData(30);

        // 7. Cleanup
        given().delete(baseUrl + REST_URL_METADATA_DATASOURCE + "/json_test_datasource");
    }
}
