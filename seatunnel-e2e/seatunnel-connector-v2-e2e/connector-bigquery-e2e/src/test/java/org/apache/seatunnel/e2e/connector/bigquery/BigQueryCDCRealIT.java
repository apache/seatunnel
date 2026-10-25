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

package org.apache.seatunnel.e2e.connector.bigquery;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.await;

/**
 * Runs BigQuery CDC and ADD COLUMN schema evolution against the real BigQuery service.
 *
 * <p>This test is opt-in because it requires GCP credentials, creates billable BigQuery jobs, and
 * cannot be verified correctly by goccy/bigquery-emulator.
 *
 * <p>Set {@code BIGQUERY_E2E_REAL=true}, {@code BIGQUERY_E2E_PROJECT_ID}, {@code
 * BIGQUERY_E2E_DATASET_ID}, and {@code GOOGLE_APPLICATION_CREDENTIALS}. The dataset must already
 * exist, and the credentials file must contain a service account key.
 */
@Slf4j
@EnabledIfEnvironmentVariable(named = "BIGQUERY_E2E_REAL", matches = "(?i)true")
@DisabledOnContainer(
        value = {},
        type = EngineType.SPARK,
        disabledReason =
                "Spark translation does not currently propagate schema change events to sink writers.")
public class BigQueryCDCRealIT extends TestSuiteBase {

    private static final int INITIAL_SNAPSHOT_TIMEOUT_SECONDS = 180;
    // The Storage Write API can take minutes to detect a table schema update.
    private static final int CHANGE_TIMEOUT_SECONDS = 420;
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";
    private static final String CREDENTIALS_PATH_IN_CONTAINER =
            "/tmp/seatunnel/bigquery-service-account.json";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final Pattern PROJECT_ID_PATTERN =
            Pattern.compile("[a-z][a-z0-9-]{4,28}[a-z0-9]");
    private static final Pattern DATASET_ID_PATTERN = Pattern.compile("[A-Za-z0-9_]+");
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private String projectId;
    private String datasetId;
    private Path credentialsPath;
    private BigQuery bigQuery;
    private TableId tableId;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.copyFileToContainer(
                        MountableFile.forHostPath(credentialsPath), CREDENTIALS_PATH_IN_CONTAINER);
                Container.ExecResult mkdirResult =
                        container.execInContainer("bash", "-c", "mkdir -p " + MYSQL_CDC_PLUGIN_LIB);
                Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

                Path driverJarPath = mysqlDriverJarPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(driverJarPath),
                        MYSQL_CDC_PLUGIN_LIB + "/" + driverJarPath.getFileName());
            };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private static Path mysqlDriverJarPath() {
        try {
            Path driverJarPath =
                    Paths.get(
                            Driver.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(driverJarPath),
                    "MySQL JDBC driver should be resolved from the test classpath");
            return driverJarPath;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve MySQL JDBC driver jar from the test classpath", e);
        }
    }

    @BeforeAll
    public void startResources() throws IOException {
        projectId =
                requireIdentifier(
                        "BIGQUERY_E2E_PROJECT_ID",
                        requireEnvironment("BIGQUERY_E2E_PROJECT_ID"),
                        PROJECT_ID_PATTERN);
        datasetId =
                requireIdentifier(
                        "BIGQUERY_E2E_DATASET_ID",
                        requireEnvironment("BIGQUERY_E2E_DATASET_ID"),
                        DATASET_ID_PATTERN);
        credentialsPath =
                Paths.get(requireEnvironment("GOOGLE_APPLICATION_CREDENTIALS"))
                        .toAbsolutePath()
                        .normalize();
        Assertions.assertTrue(
                Files.isRegularFile(credentialsPath),
                "GOOGLE_APPLICATION_CREDENTIALS must point to a service account JSON file");

        try (InputStream credentials = Files.newInputStream(credentialsPath)) {
            bigQuery =
                    BigQueryOptions.newBuilder()
                            .setProjectId(projectId)
                            .setCredentials(ServiceAccountCredentials.fromStream(credentials))
                            .build()
                            .getService();
        }

        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
    }

    @BeforeEach
    public void initializeSourceTable() {
        executeMysqlSql("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + SOURCE_TABLE);
        executeMysqlSql(
                "CREATE TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " ("
                        + "uuid BIGINT NOT NULL, "
                        + "name VARCHAR(128), "
                        + "score INT, "
                        + "PRIMARY KEY (uuid)"
                        + ") ENGINE=InnoDB");
        executeMysqlSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (uuid, name, score) VALUES "
                        + "(1, 'Alice', 95), (2, 'Bob', 88)");
    }

    @AfterEach
    public void deleteBigQueryTable() {
        if (tableId != null) {
            bigQuery.delete(tableId);
            tableId = null;
        }
    }

    @AfterAll
    public void stopMySql() {
        MYSQL_CONTAINER.close();
    }

    @TestTemplate
    public void testCdcAndAddColumnSchemaEvolution(TestContainer container) throws Exception {
        tableId =
                TableId.of(
                        projectId,
                        datasetId,
                        "seatunnel_cdc_schema_" + UUID.randomUUID().toString().replace("-", ""));
        createBigQueryTable();

        CompletableFuture<Void> jobFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                Container.ExecResult result =
                                        container.executeJob(
                                                "/mysql_cdc_to_bigquery_real_sink.conf",
                                                Arrays.asList(
                                                        "bigquery_project_id=" + projectId,
                                                        "bigquery_dataset_id=" + datasetId,
                                                        "bigquery_table_id=" + tableId.getTable()));
                                if (result.getExitCode() != 0) {
                                    throw new IllegalStateException(result.getStderr());
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Set<List<Object>> initialRows =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95L), Arrays.asList(2L, "Bob", 88L))
                        .collect(Collectors.toSet());
        await().atMost(INITIAL_SNAPSHOT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(initialRows, queryRows(false));
                        });

        executeMysqlSql(
                "ALTER TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " ADD COLUMN email VARCHAR(255)");
        executeMysqlSql(
                "UPDATE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " SET email = CASE uuid "
                        + "WHEN 1 THEN 'alice@example.com' "
                        + "WHEN 2 THEN 'bob@example.com' END");

        Set<List<Object>> evolvedRows =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95L, "alice@example.com"),
                                Arrays.asList(2L, "Bob", 88L, "bob@example.com"))
                        .collect(Collectors.toSet());
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            assertEmailColumnExists();
                            Assertions.assertEquals(evolvedRows, queryRows(true));
                        });

        executeMysqlSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE + " WHERE uuid = 1");
        Set<List<Object>> rowsAfterDelete =
                Stream.<List<Object>>of(Arrays.asList(2L, "Bob", 88L, "bob@example.com"))
                        .collect(Collectors.toSet());
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(rowsAfterDelete, queryRows(true));
                        });

        executeMysqlSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (uuid, name, score, email) "
                        + "VALUES (1, 'Alice', 95, 'alice@example.com')");
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(evolvedRows, queryRows(true));
                        });
    }

    private void createBigQueryTable() throws InterruptedException {
        String sql =
                String.format(
                        "CREATE TABLE `%s.%s.%s` ("
                                + "uuid INT64 NOT NULL, "
                                + "name STRING, "
                                + "score INT64, "
                                + "PRIMARY KEY (uuid) NOT ENFORCED"
                                + ") OPTIONS ("
                                + "max_staleness = INTERVAL 0 MINUTE, "
                                + "expiration_timestamp = "
                                + "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
                                + ")",
                        projectId, datasetId, tableId.getTable());
        bigQuery.query(QueryJobConfiguration.of(sql));
    }

    private void assertEmailColumnExists() {
        Table table = bigQuery.getTable(tableId);
        Assertions.assertNotNull(table, "BigQuery target table does not exist");
        Assertions.assertNotNull(table.getDefinition().getSchema());
        boolean emailExists =
                table.getDefinition().getSchema().getFields().stream()
                        .map(Field::getName)
                        .anyMatch("email"::equalsIgnoreCase);
        Assertions.assertTrue(emailExists, "BigQuery target table does not contain email");
    }

    private Set<List<Object>> queryRows(boolean includeEmail) throws InterruptedException {
        String columns = includeEmail ? "uuid, name, score, email" : "uuid, name, score";
        String query =
                String.format(
                        "SELECT %s FROM `%s.%s.%s`",
                        columns, projectId, datasetId, tableId.getTable());
        TableResult result =
                bigQuery.query(
                        QueryJobConfiguration.newBuilder(query).setUseQueryCache(false).build());
        return StreamSupport.stream(result.iterateAll().spliterator(), false)
                .map(
                        row -> {
                            List<Object> values =
                                    Arrays.<Object>asList(
                                            row.get(0).isNull() ? null : row.get(0).getLongValue(),
                                            row.get(1).isNull()
                                                    ? null
                                                    : row.get(1).getStringValue(),
                                            row.get(2).isNull() ? null : row.get(2).getLongValue());
                            if (!includeEmail) {
                                return values;
                            }
                            return Arrays.<Object>asList(
                                    values.get(0),
                                    values.get(1),
                                    values.get(2),
                                    row.get(3).isNull() ? null : row.get(3).getStringValue());
                        })
                .collect(Collectors.toSet());
    }

    private void executeMysqlSql(String sql) {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void rethrowJobFailure(CompletableFuture<Void> jobFuture) {
        if (jobFuture.isCompletedExceptionally()) {
            jobFuture.join();
        }
    }

    private static String requireEnvironment(String name) {
        String value = System.getenv(name);
        Assertions.assertNotNull(value, name + " must be set when BIGQUERY_E2E_REAL=true");
        Assertions.assertFalse(value.trim().isEmpty(), name + " must not be empty");
        return value.trim();
    }

    private static String requireIdentifier(String name, String value, Pattern pattern) {
        Assertions.assertTrue(pattern.matcher(value).matches(), name + " is invalid: " + value);
        return value;
    }
}
