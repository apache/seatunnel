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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
public class TransformErrorToMysqlIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    private MySQLContainer<?> mysqlContainer;

    @BeforeAll
    @Override
    public void startUp() {
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withNetwork(TestContainer.NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withUrlParam("allowPublicKeyRetrieval", "true");

        Startables.deepStart(Stream.of(mysqlContainer)).join();
        log.info("MySQL container started with IP: {}", mysqlContainer.getHost());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE orders_from_transform ("
                                + "id INT PRIMARY KEY, "
                                + "name_int INT, "
                                + "age INT)");

                statement.execute(
                        "CREATE TABLE orders_transform_error ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_no_error ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_global ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_no_orig_stack ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_orig_only ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_drop ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_fail ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_block ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
                statement.execute(
                        "CREATE TABLE orders_transform_error_bad_sink ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "job_id BIGINT, "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create tables", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }

    @BeforeEach
    public void clearTables() throws Exception {
        if (mysqlContainer == null) {
            return;
        }
        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE TABLE orders_from_transform");
                statement.execute("TRUNCATE TABLE orders_transform_error");
                statement.execute("TRUNCATE TABLE orders_transform_error_no_error");
                statement.execute("TRUNCATE TABLE orders_transform_error_global");
                statement.execute("TRUNCATE TABLE orders_transform_error_no_orig_stack");
                statement.execute("TRUNCATE TABLE orders_transform_error_orig_only");
                statement.execute("TRUNCATE TABLE orders_transform_error_drop");
                statement.execute("TRUNCATE TABLE orders_transform_error_fail");
                statement.execute("TRUNCATE TABLE orders_transform_error_block");
                statement.execute("TRUNCATE TABLE orders_transform_error_bad_sink");
            }
        }
    }

    @TestTemplate
    public void testTransformErrorRoutedToMysql(TestContainer container) throws Exception {
        // No variables needed - credentials are hardcoded in the config file
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_fakesource_to_mysql_with_error_handler.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        // Verify data in MySQL
        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                // Normal rows - expect 2 rows that passed transformation
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(2, normalCount, "Should have 2 normal rows in main table");

                // Error rows - expect 2 rows that failed transformation
                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_transform_error");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(2, errorCount, "Should have 2 error rows in error table");

                log.info(
                        "Successfully verified normal count: {} and error count: {}",
                        normalCount,
                        errorCount);
            }
        } catch (SQLException e) {
            log.error("Failed to verify MySQL data", e);
            throw new RuntimeException("Failed to verify MySQL data", e);
        }
    }

    @TestTemplate
    public void testTransformErrorLogMode(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_error_handler_log_mode.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        2,
                        normalCount,
                        "In LOG mode, 2 normal rows should still be written to main table");
            }
        }
    }

    @TestTemplate
    public void testTransformLogModeMaxErrorRecordsThreshold(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_error_handler_log_max_error_records.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_records is exceeded in transform stage even in LOG mode");
    }

    @TestTemplate
    public void testTransformErrorHandlerWithNoErrors(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_no_error_with_error_handler.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(4, normalCount, "Should have 4 normal rows in main table");

                ResultSet ers =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM orders_transform_error_no_error");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        0, errorCount, "Error table should stay empty when no row errors");
            }
        }
    }

    @TestTemplate
    public void testGlobalErrorHandlerRoutesTransformErrors(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_fakesource_to_mysql_with_global_error_handler.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(2, normalCount, "Should have 2 normal rows in main table");

                ResultSet ers =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM orders_transform_error_global");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        2, errorCount, "Should have 2 error rows in global error handler table");
            }
        }
    }

    @TestTemplate
    public void testErrorRowsWithoutOriginalDataAndStacktrace(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_error_handler_no_original_no_stacktrace.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet ers =
                        statement.executeQuery(
                                "SELECT error_message, original_data, stacktrace "
                                        + "FROM orders_transform_error_no_orig_stack");
                int count = 0;
                while (ers.next()) {
                    count++;
                    String errorMessage = ers.getString(1);
                    String originalData = ers.getString(2);
                    String stacktrace = ers.getString(3);
                    Assertions.assertNotNull(
                            errorMessage, "error_message should not be null for error rows");
                    Assertions.assertNull(
                            originalData,
                            "original_data should be NULL when include_original_data=false");
                    Assertions.assertNull(
                            stacktrace, "stacktrace should be NULL when include_stacktrace=false");
                }
                Assertions.assertEquals(2, count, "Should have 2 error rows in error table");
            }
        }
    }

    @TestTemplate
    public void testErrorRowsWithOriginalDataOnly(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_error_handler_original_only.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet ers =
                        statement.executeQuery(
                                "SELECT error_message, original_data, stacktrace "
                                        + "FROM orders_transform_error_orig_only");
                int count = 0;
                while (ers.next()) {
                    count++;
                    String errorMessage = ers.getString(1);
                    String originalData = ers.getString(2);
                    String stacktrace = ers.getString(3);
                    Assertions.assertNotNull(
                            errorMessage, "error_message should not be null for error rows");
                    Assertions.assertNotNull(
                            originalData,
                            "original_data should not be NULL when include_original_data=true");
                    Assertions.assertNull(
                            stacktrace, "stacktrace should be NULL when include_stacktrace=false");
                }
                Assertions.assertEquals(2, count, "Should have 2 error rows in error table");
            }
        }
    }

    @TestTemplate
    public void testQueueOverflowDropPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_error_handler_queue_drop.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0, normalCount, "All rows fail in transform, main table should be empty");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_transform_error_drop");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertTrue(
                        errorCount >= 2 && errorCount <= 20,
                        "Error table should contain some but not more than total error rows");
            }
        }
    }

    @TestTemplate
    @Disabled("Depends on error sink queue overflow timing")
    public void testQueueOverflowFailPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_error_handler_queue_fail.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when queue_overflow_policy=FAIL and queue overflows");
    }

    @TestTemplate
    public void testQueueOverflowBlockPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_error_handler_queue_block.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0,
                        normalCount,
                        "All rows fail in transform for BLOCK policy scenario, main table should be empty");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_transform_error_block");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        20,
                        errorCount,
                        "With BLOCK policy, all error rows should be written to the error table");
            }
        }
    }

    @TestTemplate
    public void testTransformMaxErrorRecordsThreshold(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_error_handler_max_error_records.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_records is exceeded in transform stage");
    }

    @TestTemplate
    public void testTransformMaxErrorRatioThreshold(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_error_handler_max_error_ratio.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_ratio is exceeded in transform stage");
    }

    @TestTemplate
    public void testErrorSinkInitializationFailure(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/transform_fakesource_to_mysql_with_bad_error_sink.conf");

        Assertions.assertNotEquals(
                0, result.getExitCode(), "Job should fail when error sink initialization fails");
    }
}
