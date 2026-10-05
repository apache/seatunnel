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
public class SinkErrorToMysqlIT extends TestSuiteBase implements TestResource {

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
                        "CREATE TABLE orders_from_sink ("
                                + "id INT PRIMARY KEY, "
                                + "name VARCHAR(10), "
                                + "age INT)");

                statement.execute(
                        "CREATE TABLE orders_sink_error_basic ("
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
                        "CREATE TABLE orders_sink_error_no_orig_stack ("
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
                        "CREATE TABLE orders_sink_error_orig_only ("
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
                        "CREATE TABLE orders_sink_error_drop ("
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
                        "CREATE TABLE orders_sink_error_fail ("
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
                        "CREATE TABLE orders_sink_error_block ("
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
                        "CREATE TABLE orders_sink_error_both ("
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
                        "CREATE TABLE orders_sink_error_bad_sink ("
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
                        "CREATE TABLE orders_transform_error_both ("
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
            throw new RuntimeException("Failed to create tables for sink tests", e);
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
                statement.execute("TRUNCATE TABLE orders_from_sink");
                statement.execute("TRUNCATE TABLE orders_sink_error_basic");
                statement.execute("TRUNCATE TABLE orders_sink_error_no_orig_stack");
                statement.execute("TRUNCATE TABLE orders_sink_error_orig_only");
                statement.execute("TRUNCATE TABLE orders_sink_error_drop");
                statement.execute("TRUNCATE TABLE orders_sink_error_fail");
                statement.execute("TRUNCATE TABLE orders_sink_error_block");
                statement.execute("TRUNCATE TABLE orders_sink_error_both");
                statement.execute("TRUNCATE TABLE orders_sink_error_bad_sink");
                statement.execute("TRUNCATE TABLE orders_transform_error_both");
            }
        }
    }

    @TestTemplate
    public void testSinkErrorRoutedToMysql(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_with_error_handler.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        2, normalCount, "Should have 2 normal rows in sink main table");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_basic");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        2, errorCount, "Should have 2 error rows in sink error table");
            }
        }
    }

    @TestTemplate
    public void testSinkBatchDataErrorRoutedToMysqlAtClose(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_with_error_handler_batch_close.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0,
                        normalCount,
                        "Batch data error should drop the whole batch, no rows in main sink table");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_basic");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        4,
                        errorCount,
                        "RowErrorCollector should report all rows in the failing batch");
            }
        }
    }

    @TestTemplate
    public void testSinkBatchConstraintErrorRoutedToMysqlAtClose(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_with_error_handler_batch_close_constraint.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0,
                        normalCount,
                        "Batch constraint error should drop the whole batch, no rows in main sink table");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_basic");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        4,
                        errorCount,
                        "RowErrorCollector should report all rows in the failing batch");
            }
        }
    }

    @TestTemplate
    public void testSinkErrorLogMode(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_log_mode.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        2,
                        normalCount,
                        "In LOG mode, 2 normal rows should still be written to main sink table");
            }
        }
    }

    @TestTemplate
    public void testSinkErrorWithoutHandlerFailsJob(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_without_error_handler.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when sink_error_handler is disabled and sink row errors occur");
    }

    @TestTemplate
    public void testSinkErrorRoutedWithGlobalErrorHandler(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_with_global_error_handler.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0 when using global env.error_handler, stderr: "
                        + result.getStderr());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        2, normalCount, "Should have 2 normal rows in sink main table");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_basic");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        2,
                        errorCount,
                        "Global env.error_handler for SINK should route 2 error rows to sink error table");
            }
        }
    }

    @TestTemplate
    public void testSinkErrorRowsWithoutOriginalDataAndStacktrace(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_error_handler_no_original_no_stacktrace.conf");

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
                                        + "FROM orders_sink_error_no_orig_stack");
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
                Assertions.assertEquals(2, count, "Should have 2 error rows in sink error table");
            }
        }
    }

    @TestTemplate
    public void testSinkErrorRowsWithOriginalDataOnly(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_original_only.conf");

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
                                        + "FROM orders_sink_error_orig_only");
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
                Assertions.assertEquals(2, count, "Should have 2 error rows in sink error table");
            }
        }
    }

    @TestTemplate
    public void testSinkQueueOverflowDropPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_queue_drop.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0, normalCount, "All rows fail in sink, main sink table should be empty");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_drop");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertTrue(
                        errorCount >= 2 && errorCount <= 20,
                        "Sink error table should contain some but not more than total error rows");
            }
        }
    }

    @Disabled("Depends on error sink queue overflow timing")
    @TestTemplate
    public void testSinkQueueOverflowFailPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_queue_fail.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when sink queue_overflow_policy=FAIL and queue overflows");
    }

    @TestTemplate
    public void testSinkQueueOverflowBlockPolicy(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_queue_block.conf");

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
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        0,
                        normalCount,
                        "All rows fail in sink for BLOCK policy scenario, main sink table should be empty");

                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_block");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(
                        20,
                        errorCount,
                        "With BLOCK policy, all error rows should be written to the sink error table");
            }
        }
    }

    @TestTemplate
    public void testSinkMaxErrorRecordsThreshold(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_max_error_records.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_records is exceeded in sink stage");
    }

    @TestTemplate
    public void testSinkMaxErrorRatioThreshold(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_max_error_ratio.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_ratio is exceeded in sink stage");
    }

    @TestTemplate
    public void testSinkErrorSinkInitializationFailure(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_fakesource_to_mysql_with_bad_error_sink.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when sink error sink initialization fails");
    }

    @TestTemplate
    public void testSinkSystemErrorNotHandledAsRowError(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/sink_error_handler_system_error.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when sink encounters a system-level error even if sink_error_handler is configured");
    }

    @TestTemplate
    public void testSinkErrorSinkRuntimeFailureFailPolicy(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_error_handler_runtime_bad_error_sink.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when error sink encounters a runtime failure and queue_overflow_policy=FAIL");
    }

    @TestTemplate
    public void testSinkLogModeMaxErrorRecordsThreshold(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/error-handling/sink_error_handler_log_max_error_records.conf");

        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "Job should fail when max_error_records is exceeded in sink stage even in LOG mode");
    }

    @TestTemplate
    public void testTransformAndSinkErrorHandlersBothStages(TestContainer container)
            throws Exception {
        Container.ExecResult result =
                container.executeJob("/error-handling/transform_and_sink_error_handlers.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0 when both transform and sink error handlers are enabled");

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_sink");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(
                        1,
                        normalCount,
                        "Exactly one row should pass both transform and sink without errors");

                ResultSet transformErrors =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_transform_error_both");
                Assertions.assertTrue(
                        transformErrors.next(), "Should have count result for transform errors");
                int transformErrorCount = transformErrors.getInt(1);
                Assertions.assertEquals(
                        1,
                        transformErrorCount,
                        "Exactly one row should be routed to transform error table");

                ResultSet sinkErrors =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_sink_error_both");
                Assertions.assertTrue(
                        sinkErrors.next(), "Should have count result for sink errors");
                int sinkErrorCount = sinkErrors.getInt(1);
                Assertions.assertEquals(
                        2, sinkErrorCount, "Exactly two rows should be routed to sink error table");
            }
        }
    }
}
