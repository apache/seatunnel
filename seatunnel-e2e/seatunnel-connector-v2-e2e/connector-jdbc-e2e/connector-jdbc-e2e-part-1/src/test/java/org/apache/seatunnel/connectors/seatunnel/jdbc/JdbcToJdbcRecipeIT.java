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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

/**
 * Verifies the JDBC-to-JDBC scenario recipe with real MySQL and PostgreSQL containers.
 *
 * <p>The job filters unpaid orders, uppercases customer names, narrows the amount scale, adds a
 * constant source-system field, and writes the transformed rows to PostgreSQL.
 */
@Slf4j
public class JdbcToJdbcRecipeIT extends TestSuiteBase implements TestResource {

    // MySQL image used by the documented source side of the migration.
    private static final String MYSQL_IMAGE = "mysql:8.0";

    // PostgreSQL image used by the documented target side of the migration.
    private static final String POSTGRES_IMAGE = "postgres:14-alpine";

    // Stable Docker network alias referenced by the E2E job configuration.
    private static final String MYSQL_HOST = "mysql-jdbc-recipe";

    // Stable PostgreSQL network alias referenced by the E2E job configuration.
    private static final String POSTGRES_HOST = "postgresql-jdbc-recipe";

    // MySQL database containing the source orders table.
    private static final String MYSQL_DATABASE = "source_db";

    // PostgreSQL database containing the migrated paid orders.
    private static final String POSTGRES_DATABASE = "target_db";

    // Shared test user provisioned independently in both database containers.
    private static final String DATABASE_USER = "test";

    // Password used only by the isolated database containers in this test.
    private static final String DATABASE_PASSWORD = "test";

    // MySQL source container shared by all engine invocations in this test class.
    private MySQLContainer<?> mysqlContainer;

    // PostgreSQL target container shared by all engine invocations in this test class.
    private PostgreSQLContainer<?> postgreSQLContainer;

    // Installs both JDBC drivers in every SeaTunnel engine container before job submission.
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                        .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");
                DependencyJar.ofClassName("org.postgresql.Driver")
                        .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");
            };

    /**
     * Starts both databases and creates deterministic source and target tables.
     *
     * @throws Exception if a container, JDBC driver, or table initialization cannot be prepared
     */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(DATABASE_USER)
                        .withPassword(DATABASE_PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE))
                        .withDatabaseName(POSTGRES_DATABASE)
                        .withUsername(DATABASE_USER)
                        .withPassword(DATABASE_PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(POSTGRES_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(POSTGRES_IMAGE)));

        Startables.deepStart(Stream.of(mysqlContainer, postgreSQLContainer)).join();
        Class.forName(mysqlContainer.getDriverClassName());
        Class.forName(postgreSQLContainer.getDriverClassName());

        given().ignoreExceptions()
                .await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            initializeMysqlSource();
                            initializePostgresTarget();
                        });
        log.info("MySQL source and PostgreSQL target are ready for the recipe test.");
    }

    /**
     * Clears target rows before each engine invocation so stale data cannot satisfy assertions.
     *
     * @throws SQLException if the PostgreSQL target cannot be reset
     */
    @BeforeEach
    public void clearTargetTable() throws SQLException {
        try (Connection connection = getPostgresConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE public.paid_orders");
        }
    }

    /**
     * Runs the documented pipeline and checks every transformed PostgreSQL value.
     *
     * @param container SeaTunnel engine container supplied by the E2E extension
     * @throws Exception if the job cannot run or the target rows cannot be queried
     */
    @TestTemplate
    public void testJdbcToJdbcRecipe(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/jdbc_to_jdbc_with_transform.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        try (Connection connection = getPostgresConnection();
                Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT id, customer_name, amount, source_system "
                                        + "FROM public.paid_orders ORDER BY id")) {
            assertRow(rows, 1001L, "ALICE CHEN", new BigDecimal("120.50"));
            assertRow(rows, 1003L, "CAROL WU", new BigDecimal("42.00"));
            Assertions.assertFalse(rows.next(), "Only the two PAID orders should be written");
        }
    }

    /**
     * Creates and seeds the MySQL table used by the documented source query.
     *
     * @throws SQLException if the source table cannot be recreated
     */
    private void initializeMysqlSource() throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysqlContainer.getJdbcUrl(), DATABASE_USER, DATABASE_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute(
                    "CREATE TABLE orders ("
                            + "id BIGINT PRIMARY KEY, "
                            + "customer_name VARCHAR(100) NOT NULL, "
                            + "amount DECIMAL(16, 4) NOT NULL, "
                            + "status VARCHAR(20) NOT NULL)");
            statement.execute(
                    "INSERT INTO orders (id, customer_name, amount, status) VALUES "
                            + "(1001, 'alice chen', 120.5000, 'PAID'), "
                            + "(1002, 'bob li', 80.0000, 'CREATED'), "
                            + "(1003, 'carol wu', 42.0000, 'PAID')");
        }
    }

    /**
     * Creates the PostgreSQL table whose contents are asserted after the job.
     *
     * @throws SQLException if the target table cannot be created
     */
    private void initializePostgresTarget() throws SQLException {
        try (Connection connection = getPostgresConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE IF NOT EXISTS public.paid_orders ("
                            + "id BIGINT PRIMARY KEY, "
                            + "customer_name VARCHAR(100) NOT NULL, "
                            + "amount DECIMAL(12, 2) NOT NULL, "
                            + "source_system VARCHAR(20) NOT NULL)");
        }
    }

    /**
     * Returns a host-side connection to the PostgreSQL test container.
     *
     * @return an open PostgreSQL connection that the caller must close
     * @throws SQLException if the container database cannot be reached
     */
    private Connection getPostgresConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(), DATABASE_USER, DATABASE_PASSWORD);
    }

    /**
     * Asserts one ordered target row, including the constant source-system field.
     *
     * @param rows ordered query result positioned before the expected row
     * @param id expected order identifier
     * @param customerName expected normalized customer name
     * @param amount expected amount with scale two
     * @throws SQLException if a target value cannot be read
     */
    private void assertRow(ResultSet rows, long id, String customerName, BigDecimal amount)
            throws SQLException {
        Assertions.assertTrue(rows.next(), "Expected target row " + id);
        Assertions.assertEquals(id, rows.getLong("id"));
        Assertions.assertEquals(customerName, rows.getString("customer_name"));
        Assertions.assertEquals(amount, rows.getBigDecimal("amount"));
        Assertions.assertEquals("MYSQL", rows.getString("source_system"));
    }

    /**
     * Stops the source and target database containers after all engine invocations have completed.
     */
    @AfterAll
    @Override
    public void tearDown() {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
        if (postgreSQLContainer != null) {
            postgreSQLContainer.close();
        }
    }
}
