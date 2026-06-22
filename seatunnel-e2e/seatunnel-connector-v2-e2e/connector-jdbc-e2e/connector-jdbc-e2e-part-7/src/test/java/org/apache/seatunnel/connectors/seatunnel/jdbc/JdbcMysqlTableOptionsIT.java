/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Stream;

@Slf4j
public class JdbcMysqlTableOptionsIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e-table-options";
    private static final String MYSQL_DATABASE = "seatunnel";
    private static final String MYSQL_SOURCE = "source";
    private static final String MYSQL_SINK = "sink_table_options";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";

    private static final String CONFIG_FILE = "/jdbc_mysql_sink_with_table_options.conf";

    private static final String CREATE_SOURCE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS `"
                    + MYSQL_SOURCE
                    + "` (\n"
                    + "    `id`   BIGINT       NOT NULL,\n"
                    + "    `name` VARCHAR(255) DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ");";

    private static final String INSERT_SOURCE_SQL =
            "INSERT INTO `"
                    + MYSQL_SOURCE
                    + "` (`id`, `name`) VALUES (1, 'name_1'), (2, 'name_2'), (3, 'name_3');";

    // MySQL 8.0.43 cold start may exceed the default 120s JDBC wait in Testcontainers.
    private static final int MYSQL_STARTUP_TIMEOUT_SECONDS =
            (int) Duration.ofMinutes(10).getSeconds();

    private MySQLContainer<?> mysqlContainer;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + MYSQL_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    void initContainer() {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);
        mysqlContainer =
                new MySQLContainer<>(imageName)
                        .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(7)))
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withStartupTimeoutSeconds(MYSQL_STARTUP_TIMEOUT_SECONDS)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        Startables.deepStart(Stream.of(mysqlContainer)).join();
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        initContainer();
        initializeJdbcTable();
    }

    @Override
    @AfterAll
    public void tearDown() {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    @TestTemplate
    public void testTableOptionsSink(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        try {
            Container.ExecResult execResult = container.executeJob(CONFIG_FILE);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            assertSinkTableOptions();
        } finally {
            clearSinkTable();
        }
    }

    private void assertSinkTableOptions() throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            ResultSet createTableResult =
                    statement.executeQuery(
                            String.format(
                                    "SHOW CREATE TABLE `%s`.`%s`", MYSQL_DATABASE, MYSQL_SINK));
            Assertions.assertTrue(createTableResult.next());
            String createTableSql = createTableResult.getString(2).toLowerCase();
            Assertions.assertTrue(
                    createTableSql.contains(
                            MySqlCatalog.TABLE_OPTION_ENGINE.toLowerCase() + "=innodb"),
                    createTableSql);
            Assertions.assertTrue(
                    createTableSql.contains(
                            MySqlCatalog.TABLE_OPTION_CHARSET.toLowerCase() + "=utf8mb4"),
                    createTableSql);
            Assertions.assertTrue(
                    createTableSql.contains(
                            MySqlCatalog.TABLE_OPTION_COLLATE.toLowerCase()
                                    + "=utf8mb4_unicode_ci"),
                    createTableSql);

            ResultSet countResult =
                    statement.executeQuery(
                            String.format(
                                    "SELECT COUNT(*) FROM `%s`.`%s`", MYSQL_DATABASE, MYSQL_SINK));
            Assertions.assertTrue(countResult.next());
            Assertions.assertEquals(3, countResult.getInt(1));
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(CREATE_SOURCE_TABLE_SQL);
            statement.execute(INSERT_SOURCE_SQL);
        } catch (SQLException e) {
            throw new RuntimeException("Initializing MySQL table failed!", e);
        }
    }

    private void clearSinkTable() {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("DROP TABLE IF EXISTS `%s`.`%s`", MYSQL_DATABASE, MYSQL_SINK));
        } catch (SQLException e) {
            throw new RuntimeException("Clearing sink table failed!", e);
        }
    }
}
