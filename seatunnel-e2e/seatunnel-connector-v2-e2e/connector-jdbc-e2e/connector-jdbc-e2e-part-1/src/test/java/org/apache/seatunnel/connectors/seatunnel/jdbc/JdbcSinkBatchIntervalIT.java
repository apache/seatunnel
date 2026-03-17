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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class JdbcSinkBatchIntervalIT extends TestSuiteBase implements TestResource {

    private static final String PG_IMAGE = "postgres:14-alpine";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        Startables.deepStart(Stream.of(postgreSQLContainer)).join();
        log.info("PostgreSQL container started");
        Class.forName(postgreSQLContainer.getDriverClassName());
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(this::initializeJdbcTable);
    }

    @TestTemplate
    public void testBatchIntervalFlush(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_sink_batch_interval.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Thread.sleep(2000);
        assertSinkRowCount("sink_batch_interval_timer", 100);
        truncateTable("sink_batch_interval_timer");
    }

    @TestTemplate
    public void testBatchSizeFlush(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_sink_batch_size.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Thread.sleep(2000);
        assertSinkRowCount("sink_batch_size_only", 100);
        truncateTable("sink_batch_size_only");
    }

    @TestTemplate
    public void testBatchIntervalWithBatchSize1(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult =
                container.executeJob("/jdbc_postgres_sink_batch_interval_with_batch_size_1.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        Thread.sleep(10000);
        assertSinkRowCount("sink_batch_interval_bs1", 50);
        truncateTable("sink_batch_interval_bs1");
    }

    private void assertSinkRowCount(String tableName, int expectedCount) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("SELECT count(*) FROM " + tableName)) {
                Assertions.assertTrue(resultSet.next());
                int actual = resultSet.getInt(1);
                Assertions.assertEquals(
                        expectedCount,
                        actual,
                        String.format(
                                "Expected %d rows in %s but found %d",
                                expectedCount, tableName, actual));
            }
        }
    }

    private void truncateTable(String tableName) throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE TABLE " + tableName);
            }
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                postgreSQLContainer.getJdbcUrl(),
                postgreSQLContainer.getUsername(),
                postgreSQLContainer.getPassword());
    }

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS %s(\n"
                    + "pk_id BIGINT NOT NULL PRIMARY KEY,\n"
                    + "name VARCHAR(255),\n"
                    + "score INT\n"
                    + ")";

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_timer"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_size_only"));
            statement.execute(String.format(CREATE_TABLE_SQL, "sink_batch_interval_bs1"));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSQL table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
