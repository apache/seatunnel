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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark engine will lose the row kind of record")
@Slf4j
public class JdbcSinkCDCChangelogIT extends TestSuiteBase implements TestResource {
    private static final String PG_IMAGE = "postgres:14-alpine";
    private PostgreSQLContainer<?> postgreSQLContainer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("org.postgresql.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/Jdbc/lib");

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        postgreSQLContainer =
                new PostgreSQLContainer<>(DockerImageName.parse(PG_IMAGE))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgresql")
                        .withExposedPorts(5432)
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
    public void testSinkCDCChangelog(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = container.executeJob("/jdbc_sink_cdc_changelog.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection =
                DriverManager.getConnection(
                        postgreSQLContainer.getJdbcUrl(),
                        postgreSQLContainer.getUsername(),
                        postgreSQLContainer.getPassword())) {
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "select pk_id, name, score from sink order by pk_id")) {
                while (resultSet.next()) {
                    List<Object> row =
                            Arrays.asList(
                                    resultSet.getLong("pk_id"),
                                    resultSet.getString("name"),
                                    resultSet.getInt("score"));
                    actual.add(row);
                }
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(Arrays.asList(1L, "A_1", 100), Arrays.asList(3L, "C", 100))
                        .collect(Collectors.toSet());
        Assertions.assertEquals(expected, actual);
        try (Connection connection =
                DriverManager.getConnection(
                        postgreSQLContainer.getJdbcUrl(),
                        postgreSQLContainer.getUsername(),
                        postgreSQLContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("truncate table sink");
                log.info("testSinkCDCChangelog truncate table sink");
            }
        }
    }

    /**
     * A WHERE clause of the Sql transform on a CDC stream: row 1 goes from active to inactive and
     * must be removed from the keyed sink table, row 3 goes from inactive to active and must be
     * written, row 2 stays active.
     */
    @TestTemplate
    public void testSqlWhereOnCDCChangelog(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        assertWhereOnCDCChangelog(container, "/jdbc_sink_cdc_changelog_sql_where.conf");
    }

    /** The same as {@link #testSqlWhereOnCDCChangelog} with the Calcite transform. */
    @TestTemplate
    public void testCalciteWhereOnCDCChangelog(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        assertWhereOnCDCChangelog(container, "/jdbc_sink_cdc_changelog_calcite_where.conf");
    }

    private void assertWhereOnCDCChangelog(TestContainer container, String config)
            throws IOException, InterruptedException, SQLException {
        executeSql("truncate table sink_status");
        try {
            Container.ExecResult execResult = container.executeJob(config);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            List<List<Object>> actual = new ArrayList<>();
            try (Connection connection =
                            DriverManager.getConnection(
                                    postgreSQLContainer.getJdbcUrl(),
                                    postgreSQLContainer.getUsername(),
                                    postgreSQLContainer.getPassword());
                    Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery(
                                    "select pk_id, status from sink_status order by pk_id")) {
                while (resultSet.next()) {
                    actual.add(
                            Arrays.asList(
                                    resultSet.getLong("pk_id"), resultSet.getString("status")));
                }
            }
            Assertions.assertEquals(
                    Arrays.asList(Arrays.asList(2L, "active"), Arrays.asList(3L, "active")),
                    actual);
        } finally {
            executeSql("truncate table sink_status");
        }
    }

    private void executeSql(String sql) throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                postgreSQLContainer.getJdbcUrl(),
                                postgreSQLContainer.getUsername(),
                                postgreSQLContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private void initializeJdbcTable() {
        try (Connection connection =
                DriverManager.getConnection(
                        postgreSQLContainer.getJdbcUrl(),
                        postgreSQLContainer.getUsername(),
                        postgreSQLContainer.getPassword())) {
            Statement statement = connection.createStatement();
            String sink =
                    "create table sink(\n"
                            + "pk_id BIGINT NOT NULL PRIMARY KEY,\n"
                            + "name varchar(255),\n"
                            + "score INT\n"
                            + ")";
            statement.execute(sink);
            statement.execute(
                    "create table if not exists sink_status(\n"
                            + "pk_id BIGINT NOT NULL PRIMARY KEY,\n"
                            + "status varchar(255)\n"
                            + ")");
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (postgreSQLContainer != null) {
            postgreSQLContainer.stop();
        }
    }
}
