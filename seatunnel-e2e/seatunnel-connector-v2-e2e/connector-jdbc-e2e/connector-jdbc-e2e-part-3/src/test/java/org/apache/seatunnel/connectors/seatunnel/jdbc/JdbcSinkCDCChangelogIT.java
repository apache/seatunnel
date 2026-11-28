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
     * {@code where_rejected_update_as_delete} on a CDC stream, with the key taken from the primary
     * key of the source schema: row 1 goes from active to inactive and must be removed from the
     * keyed sink table, row 3 goes from inactive to active and must be written, row 2 stays active.
     */
    @TestTemplate
    public void testWhereRejectedUpdateAsDeleteWithSchemaKey(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        for (String config :
                Arrays.asList(
                        "/jdbc_sink_cdc_changelog_sql_where.conf",
                        "/jdbc_sink_cdc_changelog_calcite_where.conf")) {
            assertWhereOnCDCChangelog(
                    container,
                    config,
                    "sink_status",
                    Arrays.asList(Arrays.asList(2L, "active"), Arrays.asList(3L, "active")));
        }
    }

    /**
     * The same as {@link #testWhereRejectedUpdateAsDeleteWithSchemaKey}, with a source schema
     * without a primary key and the key set only by {@code primary_keys} on the sink.
     */
    @TestTemplate
    public void testWhereRejectedUpdateAsDeleteWithSinkKey(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        for (String config :
                Arrays.asList(
                        "/jdbc_sink_cdc_changelog_sql_where_sink_key.conf",
                        "/jdbc_sink_cdc_changelog_calcite_where_sink_key.conf")) {
            assertWhereOnCDCChangelog(
                    container,
                    config,
                    "sink_status",
                    Arrays.asList(Arrays.asList(2L, "active"), Arrays.asList(3L, "active")));
        }
    }

    /**
     * A keyless append-only Jdbc sink with the default {@code where_rejected_update_as_delete =
     * false}: the WHERE clause drops every row that does not match, as before, so the rejected
     * UPDATE_AFTER of row 1 is not appended as a new row. The UPDATE_BEFORE of row 1 matches and is
     * appended like any other row, as before.
     */
    @TestTemplate
    public void testWhereOnCDCChangelogKeylessByDefault(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        for (String config :
                Arrays.asList(
                        "/jdbc_sink_cdc_changelog_sql_where_keyless.conf",
                        "/jdbc_sink_cdc_changelog_calcite_where_keyless.conf")) {
            assertWhereOnCDCChangelog(
                    container,
                    config,
                    "sink_status_append",
                    Arrays.asList(
                            Arrays.asList(1L, "active"),
                            Arrays.asList(1L, "active"),
                            Arrays.asList(2L, "active"),
                            Arrays.asList(3L, "active")));
        }
    }

    private void assertWhereOnCDCChangelog(
            TestContainer container, String config, String table, List<List<Object>> expected)
            throws IOException, InterruptedException, SQLException {
        executeSql("truncate table " + table);
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
                                    "select pk_id, status from "
                                            + table
                                            + " order by pk_id, status")) {
                while (resultSet.next()) {
                    actual.add(
                            Arrays.asList(
                                    resultSet.getLong("pk_id"), resultSet.getString("status")));
                }
            }
            Assertions.assertEquals(expected, actual, config);
        } finally {
            executeSql("truncate table " + table);
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

    /**
     * A table with only a UNIQUE key: the sink upserts by that key without {@code primary_keys},
     * and with {@code multi_table_sink_replica = 2} the changes of one key must stay in order, so
     * the table ends with the last version of each key and without the deleted keys.
     */
    @TestTemplate
    public void testUniqueKeyTableKeepsTheLastChangeOfEachKeyWithReplicas(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        // Each engine runs this against the same table, so a failed run must not leave rows for
        // the next one: empty it before the job and again when the test ends either way.
        truncateSinkUnique();
        try {
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_sink_cdc_changelog_unique_key_replicas.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

            Set<List<Object>> actual = new HashSet<>();
            try (Connection connection =
                            DriverManager.getConnection(
                                    postgreSQLContainer.getJdbcUrl(),
                                    postgreSQLContainer.getUsername(),
                                    postgreSQLContainer.getPassword());
                    Statement statement = connection.createStatement();
                    ResultSet resultSet =
                            statement.executeQuery("select code, val from sink_unique")) {
                while (resultSet.next()) {
                    actual.add(
                            Arrays.asList(resultSet.getLong("code"), resultSet.getString("val")));
                }
            }
            // Every key is inserted as v1 and updated to v2; every even key is then deleted.
            Set<List<Object>> expected = new HashSet<>();
            for (long code = 1; code <= 100; code += 2) {
                expected.add(Arrays.asList(code, "v2"));
            }
            Assertions.assertEquals(expected, actual);
        } finally {
            truncateSinkUnique();
        }
    }

    private void truncateSinkUnique() throws SQLException {
        try (Connection connection =
                        DriverManager.getConnection(
                                postgreSQLContainer.getJdbcUrl(),
                                postgreSQLContainer.getUsername(),
                                postgreSQLContainer.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("truncate table sink_unique");
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
            statement.execute(
                    "create table if not exists sink_status_append(\n"
                            + "pk_id BIGINT NOT NULL,\n"
                            + "status varchar(255)\n"
                            + ")");
            statement.execute(
                    "create table sink_unique(\n"
                            + "code BIGINT NOT NULL UNIQUE,\n"
                            + "val varchar(255)\n"
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
