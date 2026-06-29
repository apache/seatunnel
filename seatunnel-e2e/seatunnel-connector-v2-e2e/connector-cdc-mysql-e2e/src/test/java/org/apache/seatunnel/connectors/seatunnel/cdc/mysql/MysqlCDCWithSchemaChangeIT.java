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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public class MysqlCDCWithSchemaChangeIT extends TestSuiteBase implements TestResource {
    /**
     * The zeta schema-evolution path applies DDL and follow-up CDC records more slowly than a local
     * MySQL/MySQL comparison, especially on loaded CI runners.
     */
    private static final long SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS = 180_000L;

    private static final long STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS = 300_000L;
    private static final int MAX_TIMESTAMP_DRIFT_SECONDS = 60;

    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_with_schema_change";
    private static final String SINK_TABLE2 =
            "mysql_cdc_e2e_sink_table_with_schema_change_exactly_once";
    private static final String SINK_TABLE_FILTER = "mysql_cdc_e2e_sink_table_schema_change_filter";
    private static final String STABLE_QUERY =
            "select id,name,description,weight from %s.%s order by id";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String QUERY = "select * from %s.%s";
    private static final String DESC = "desc %s.%s";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s;";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

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

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @Order(1)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCase(TestContainer container)
            throws IOException, InterruptedException {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/mysqlcdc_to_mysql_with_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // waiting for case1 completed
        assertSchemaEvolutionForAddColumns(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // savepoint 1
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // case2 drop columns with cdc data at same time
        shopDatabase.setTemplateName("drop_columns").createAndInitialize();

        // restore 1
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // waiting for case2 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // savepoint 2
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // case3 change column name with cdc data at same time
        shopDatabase.setTemplateName("change_columns").createAndInitialize();

        // case4 modify column data type with cdc data at same time
        shopDatabase.setTemplateName("modify_columns").createAndInitialize();

        // restore 2
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.restoreJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        // waiting for case3/case4 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);
    }

    @Order(2)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCaseExactlyOnce(TestContainer container) {

        shopDatabase.setTemplateName("shop").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_schema_change_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        assertSchemaEvolution(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE2);
    }

    /**
     * Regression for issue #11044. With {@code schema-changes.exclude = ["drop.column"]}: a dropped
     * column must NOT propagate to the sink (it stays in the sink schema), and the data changes
     * happening at the same time must still reach the sink.
     *
     * <p>The dropped column here is intentionally <b>NULLABLE</b>. #11044 is event-type filtering
     * only and, per its non-goals, does not define a schema-change data-handling policy, so the
     * source simply writes {@code null} for a retained-but-no-longer-supplied column — valid for a
     * nullable column. Excluding {@code drop.column} for a NOT NULL column is a known limitation
     * that fails at the sink and is deferred to a future behavior-policy feature; see MySQL-CDC.md.
     * Using a nullable column is exactly why this test needs its own {@code *_filter} DDL templates
     * instead of the shared {@code add_columns}/{@code drop_columns} (which add/drop NOT NULL
     * columns).
     */
    @Order(3)
    @TestTemplate
    public void testMysqlCdcSchemaChangeEventTypeFilter(TestContainer container) {
        shopDatabase.setTemplateName("shop").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob("/mysqlcdc_to_mysql_with_schema_change_filter.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // initial snapshot synced
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(
                                                        STABLE_QUERY,
                                                        MYSQL_DATABASE,
                                                        SOURCE_TABLE)),
                                        query(
                                                String.format(
                                                        STABLE_QUERY,
                                                        MYSQL_DATABASE,
                                                        SINK_TABLE_FILTER))));

        // add.column is NOT excluded
        shopDatabase.setTemplateName("add_columns_filter").createAndInitialize();
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        columnExists(
                                                MYSQL_DATABASE, SINK_TABLE_FILTER, "add_column1"),
                                        "add.column should propagate to the sink"));

        // drop.column IS excluded; this template also inserts/updates/deletes rows at the same time
        shopDatabase.setTemplateName("drop_columns_filter").createAndInitialize();

        // regression: the concurrent data changes must still reach the sink (job did not crash)
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(
                                                        STABLE_QUERY,
                                                        MYSQL_DATABASE,
                                                        SOURCE_TABLE)),
                                        query(
                                                String.format(
                                                        STABLE_QUERY,
                                                        MYSQL_DATABASE,
                                                        SINK_TABLE_FILTER))));

        // Row-level hardening: drop_columns_filter.sql performs INSERT + UPDATE + DELETE alongside
        // the filtered drop.column DDL. Every one of those data changes must be reflected in the
        // sink (the earlier iterable-equals already converged, so these reads are stable).
        List<List<Object>> sourceRows =
                query(String.format(STABLE_QUERY, MYSQL_DATABASE, SOURCE_TABLE));
        List<List<Object>> sinkRows =
                query(String.format(STABLE_QUERY, MYSQL_DATABASE, SINK_TABLE_FILTER));
        Assertions.assertEquals(
                sourceRows.size(),
                sinkRows.size(),
                "sink row count must match source after the concurrent INSERT/UPDATE/DELETE");
        // DELETE propagated: drop_columns_filter.sql runs `delete from products where id = 102`.
        Assertions.assertTrue(
                sinkRows.stream().noneMatch(row -> ((Number) row.get(0)).intValue() == 102),
                "rows deleted at the source must also be deleted in the sink");
        // INSERT propagated: id 110 is inserted by drop_columns_filter.sql.
        Assertions.assertTrue(
                sinkRows.stream().anyMatch(row -> ((Number) row.get(0)).intValue() == 110),
                "rows inserted at the source must appear in the sink");
        // UPDATE propagated: `set name='dailai' where id = 101`.
        assertSinkNameEquals(sinkRows, 101, "dailai");

        // the excluded drop.column must NOT have been applied to the sink schema
        Assertions.assertTrue(
                columnExists(MYSQL_DATABASE, SINK_TABLE_FILTER, "add_column1"),
                "drop.column was excluded, so the sink must keep the column the source dropped");
    }

    /** Asserts the sink row with the given id exists and its {@code name} matches expectedName. */
    private void assertSinkNameEquals(List<List<Object>> sinkRows, int id, Object expectedName) {
        Optional<List<Object>> row =
                sinkRows.stream().filter(r -> ((Number) r.get(0)).intValue() == id).findFirst();
        Assertions.assertTrue(row.isPresent(), "expected sink row with id=" + id);
        Assertions.assertEquals(
                expectedName,
                row.get().get(1),
                "updated value must propagate to the sink for id=" + id);
    }

    private boolean columnExists(String database, String table, String column) {
        return query(String.format(DESC, database, table)).stream()
                .anyMatch(row -> column.equalsIgnoreCase(String.valueOf(row.get(0))));
    }

    private void assertSchemaEvolution(String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(QUERY, database, sourceTable)),
                                        query(String.format(QUERY, database, sinkTable))));

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(DESC, database, sourceTable)),
                                        query(String.format(DESC, database, sinkTable))));
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(QUERY, database, sourceTable)
                                                    + " where id >= 128"),
                                    query(
                                            String.format(QUERY, database, sinkTable)
                                                    + " where id >= 128"));

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp()，so the
                            // history data of sink table with this column may be different from the
                            // source table because delay of apply schema change.
                            String query =
                                    String.format(
                                            "SELECT t1.id AS table1_id, t1.add_column4 AS table1_timestamp, "
                                                    + "t2.id AS table2_id, t2.add_column4 AS table2_timestamp, "
                                                    + "ABS(TIMESTAMPDIFF(SECOND, t1.add_column4, t2.add_column4)) AS time_diff "
                                                    + "FROM %s.%s t1 "
                                                    + "INNER JOIN %s.%s t2 ON t1.id = t2.id",
                                            database, sourceTable, database, sinkTable);
                            try (Connection jdbcConnection = getJdbcConnection();
                                    Statement statement = jdbcConnection.createStatement();
                                    ResultSet resultSet = statement.executeQuery(query); ) {
                                while (resultSet.next()) {
                                    int timeDiff = resultSet.getInt("time_diff");
                                    Assertions.assertTrue(
                                            timeDiff <= MAX_TIMESTAMP_DRIFT_SECONDS,
                                            "Time difference exceeds "
                                                    + MAX_TIMESTAMP_DRIFT_SECONDS
                                                    + " seconds: "
                                                    + timeDiff
                                                    + " seconds");
                                }
                            }
                        });

        // case2 drop columns with cdc data at same time
        assertCaseByDdlName("drop_columns", database, sourceTable, sinkTable);

        // case3 change column name with cdc data at same time
        assertCaseByDdlName("change_columns", database, sourceTable, sinkTable);

        // case4 modify column data type with cdc data at same time
        assertCaseByDdlName("modify_columns", database, sourceTable, sinkTable);
    }

    private void assertCaseByDdlName(
            String drop_columns, String database, String sourceTable, String sinkTable) {
        shopDatabase.setTemplateName(drop_columns).createAndInitialize();
        assertTableStructureAndData(database, sourceTable, sinkTable);
    }

    private void assertSchemaEvolutionForAddColumns(
            String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(QUERY, database, sourceTable)),
                                        query(String.format(QUERY, database, sinkTable))));

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(DESC, database, sourceTable)),
                                        query(String.format(DESC, database, sinkTable))));
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    query(
                                            String.format(QUERY, database, sourceTable)
                                                    + " where id >= 128"),
                                    query(
                                            String.format(QUERY, database, sinkTable)
                                                    + " where id >= 128"));

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp()，so the
                            // history data of sink table with this column may be different from the
                            // source table because delay of apply schema change.
                            String query =
                                    String.format(
                                            "SELECT t1.id AS table1_id, t1.add_column4 AS table1_timestamp, "
                                                    + "t2.id AS table2_id, t2.add_column4 AS table2_timestamp, "
                                                    + "ABS(TIMESTAMPDIFF(SECOND, t1.add_column4, t2.add_column4)) AS time_diff "
                                                    + "FROM %s.%s t1 "
                                                    + "INNER JOIN %s.%s t2 ON t1.id = t2.id",
                                            database, sourceTable, database, sinkTable);
                            try (Connection jdbcConnection = getJdbcConnection();
                                    Statement statement = jdbcConnection.createStatement();
                                    ResultSet resultSet = statement.executeQuery(query); ) {
                                while (resultSet.next()) {
                                    int timeDiff = resultSet.getInt("time_diff");
                                    Assertions.assertTrue(
                                            timeDiff <= MAX_TIMESTAMP_DRIFT_SECONDS,
                                            "Time difference exceeds "
                                                    + MAX_TIMESTAMP_DRIFT_SECONDS
                                                    + " seconds: "
                                                    + timeDiff
                                                    + " seconds");
                                }
                            }
                        });
    }

    private void assertTableStructureAndData(
            String database, String sourceTable, String sinkTable) {
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(DESC, database, sourceTable)),
                                        query(String.format(DESC, database, sinkTable))));
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(QUERY, database, sourceTable)),
                                        query(String.format(QUERY, database, sinkTable))));
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        shopDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print MySQL-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
