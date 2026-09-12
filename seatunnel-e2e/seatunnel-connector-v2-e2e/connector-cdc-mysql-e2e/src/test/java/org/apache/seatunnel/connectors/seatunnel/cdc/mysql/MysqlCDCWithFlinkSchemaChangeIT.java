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
import org.apache.seatunnel.e2e.common.util.DependencyJar;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.SEATUNNEL},
        disabledReason =
                "Currently SPARK do not support cdc, only test the change process related to Flink.")
public class MysqlCDCWithFlinkSchemaChangeIT extends TestSuiteBase implements TestResource {
    /**
     * Flink schema evolution can restart after XA recover/rollback on loaded CI runners, so these
     * assertions need enough time for the job to recover and replay the schema-change event. The
     * long bound is a failure deadline, not the expected recovery duration.
     */
    private static final long SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS = 600_000L;

    private static final long STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS = 300_000L;
    /**
     * The timestamp default is evaluated by different MySQL statements during CDC replay, so this
     * tolerance allows loaded CI scheduling jitter without hiding minute-level CDC lag.
     */
    private static final int MAX_TIMESTAMP_DRIFT_SECONDS = 45;

    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_with_schema_change";
    private static final String SINK_TABLE2 =
            "mysql_cdc_e2e_sink_table_with_schema_change_exactly_once";
    private static final String SINK_TABLE_IGNORE =
            "mysql_cdc_e2e_sink_table_with_schema_change_ignore";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_SOURCE_USER_NAME = "st_user_source";

    private static final String DESC = "desc %s.%s";
    private static final String QUERY = "select * from %s.%s";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s order by id;";

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

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    @Order(1)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCase(TestContainer container) {
        // Reset database to initial state to avoid issues from previous test runs
        resetDatabaseToInitialState();

        String jobConfigFile = "/mysqlcdc_to_mysql_with_flink_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // waiting for case1 completed
        assertSchemaEvolutionForAddColumns(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case2 drop columns with cdc data at same time
        shopDatabase.setTemplateName("drop_columns").createAndInitialize();

        // waiting for case2 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case3 change column name with cdc data at same time
        shopDatabase.setTemplateName("change_columns").createAndInitialize();

        // case4 modify column data type with cdc data at same time
        shopDatabase.setTemplateName("modify_columns").createAndInitialize();

        // waiting for case3/case4 completed
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // case5 comment changes with cdc data at same time
        shopDatabase.setTemplateName("comment_changes").createAndInitialize();

        // waiting for case5 completed – data must continue flowing after comment DDL
        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        // verify the final source table comment was applied (tests comment update idempotency)
        assertSourceTableComment(
                MYSQL_DATABASE, SOURCE_TABLE, "Updated product catalog with sports equipment");
    }

    @Order(2)
    @TestTemplate
    public void testMysqlCdcWithSchemaEvolutionCaseExactlyOnce(TestContainer container) {

        shopDatabase.setTemplateName("shop").createAndInitialize();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/mysqlcdc_to_mysql_with_flink_schema_change_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        assertSchemaEvolution(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE2);
    }

    @Order(3)
    @TestTemplate
    public void testStrictSchemaChangeBehaviorFailsOnSchemaChange(TestContainer container) {
        resetDatabaseToInitialState();
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () ->
                                executeJob(
                                        container,
                                        "/mysqlcdc_to_mysql_with_flink_schema_change_strict.conf"));

        awaitBinlogReaderStarted(jobFuture);
        if (!jobFuture.isDone()) {
            shopDatabase.setTemplateName("add_columns").createAndInitialize();
        }

        Container.ExecResult result = awaitJobFinished(jobFuture);
        Assertions.assertNotEquals(
                0,
                result.getExitCode(),
                "strict schema change behavior should fail when a schema change event is observed");
        Assertions.assertTrue(
                result.getStderr().contains("Schema change behavior is STRICT")
                        || result.getStdout().contains("Schema change behavior is STRICT"),
                "strict failure should report the schema change behavior contract");
    }

    @Order(4)
    @TestTemplate
    public void testIgnoreSchemaChangeBehaviorDropsCommentOnlySchemaChange(
            TestContainer container) {
        resetDatabaseToInitialState();
        CompletableFuture.runAsync(
                () ->
                        executeJob(
                                container,
                                "/mysqlcdc_to_mysql_with_flink_schema_change_ignore.conf"));

        awaitInitialSnapshot(SINK_TABLE_IGNORE);
        shopDatabase.setTemplateName("comment_only_changes").createAndInitialize();

        assertTableStructureAndData(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE_IGNORE);
        assertSourceTableComment(
                MYSQL_DATABASE, SOURCE_TABLE, "Ignored product catalog comment updated");
    }

    private void assertSchemaEvolution(String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertTableDataEqualsBySourceColumnOrder(
                                    database, sourceTable, sinkTable, "id >= 128");

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp(), so the
                            // history data of the sink table can lag briefly behind the source
                            // table while the schema-change event is still propagating.
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

        // case5 comment changes with cdc data at same time
        assertCaseByDdlName("comment_changes", database, sourceTable, sinkTable);
        assertSourceTableComment(
                database, sourceTable, "Updated product catalog with sports equipment");
    }

    private void assertCaseByDdlName(
            String drop_columns, String database, String sourceTable, String sinkTable) {
        shopDatabase.setTemplateName(drop_columns).createAndInitialize();
        assertTableStructureAndData(database, sourceTable, sinkTable);
    }

    /**
     * Flink JDBC schema evolution can materialize columns in a different physical order even when
     * the effective schema matches, so normalize DESCRIBE output by column name before asserting.
     */
    private void assertSchemaDescriptionEqualsIgnoringColumnOrder(
            String database, String sourceTable, String sinkTable) {
        Assertions.assertIterableEquals(
                normalizeDescRows(query(String.format(DESC, database, sourceTable))),
                normalizeDescRows(query(String.format(DESC, database, sinkTable))));
    }

    private void assertSchemaEvolutionForAddColumns(
            String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));

        // case1 add columns with cdc data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        await().pollInterval(5, TimeUnit.SECONDS)
                .atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().pollInterval(5, TimeUnit.SECONDS)
                .atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertTableDataEqualsBySourceColumnOrder(
                                    database, sourceTable, sinkTable, "id >= 128");

                            Assertions.assertIterableEquals(
                                    query(String.format(PROJECTION_QUERY, database, sourceTable)),
                                    query(String.format(PROJECTION_QUERY, database, sinkTable)));

                            // The default value of add_column4 is current_timestamp(), so the
                            // history data of the sink table can lag briefly behind the source
                            // table while the schema-change event is still propagating.
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
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        database, sourceTable, sinkTable));
        await().atMost(STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));
    }

    private void awaitInitialSnapshot(String sinkTable) {
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(String.format(QUERY, MYSQL_DATABASE, SOURCE_TABLE)),
                                        query(String.format(QUERY, MYSQL_DATABASE, sinkTable))));
    }

    private void awaitBinlogReaderStarted(CompletableFuture<Container.ExecResult> jobFuture) {
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .until(() -> jobFuture.isDone() || binlogReaderStarted());
    }

    private boolean binlogReaderStarted() {
        try {
            return query(
                            "select COMMAND from information_schema.processlist where USER = '"
                                    + MYSQL_SOURCE_USER_NAME
                                    + "'")
                    .stream()
                    .flatMap(List::stream)
                    .filter(command -> command != null)
                    .map(command -> command.toString().toLowerCase(Locale.ROOT))
                    .anyMatch(command -> command.contains("binlog dump"));
        } catch (RuntimeException e) {
            return false;
        }
    }

    private Container.ExecResult executeJob(TestContainer container, String jobConfigFile) {
        try {
            return container.executeJob(jobConfigFile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Container.ExecResult awaitJobFinished(
            CompletableFuture<Container.ExecResult> jobFuture) {
        try {
            return jobFuture.get(180, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuntimeException(
                    "Timed out waiting for schema change policy job to finish", e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    /**
     * Read both tables using the source column order so data assertions stay stable when the sink
     * keeps equivalent columns but stores them in a different physical position.
     */
    private void assertTableDataEqualsBySourceColumnOrder(
            String database, String sourceTable, String sinkTable, String whereClause) {
        List<String> sourceColumns = getColumnNames(database, sourceTable);
        Assertions.assertIterableEquals(
                query(
                        buildOrderedProjectionQuery(
                                database, sourceTable, sourceColumns, whereClause)),
                query(
                        buildOrderedProjectionQuery(
                                database, sinkTable, sourceColumns, whereClause)));
    }

    /**
     * Returns source column names from DESCRIBE so later projections follow the semantic schema.
     */
    private List<String> getColumnNames(String database, String table) {
        List<String> columnNames = new ArrayList<>();
        for (List<Object> row : query(String.format(DESC, database, table))) {
            columnNames.add(String.valueOf(row.get(0)));
        }
        return columnNames;
    }

    /** Builds an explicit projection to avoid relying on engine-specific physical column order. */
    private String buildOrderedProjectionQuery(
            String database, String table, List<String> columns, String whereClause) {
        StringBuilder queryBuilder =
                new StringBuilder("select ")
                        .append(
                                columns.stream()
                                        .map(this::quoteIdentifier)
                                        .collect(Collectors.joining(",")))
                        .append(" from ")
                        .append(quoteIdentifier(database))
                        .append(".")
                        .append(quoteIdentifier(table));
        if (whereClause != null && !whereClause.isEmpty()) {
            queryBuilder.append(" where ").append(whereClause);
        }
        return queryBuilder.append(" order by id").toString();
    }

    /** Quotes identifiers because schema-change cases rename and reposition columns dynamically. */
    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /** Sorts schema rows by column name so the assertion ignores physical column placement only. */
    private List<List<Object>> normalizeDescRows(List<List<Object>> descRows) {
        List<List<Object>> normalizedRows = new ArrayList<>(descRows);
        normalizedRows.sort(Comparator.comparing(row -> String.valueOf(row.get(0))));
        return normalizedRows;
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

    private void resetDatabaseToInitialState() {
        try {
            log.info("Resetting database to initial state...");
            // Reset to original template and recreate database
            shopDatabase.setTemplateName(MYSQL_DATABASE).createAndInitialize();
            log.info("Database reset to initial state completed");
        } catch (Exception e) {
            log.error("Failed to reset database to initial state", e);
            throw new RuntimeException("Failed to reset database to initial state", e);
        }
    }

    /**
     * Asserts that the given source table's TABLE_COMMENT in {@code information_schema.TABLES}
     * matches {@code expectedComment}. This check is performed directly against the source MySQL
     * instance; it is intentionally not compared with the sink because the JDBC sink treats comment
     * events as no-ops.
     */
    private void assertSourceTableComment(String database, String table, String expectedComment) {
        String sql =
                String.format(
                        "SELECT TABLE_COMMENT FROM information_schema.TABLES"
                                + " WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        database, table);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            Assertions.assertTrue(rs.next(), "Table " + table + " not found in information_schema");
            Assertions.assertEquals(
                    expectedComment,
                    rs.getString("TABLE_COMMENT"),
                    "Source table comment should have been updated by the ALTER TABLE COMMENT DDL");
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
