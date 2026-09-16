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
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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

    /** Covers the longer convergence window for DDL plus data assertions in CI. */
    private static final long STRUCTURE_AND_DATA_ASSERT_TIMEOUT_MILLIS = 300_000L;

    /** Allows timestamp columns to drift slightly while source and sink are converging in CI. */
    private static final int MAX_TIMESTAMP_DRIFT_SECONDS = 60;

    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_with_schema_change";
    private static final String SINK_TABLE2 =
            "mysql_cdc_e2e_sink_table_with_schema_change_exactly_once";
    /** Dedicated sink table used by the event-type filter regression coverage. */
    private static final String SINK_TABLE_FILTER = "mysql_cdc_e2e_sink_table_schema_change_filter";

    /** Stable projection used after add-column evolution to compare source and sink rows. */
    private static final String STABLE_QUERY =
            "select id,name,description,weight from %s.%s order by id";

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String QUERY = "select * from %s.%s";
    private static final String DESC = "desc %s.%s";
    private static final String PROJECTION_QUERY =
            "select id,name,description,weight,add_column1,add_column2,add_column3 from %s.%s;";
    /** Source table reused by both source databases in the multi-database schema-evolution case. */
    private static final String MULTI_DB_SAME_NAME_TABLE = "products";
    /** First source database for the multi-database schema-evolution routing scenario. */
    private static final String MULTI_DB_SOURCE_DATABASE_A = "multi_schema_shop_a";
    /** Second source database for the multi-database schema-evolution routing scenario. */
    private static final String MULTI_DB_SOURCE_DATABASE_B = "multi_schema_shop_b";
    /** First sink database that receives the table from {@link #MULTI_DB_SOURCE_DATABASE_A}. */
    private static final String MULTI_DB_SINK_DATABASE_A = "multi_schema_shop_a_sink";
    /** Second sink database that receives the table from {@link #MULTI_DB_SOURCE_DATABASE_B}. */
    private static final String MULTI_DB_SINK_DATABASE_B = "multi_schema_shop_b_sink";
    /** Job config used to validate schema evolution for same-name tables across databases. */
    private static final String MULTI_DB_SCHEMA_CHANGE_JOB_CONFIG =
            "/mysqlcdc_to_mysql_with_multi_db_same_name_schema_change.conf";
    /** SQL template that resets the multi-database schema-evolution fixture. */
    private static final String MULTI_DB_SCHEMA_CHANGE_INIT_TEMPLATE =
            "multi_db_same_name_schema_change";
    /** SQL template that triggers add-column DDL and post-DDL DML on both source databases. */
    private static final String MULTI_DB_SCHEMA_CHANGE_ADD_COLUMNS_TEMPLATE =
            "multi_db_same_name_add_columns";
    /** Strips inline SQL comments when a single template manages multiple databases at once. */
    private static final Pattern INLINE_SQL_COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    /**
     * Marker emitted once the CDC reader leaves the snapshot phase and starts reading binlog events
     * for the captured tables.
     */
    private static final String INCREMENTAL_READ_MARKER =
            "Start incremental read task for incremental split";

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
        assertSchemaEvolutionForAddColumns(container, MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

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

        // savepoint 3
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // case5 table comment change with cdc data at same time
        shopDatabase.setTemplateName("comment_changes").createAndInitialize();

        // restore 3
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
                                "/mysqlcdc_to_mysql_with_schema_change_exactly_once.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        assertSchemaEvolution(container, MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE2);
    }

    /** Default wait used while polling the shared-name multi-database source tables. */
    private static final long DEFAULT_TABLE_SYNC_TIMEOUT_MS = 60000L;

    /** Extended wait for the downstream tables after the second source database starts syncing. */
    private static final long MULTI_DB_TABLE_SYNC_TIMEOUT_MS = 180000L;

    /**
     * Verifies that two upstream databases can expose the same table name, evolve that schema, and
     * still land in different downstream databases without mixing data or DDL.
     */
    @Order(4)
    @TestTemplate
    public void testMysqlCdcWithMultiDatabaseSameTableSchemaEvolution(TestContainer container) {
        executeSqlTemplate(MULTI_DB_SCHEMA_CHANGE_INIT_TEMPLATE);
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(MULTI_DB_SCHEMA_CHANGE_JOB_CONFIG);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // Once the job submission succeeds, both sink tables must first catch up with their
        // matching source tables before the DDL phase starts. This keeps the DDL and later DML
        // assertions focused on post-snapshot schema evolution instead of snapshot warm-up lag.
        assertTableStructureAndData(
                MULTI_DB_SOURCE_DATABASE_A,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_SINK_DATABASE_A,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_TABLE_SYNC_TIMEOUT_MS);
        assertTableStructureAndData(
                MULTI_DB_SOURCE_DATABASE_B,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_SINK_DATABASE_B,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_TABLE_SYNC_TIMEOUT_MS);

        // The DDL phase must start only after the CDC reader switches from snapshot discovery to
        // binlog consumption for both same-name source tables.
        waitForIncrementalRead(
                container,
                MULTI_DB_TABLE_SYNC_TIMEOUT_MS,
                MULTI_DB_SOURCE_DATABASE_A + "." + MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_SOURCE_DATABASE_B + "." + MULTI_DB_SAME_NAME_TABLE);

        executeSqlTemplate(MULTI_DB_SCHEMA_CHANGE_ADD_COLUMNS_TEMPLATE);

        assertTableStructureAndData(
                MULTI_DB_SOURCE_DATABASE_A,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_SINK_DATABASE_A,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_TABLE_SYNC_TIMEOUT_MS);
        assertTableStructureAndData(
                MULTI_DB_SOURCE_DATABASE_B,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_SINK_DATABASE_B,
                MULTI_DB_SAME_NAME_TABLE,
                MULTI_DB_TABLE_SYNC_TIMEOUT_MS);
    }

    /**
     * Regression for issue #11044. With {@code schema-changes.exclude = ["drop.column"]}: a dropped
     * column must NOT propagate to the sink, and the concurrent data changes must still reach the
     * sink.
     *
     * <p>The dropped column here is intentionally <b>NULLABLE</b>. #11044 is event-type filtering
     * only and does not define a schema-change data-handling policy, so the source writes {@code
     * null} for a retained-but-no-longer-supplied column. Excluding {@code drop.column} for a NOT
     * NULL column is a known limitation deferred to a future behavior-policy feature.
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

        // regression: the concurrent data changes must still reach the sink
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

        // Row-level hardening: every INSERT, UPDATE, and DELETE must be reflected in the sink.
        List<List<Object>> sourceRows =
                query(String.format(STABLE_QUERY, MYSQL_DATABASE, SOURCE_TABLE));
        List<List<Object>> sinkRows =
                query(String.format(STABLE_QUERY, MYSQL_DATABASE, SINK_TABLE_FILTER));
        Assertions.assertEquals(
                sourceRows.size(),
                sinkRows.size(),
                "sink row count must match source after the concurrent INSERT/UPDATE/DELETE");
        Assertions.assertTrue(
                sinkRows.stream().noneMatch(row -> ((Number) row.get(0)).intValue() == 102),
                "rows deleted at the source must also be deleted in the sink");
        Assertions.assertTrue(
                sinkRows.stream().anyMatch(row -> ((Number) row.get(0)).intValue() == 110),
                "rows inserted at the source must appear in the sink");
        assertSinkNameEquals(sinkRows, 101, "dailai");

        // the excluded drop.column must NOT have been applied to the sink schema
        Assertions.assertTrue(
                columnExists(MYSQL_DATABASE, SINK_TABLE_FILTER, "add_column1"),
                "drop.column was excluded, so the sink must keep the column the source dropped");
    }

    // Asserts the sink row with the given id exists and its name matches expectedName.
    private void assertSinkNameEquals(List<List<Object>> sinkRows, int id, Object expectedName) {
        Optional<List<Object>> row =
                sinkRows.stream().filter(r -> ((Number) r.get(0)).intValue() == id).findFirst();
        Assertions.assertTrue(row.isPresent(), "expected sink row with id=" + id);
        Assertions.assertEquals(
                expectedName,
                row.get().get(1),
                "updated value must propagate to the sink for id=" + id);
    }

    /**
     * Checks whether a column remains present in the sink schema after filtered schema evolution.
     */
    private boolean columnExists(String database, String table, String column) {
        return query(String.format(DESC, database, table)).stream()
                .anyMatch(row -> column.equalsIgnoreCase(String.valueOf(row.get(0))));
    }

    private void assertSchemaEvolution(
            TestContainer container, String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));

        // case1 add columns with cdc data at same time
        waitForIncrementalRead(container, database + "." + sourceTable);
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

    private void assertSchemaEvolutionForAddColumns(
            TestContainer container, String database, String sourceTable, String sinkTable) {
        await().atMost(SCHEMA_EVOLUTION_ASSERT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        database, sourceTable, sinkTable, null));

        // case1 add columns with cdc data at same time
        waitForIncrementalRead(container, database + "." + sourceTable);
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
        assertTableStructureAndData(
                database, sourceTable, database, sinkTable, DEFAULT_TABLE_SYNC_TIMEOUT_MS);
    }

    /** Waits until the sink table matches the source table for both structure and data. */
    private void assertTableStructureAndData(
            String sourceDatabase, String sourceTable, String sinkDatabase, String sinkTable) {
        assertTableStructureAndData(
                sourceDatabase,
                sourceTable,
                sinkDatabase,
                sinkTable,
                DEFAULT_TABLE_SYNC_TIMEOUT_MS);
    }

    /**
     * Waits until the sink table matches the source table for both structure and data with a
     * scenario-specific timeout. Larger multi-table snapshots can need a longer warm-up window
     * before the first rows arrive in CI.
     */
    private void assertTableStructureAndData(
            String sourceDatabase,
            String sourceTable,
            String sinkDatabase,
            String sinkTable,
            long timeoutMs) {
        await().atMost(timeoutMs, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertSchemaDescriptionEqualsIgnoringColumnOrder(
                                        sourceDatabase, sourceTable, sinkDatabase, sinkTable));
        await().atMost(timeoutMs, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertTableDataEqualsBySourceColumnOrder(
                                        sourceDatabase,
                                        sourceTable,
                                        sinkDatabase,
                                        sinkTable,
                                        null));
    }

    /**
     * Waits until the CDC reader has switched to binlog consumption before the test emits DDL.
     * Otherwise schema-change statements can be produced while the job is still finishing the
     * snapshot phase, which makes slow environments miss the intended event ordering.
     */
    private void waitForIncrementalRead(TestContainer container, String... capturedTables) {
        waitForIncrementalRead(container, DEFAULT_TABLE_SYNC_TIMEOUT_MS, capturedTables);
    }

    /**
     * Waits until the CDC reader has switched to binlog consumption with a scenario-specific
     * timeout. Multi-database snapshot jobs can spend noticeably longer in initial split discovery
     * before the incremental-reader marker appears.
     */
    private void waitForIncrementalRead(
            TestContainer container, long timeoutMs, String... capturedTables) {
        await().atMost(timeoutMs, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String serverLogs = container.getServerLogs();
                            Assertions.assertTrue(
                                    serverLogs.contains(INCREMENTAL_READ_MARKER),
                                    "Incremental reader has not started yet");
                            for (String capturedTable : capturedTables) {
                                Assertions.assertTrue(
                                        serverLogs.contains(capturedTable),
                                        "Incremental reader has not started for "
                                                + capturedTable
                                                + "\nCurrent logs:\n"
                                                + serverLogs);
                            }
                        });
    }

    /** Executes a fixed SQL template that prepares or mutates multiple databases in one step. */
    private void executeSqlTemplate(String templateName) {
        String ddlFile = String.format("ddl/%s.sql", templateName);
        URL ddlTemplate = MysqlCDCWithSchemaChangeIT.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTemplate, "Cannot locate " + ddlFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (String sql : loadSqlStatements(ddlTemplate)) {
                statement.execute(sql);
                log.info(sql);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Converts a classpath SQL template into executable statements while preserving custom
     * delimiters.
     */
    private List<String> loadSqlStatements(URL ddlTemplate) throws Exception {
        return Arrays.stream(
                        Files.readAllLines(Paths.get(ddlTemplate.toURI())).stream()
                                .map(String::trim)
                                .filter(line -> !line.startsWith("--") && !line.isEmpty())
                                .map(this::stripInlineComment)
                                .collect(Collectors.joining("\n"))
                                .split(";"))
                .map(sql -> sql.replace("$$", ";"))
                .map(String::trim)
                .filter(sql -> !sql.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Removes end-of-line SQL comments so that the template executor can split statements safely.
     */
    private String stripInlineComment(String sql) {
        Matcher matcher = INLINE_SQL_COMMENT_PATTERN.matcher(sql);
        return matcher.matches() ? matcher.group(1).trim() : sql;
    }

    /**
     * Zeta can apply equivalent schema changes with a different physical column placement, so we
     * compare DESCRIBE output by column name rather than raw row order.
     */
    private void assertSchemaDescriptionEqualsIgnoringColumnOrder(
            String database, String sourceTable, String sinkTable) {
        assertSchemaDescriptionEqualsIgnoringColumnOrder(
                database, sourceTable, database, sinkTable);
    }

    /**
     * Compares source and sink schemas across databases without depending on physical column
     * placement.
     */
    private void assertSchemaDescriptionEqualsIgnoringColumnOrder(
            String sourceDatabase, String sourceTable, String sinkDatabase, String sinkTable) {
        Assertions.assertIterableEquals(
                normalizeDescRows(query(String.format(DESC, sourceDatabase, sourceTable))),
                normalizeDescRows(query(String.format(DESC, sinkDatabase, sinkTable))));
    }

    /**
     * Reads the sink using the source column order so data assertions remain stable when sink DDL
     * keeps the same columns but materializes them in a different physical order.
     */
    private void assertTableDataEqualsBySourceColumnOrder(
            String database, String sourceTable, String sinkTable, String whereClause) {
        assertTableDataEqualsBySourceColumnOrder(
                database, sourceTable, database, sinkTable, whereClause);
    }

    /**
     * Compares source and sink rows across databases using the source schema's semantic column
     * order.
     */
    private void assertTableDataEqualsBySourceColumnOrder(
            String sourceDatabase,
            String sourceTable,
            String sinkDatabase,
            String sinkTable,
            String whereClause) {
        List<String> sourceColumns = getColumnNames(sourceDatabase, sourceTable);
        Assertions.assertIterableEquals(
                query(
                        buildOrderedProjectionQuery(
                                sourceDatabase, sourceTable, sourceColumns, whereClause)),
                query(
                        buildOrderedProjectionQuery(
                                sinkDatabase, sinkTable, sourceColumns, whereClause)));
    }

    /**
     * Returns source column names from DESCRIBE so later projections follow the semantic schema
     * instead of the sink's physical column order.
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

    /** Quotes SQL identifiers used in generated verification queries. */
    private String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /** Sorts DESCRIBE rows by column name so equivalent schemas compare deterministically. */
    private List<List<Object>> normalizeDescRows(List<List<Object>> descRows) {
        List<List<Object>> normalizedRows = new ArrayList<>(descRows);
        normalizedRows.sort(Comparator.comparing(row -> String.valueOf(row.get(0))));
        return normalizedRows;
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
