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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support restore")
public class MysqlCDCCheckpointRestoreIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table_no_primary_key";
    private static final String SINK_TABLE = "mysql_cdc_e2e_sink_table_checkpoint_restore";
    private static final String CONF_FILE = "/mysqlcdc_to_mysql_with_checkpoint_restore.conf";
    private static final String SCHEMA_RESTORE_SOURCE_TABLE = "mysql_cdc_e2e_schema_restore_source";
    private static final String SCHEMA_RESTORE_SINK_TABLE = "mysql_cdc_e2e_schema_restore_sink";
    private static final String SCHEMA_RESTORE_SINK_BACKUP_TABLE =
            "mysql_cdc_e2e_schema_restore_sink_backup";
    private static final String SCHEMA_RESTORE_CONF_FILE =
            "/mysqlcdc_to_mysql_with_schema_change_checkpoint_restore.conf";
    private static final String SCHEMA_RESTORE_QUERY =
            "select id, name, email from %s.%s order by id";
    private static final String INCREMENTAL_READ_MARKER =
            "Start incremental read task for incremental split";
    private static final String PIPELINE_RESTORE_MARKER = "Restore time 1, pipeline";

    private static final String SOURCE_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, f_year from %s.%s";
    private static final String SINK_SQL_TEMPLATE =
            "select id, cast(f_binary as char) as f_binary, cast(f_blob as char) as f_blob, cast(f_long_varbinary as char) as f_long_varbinary,"
                    + " cast(f_longblob as char) as f_longblob, cast(f_tinyblob as char) as f_tinyblob, cast(f_varbinary as char) as f_varbinary,"
                    + " f_smallint, f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, f_integer_unsigned,"
                    + " f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, f_double_precision, f_longtext, f_mediumtext,"
                    + " f_text, f_tinytext, f_varchar, f_date, f_datetime, f_timestamp, f_bit1, cast(f_bit64 as char) as f_bit64, f_char,"
                    + " f_enum, cast(f_mediumblob as char) as f_mediumblob, f_long_varchar, f_real, f_time, f_tinyint, f_tinyint_unsigned,"
                    + " f_json, cast(f_year as year) from %s.%s";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    MYSQL_DATABASE,
                    MYSQL_USER_NAME,
                    MYSQL_USER_PASSWORD,
                    MYSQL_DATABASE);

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.ofClassName("com.mysql.cj.jdbc.Driver")
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

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

    @BeforeAll
    @Override
    public void startUp() {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcRestoreFromCheckpointWithoutSnapshotReplay(TestContainer container)
            throws Exception {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 1, 2, 3);
        createAppendOnlySinkTable(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        long sourceJobId = JobIdGenerator.newJobId();
        long restoreJobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> sourceJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(CONF_FILE, String.valueOf(sourceJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(
                                                        String.valueOf(sourceJobId))
                                                > 0));

        container.stopJob(String.valueOf(sourceJobId));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "CANCELED",
                                        container.getJobStatus(String.valueOf(sourceJobId))));
        Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 11, 12);

        CompletableFuture<Container.ExecResult> restoreFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.restoreJobWithCheckpoint(
                                        CONF_FILE,
                                        String.valueOf(sourceJobId),
                                        String.valueOf(restoreJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 21, 22);
        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 1));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 2));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 3));
        container.stopJob(String.valueOf(restoreJobId));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "CANCELED",
                                        container.getJobStatus(String.valueOf(restoreJobId))));
        Assertions.assertEquals(0, restoreFuture.get().getExitCode());
    }

    /**
     * Verifies that a savepoint taken after the incremental stream starts can restore the same job
     * without replaying already replicated CDC records.
     *
     * @param container engine test container used to run and restore the job
     * @throws Exception when savepoint or restore fails
     */
    @TestTemplate
    public void testMysqlCdcSavepointRestoreDuringIncrementalStreaming(TestContainer container)
            throws Exception {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 1, 2, 3);
        createAppendOnlySinkTable(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        long jobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> sourceJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(CONF_FILE, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);
        awaitCompletedCheckpointCountAtLeast(container, jobId, 1);

        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 11, 12);
        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        Assertions.assertEquals(0, container.savepointJob(String.valueOf(jobId)).getExitCode());
        awaitJobStatus(container, jobId, "SAVEPOINT_DONE");
        Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());

        CompletableFuture<Container.ExecResult> restoreFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.restoreJob(CONF_FILE, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitJobStatus(container, jobId, "RUNNING");
        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 21, 22);
        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 1));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 2));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 3));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 11));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 12));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 21));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 22));

        container.stopJob(String.valueOf(jobId));
        awaitJobStatus(container, jobId, "CANCELED");
        Assertions.assertEquals(0, restoreFuture.get().getExitCode());
    }

    /**
     * Verifies that a full pipeline failure after completed checkpoints can restore from the last
     * checkpoint and converge source and sink contents again.
     *
     * @param container engine test container used to run and restore the job
     * @throws Exception when the failure injection or restore flow fails
     */
    @TestTemplate
    public void testMysqlCdcRestoresAfterCheckpointedFullPipelineFailure(TestContainer container)
            throws Exception {
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 1, 2, 3);
        createAppendOnlySinkTable(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        long sourceJobId = JobIdGenerator.newJobId();
        long restoreJobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> sourceJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(CONF_FILE, String.valueOf(sourceJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);
        awaitCompletedCheckpointCountAtLeast(container, sourceJobId, 1);

        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 11, 12);
        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);
        awaitCompletedCheckpointCountAtLeast(container, sourceJobId, 2);

        addPrimaryKeyOnId(MYSQL_DATABASE, SINK_TABLE);
        insertCheckpointRestoreRow(MYSQL_DATABASE, SOURCE_TABLE, 12);
        awaitJobStatus(container, sourceJobId, "FAILED");
        Assertions.assertNotEquals(0, sourceJobFuture.get().getExitCode());

        dropPrimaryKey(MYSQL_DATABASE, SINK_TABLE);
        insertCheckpointRestoreRows(MYSQL_DATABASE, SOURCE_TABLE, 21, 22);

        CompletableFuture<Container.ExecResult> restoreFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.restoreJobWithCheckpoint(
                                        CONF_FILE,
                                        String.valueOf(sourceJobId),
                                        String.valueOf(restoreJobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitJobStatus(container, restoreJobId, "RUNNING");
        awaitSourceAndSinkConsistent(MYSQL_DATABASE, SOURCE_TABLE, SINK_TABLE);

        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 1));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 2));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 3));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 11));
        Assertions.assertEquals(2L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 12));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 21));
        Assertions.assertEquals(1L, getRowCountById(MYSQL_DATABASE, SINK_TABLE, 22));

        container.stopJob(String.valueOf(restoreJobId));
        awaitJobStatus(container, restoreJobId, "CANCELED");
        Assertions.assertEquals(0, restoreFuture.get().getExitCode());
    }

    /**
     * Verifies that the source task restores its evolved runtime schema during an automatic
     * pipeline retry.
     *
     * <p>This test deliberately avoids submitting a new restore job because rebuilding the job from
     * checkpoint metadata would initialize the source collector with the evolved schema and hide
     * the automatic-retry regression.
     */
    @TestTemplate
    public void testSourceCollectorRestoresSchemaAfterPipelineRetry(TestContainer container)
            throws Exception {
        prepareSchemaRestoreTables();

        long jobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> sourceJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        SCHEMA_RESTORE_CONF_FILE, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        awaitSchemaRestoreRows("select id, name from %s.%s order by id");
        awaitIncrementalRead(container, SCHEMA_RESTORE_SOURCE_TABLE);

        executeSql(
                "ALTER TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SOURCE_TABLE
                        + " ADD COLUMN email VARCHAR(255)");
        insertSchemaRestoreRow(2, "after-ddl@example.com");
        awaitSchemaRestoreRows(SCHEMA_RESTORE_QUERY);

        long completedCheckpointCount =
                container.getCompletedCheckpointCount(String.valueOf(jobId));
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(String.valueOf(jobId))
                                                > completedCheckpointCount,
                                        "A checkpoint completed after ADD COLUMN is required"));

        int restoreMarkerCountBeforeFailure =
                countOccurrences(container.getServerLogs(), PIPELINE_RESTORE_MARKER);
        executeSql(
                "RENAME TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SINK_TABLE
                        + " TO "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SINK_BACKUP_TABLE);
        insertSchemaRestoreRow(3, "after-restore@example.com");

        awaitPipelineRestore(container, restoreMarkerCountBeforeFailure);
        executeSql(
                "RENAME TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SINK_BACKUP_TABLE
                        + " TO "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SINK_TABLE);

        awaitPostRecoveryEmail(container, jobId, "after-restore@example.com");
        Assertions.assertEquals("RUNNING", container.getJobStatus(String.valueOf(jobId)));

        container.stopJob(String.valueOf(jobId));
        awaitJobCanceled(container, jobId);
        Assertions.assertEquals(0, sourceJobFuture.get().getExitCode());
    }

    /**
     * Waits until the target job reaches the expected terminal or running state.
     *
     * @param container test container hosting the job
     * @param jobId target job id
     * @param expectedStatus expected status string from REST polling
     */
    private void awaitJobStatus(TestContainer container, long jobId, String expectedStatus) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        container.getJobStatus(String.valueOf(jobId))));
    }

    /**
     * Waits until the target job exposes at least the required number of completed checkpoints.
     *
     * @param container test container hosting the job
     * @param jobId target job id
     * @param expectedCompletedCheckpoints minimum completed checkpoint count
     */
    private void awaitCompletedCheckpointCountAtLeast(
            TestContainer container, long jobId, long expectedCompletedCheckpoints) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        container.getCompletedCheckpointCount(String.valueOf(jobId))
                                                >= expectedCompletedCheckpoints));
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> query(String sql) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private void prepareSchemaRestoreTables() {
        executeSql(
                "DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + SCHEMA_RESTORE_SINK_BACKUP_TABLE);
        executeSql("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + SCHEMA_RESTORE_SINK_TABLE);
        executeSql("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + SCHEMA_RESTORE_SOURCE_TABLE);
        executeSql(
                "CREATE TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SOURCE_TABLE
                        + " (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        executeSql(
                "CREATE TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SINK_TABLE
                        + " LIKE "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SOURCE_TABLE);
        executeSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SOURCE_TABLE
                        + " (id, name) VALUES (1, 'before-ddl')");
    }

    private void insertSchemaRestoreRow(int id, String email) {
        executeSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SCHEMA_RESTORE_SOURCE_TABLE
                        + " (id, name, email) VALUES ("
                        + id
                        + ", 'schema-restore-"
                        + id
                        + "', '"
                        + email
                        + "')");
    }

    private void awaitSchemaRestoreRows(String queryTemplate) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(
                                                String.format(
                                                        queryTemplate,
                                                        MYSQL_DATABASE,
                                                        SCHEMA_RESTORE_SOURCE_TABLE)),
                                        query(
                                                String.format(
                                                        queryTemplate,
                                                        MYSQL_DATABASE,
                                                        SCHEMA_RESTORE_SINK_TABLE))));
    }

    private void awaitPostRecoveryEmail(TestContainer container, long jobId, String expectedEmail) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNotEquals(
                                    "FAILED",
                                    container.getJobStatus(String.valueOf(jobId)),
                                    "The recovered source task failed before forwarding the "
                                            + "post-checkpoint row with the evolved schema");
                            List<List<Object>> rows =
                                    query(
                                            String.format(
                                                    SCHEMA_RESTORE_QUERY,
                                                    MYSQL_DATABASE,
                                                    SCHEMA_RESTORE_SINK_TABLE));
                            List<Object> recoveredRow =
                                    rows.stream()
                                            .filter(row -> ((Number) row.get(0)).intValue() == 3)
                                            .findFirst()
                                            .orElseThrow(
                                                    () ->
                                                            new AssertionError(
                                                                    "The source collector has not "
                                                                            + "forwarded the "
                                                                            + "post-recovery row"));
                            Assertions.assertEquals(
                                    expectedEmail,
                                    recoveredRow.get(2),
                                    "The recovered JDBC writer used its pre-evolution schema and "
                                            + "dropped the email field");
                        });
    }

    private void awaitIncrementalRead(TestContainer container, String capturedTable) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String serverLogs = container.getServerLogs();
                            Assertions.assertTrue(
                                    serverLogs.contains(INCREMENTAL_READ_MARKER),
                                    "Incremental reader has not started yet");
                            Assertions.assertTrue(
                                    serverLogs.contains(MYSQL_DATABASE + "." + capturedTable),
                                    "Incremental reader has not started for " + capturedTable);
                        });
    }

    private void awaitPipelineRestore(TestContainer container, int previousRestoreMarkerCount) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        countOccurrences(
                                                        container.getServerLogs(),
                                                        PIPELINE_RESTORE_MARKER)
                                                > previousRestoreMarkerCount,
                                        "The pipeline retry has not started"));
    }

    private int countOccurrences(String value, String marker) {
        int count = 0;
        int index = 0;
        while ((index = value.indexOf(marker, index)) >= 0) {
            count++;
            index += marker.length();
        }
        return count;
    }

    private void awaitJobCanceled(TestContainer container, long jobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "CANCELED", container.getJobStatus(String.valueOf(jobId))));
    }

    private void awaitSourceAndSinkConsistent(
            String database, String sourceTable, String sinkTable) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertIterableEquals(
                                        query(getSourceQuerySQL(database, sourceTable)),
                                        query(getSinkQuerySQL(database, sinkTable))));
    }

    private void createAppendOnlySinkTable(String database, String sourceTable, String sinkTable) {
        executeSql("DROP TABLE IF EXISTS " + database + "." + sinkTable);
        executeSql(
                "CREATE TABLE "
                        + database
                        + "."
                        + sinkTable
                        + " LIKE "
                        + database
                        + "."
                        + sourceTable);
    }

    /**
     * Adds a primary key on the replicated id column to make a duplicate CDC event fail in the
     * append-only sink table.
     *
     * @param database target database
     * @param tableName target sink table
     */
    private void addPrimaryKeyOnId(String database, String tableName) {
        executeSql("ALTER TABLE " + database + "." + tableName + " ADD PRIMARY KEY (id)");
    }

    /**
     * Drops the temporary primary key so the restored pipeline can replay duplicate source rows
     * again.
     *
     * @param database target database
     * @param tableName target sink table
     */
    private void dropPrimaryKey(String database, String tableName) {
        executeSql("ALTER TABLE " + database + "." + tableName + " DROP PRIMARY KEY");
    }

    private long getRowCountById(String database, String tableName, int id) {
        List<List<Object>> result =
                query("select count(*) from " + database + "." + tableName + " where id = " + id);
        Object value = result.get(0).get(0);
        return value instanceof Number
                ? ((Number) value).longValue()
                : Long.parseLong(String.valueOf(value));
    }

    private void insertCheckpointRestoreRows(String database, String tableName, int... ids) {
        for (int id : ids) {
            insertCheckpointRestoreRow(database, tableName, id);
        }
    }

    private void insertCheckpointRestoreRow(String database, String tableName, int id) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( "
                        + id
                        + ", 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         "
                        + (123456789L + id)
                        + ", 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'checkpoint-restore-row-"
                        + id
                        + "', 'This is a tiny text field', 'checkpoint-restore-varchar-"
                        + id
                        + "', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\", \"id\": "
                        + id
                        + " }', "
                        + (2000 + id)
                        + " )");
    }

    private String getSourceQuerySQL(String database, String tableName) {
        return String.format(SOURCE_SQL_TEMPLATE, database, tableName);
    }

    private String getSinkQuerySQL(String database, String tableName) {
        return String.format(SINK_SQL_TEMPLATE, database, tableName);
    }

    @Override
    @AfterAll
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }
}
