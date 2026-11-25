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

package org.apache.seatunnel.e2e.connector.bigquery;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.await;

/**
 * Runs BigQuery CDC and ADD COLUMN schema evolution, and the batch-mode restore from a checkpoint,
 * against the real BigQuery service.
 *
 * <p>This test is opt-in because it requires GCP credentials, creates billable BigQuery jobs, and
 * cannot be verified correctly by goccy/bigquery-emulator. The emulator ignores append offsets and
 * inserts the buffered rows again on every FlushRows call.
 *
 * <p>Set {@code BIGQUERY_E2E_REAL=true}, {@code BIGQUERY_E2E_PROJECT_ID}, {@code
 * BIGQUERY_E2E_DATASET_ID}, and {@code GOOGLE_APPLICATION_CREDENTIALS}. The dataset must already
 * exist, and the credentials file must contain a service account key.
 */
@Slf4j
@EnabledIfEnvironmentVariable(named = "BIGQUERY_E2E_REAL", matches = "(?i)true")
@DisabledOnContainer(
        value = {},
        type = EngineType.SPARK,
        disabledReason =
                "Spark translation does not currently propagate schema change events to sink writers.")
public class BigQueryCDCRealIT extends TestSuiteBase {

    private static final int INITIAL_SNAPSHOT_TIMEOUT_SECONDS = 180;
    // The Storage Write API can take minutes to detect a table schema update.
    private static final int CHANGE_TIMEOUT_SECONDS = 420;
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";
    private static final String CREDENTIALS_PATH_IN_CONTAINER =
            "/tmp/seatunnel/bigquery-service-account.json";
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final Pattern PROJECT_ID_PATTERN =
            Pattern.compile("[a-z][a-z0-9-]{4,28}[a-z0-9]");
    private static final Pattern DATASET_ID_PATTERN = Pattern.compile("[A-Za-z0-9_]+");
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private static final String BATCH_RESTORE_CONF = "/fake_to_bigquery_batch_real_restore.conf";
    private static final long BATCH_RESTORE_ROW_COUNT = 60L;
    private static final int BATCH_RESTORE_TIMEOUT_SECONDS = 300;
    private static final long BATCH_RESTORE_CHECKPOINT_INTERVAL_MILLIS = 10_000L;
    // Long enough that no later checkpoint completes before the cancel takes effect, and short
    // enough that the source is still emitting rows after the first checkpoint.
    private static final long CANCELLED_JOB_CHECKPOINT_INTERVAL_MILLIS = 20_000L;
    // Longer than a restored job runs, so that job never completes a checkpoint of its own.
    private static final long NO_CHECKPOINT_INTERVAL_MILLIS = 3_600_000L;
    private static final Pattern FLUSHED_STREAM_PATTERN =
            Pattern.compile(
                    "Successfully flushed BigQuery buffered stream (\\S+) to offset (\\d+)");
    private static final Pattern FINALIZED_STREAM_PATTERN =
            Pattern.compile("Finalized restored Buffered write stream (\\S+) with (\\d+) rows");

    private String projectId;
    private String datasetId;
    private Path credentialsPath;
    private BigQuery bigQuery;
    private TableId tableId;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.copyFileToContainer(
                        MountableFile.forHostPath(credentialsPath), CREDENTIALS_PATH_IN_CONTAINER);
                Container.ExecResult mkdirResult =
                        container.execInContainer("bash", "-c", "mkdir -p " + MYSQL_CDC_PLUGIN_LIB);
                Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

                Path driverJarPath = mysqlDriverJarPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(driverJarPath),
                        MYSQL_CDC_PLUGIN_LIB + "/" + driverJarPath.getFileName());
            };

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

    private static Path mysqlDriverJarPath() {
        try {
            Path driverJarPath =
                    Paths.get(
                            Driver.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(
                    Files.isRegularFile(driverJarPath),
                    "MySQL JDBC driver should be resolved from the test classpath");
            return driverJarPath;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve MySQL JDBC driver jar from the test classpath", e);
        }
    }

    @BeforeAll
    public void startResources() throws IOException {
        projectId =
                requireIdentifier(
                        "BIGQUERY_E2E_PROJECT_ID",
                        requireEnvironment("BIGQUERY_E2E_PROJECT_ID"),
                        PROJECT_ID_PATTERN);
        datasetId =
                requireIdentifier(
                        "BIGQUERY_E2E_DATASET_ID",
                        requireEnvironment("BIGQUERY_E2E_DATASET_ID"),
                        DATASET_ID_PATTERN);
        credentialsPath =
                Paths.get(requireEnvironment("GOOGLE_APPLICATION_CREDENTIALS"))
                        .toAbsolutePath()
                        .normalize();
        Assertions.assertTrue(
                Files.isRegularFile(credentialsPath),
                "GOOGLE_APPLICATION_CREDENTIALS must point to a service account JSON file");

        try (InputStream credentials = Files.newInputStream(credentialsPath)) {
            bigQuery =
                    BigQueryOptions.newBuilder()
                            .setProjectId(projectId)
                            .setCredentials(ServiceAccountCredentials.fromStream(credentials))
                            .build()
                            .getService();
        }

        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
    }

    @BeforeEach
    public void initializeSourceTable() {
        executeMysqlSql("DROP TABLE IF EXISTS " + MYSQL_DATABASE + "." + SOURCE_TABLE);
        executeMysqlSql(
                "CREATE TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " ("
                        + "uuid BIGINT NOT NULL, "
                        + "name VARCHAR(128), "
                        + "score INT, "
                        + "PRIMARY KEY (uuid)"
                        + ") ENGINE=InnoDB");
        executeMysqlSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (uuid, name, score) VALUES "
                        + "(1, 'Alice', 95), (2, 'Bob', 88)");
    }

    @AfterEach
    public void deleteBigQueryTable() {
        if (tableId != null) {
            bigQuery.delete(tableId);
            tableId = null;
        }
    }

    @AfterAll
    public void stopMySql() {
        MYSQL_CONTAINER.close();
    }

    @TestTemplate
    public void testCdcAndAddColumnSchemaEvolution(TestContainer container) throws Exception {
        tableId =
                TableId.of(
                        projectId,
                        datasetId,
                        "seatunnel_cdc_schema_" + UUID.randomUUID().toString().replace("-", ""));
        createBigQueryTable();

        CompletableFuture<Void> jobFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                Container.ExecResult result =
                                        container.executeJob(
                                                "/mysql_cdc_to_bigquery_real_sink.conf",
                                                Arrays.asList(
                                                        "bigquery_project_id=" + projectId,
                                                        "bigquery_dataset_id=" + datasetId,
                                                        "bigquery_table_id=" + tableId.getTable()));
                                if (result.getExitCode() != 0) {
                                    throw new IllegalStateException(result.getStderr());
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Set<List<Object>> initialRows =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95L), Arrays.asList(2L, "Bob", 88L))
                        .collect(Collectors.toSet());
        await().atMost(INITIAL_SNAPSHOT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(initialRows, queryRows(false));
                        });

        executeMysqlSql(
                "ALTER TABLE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " ADD COLUMN email VARCHAR(255)");
        executeMysqlSql(
                "UPDATE "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " SET email = CASE uuid "
                        + "WHEN 1 THEN 'alice@example.com' "
                        + "WHEN 2 THEN 'bob@example.com' END");

        Set<List<Object>> evolvedRows =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice", 95L, "alice@example.com"),
                                Arrays.asList(2L, "Bob", 88L, "bob@example.com"))
                        .collect(Collectors.toSet());
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            assertEmailColumnExists();
                            Assertions.assertEquals(evolvedRows, queryRows(true));
                        });

        executeMysqlSql("DELETE FROM " + MYSQL_DATABASE + "." + SOURCE_TABLE + " WHERE uuid = 1");
        Set<List<Object>> rowsAfterDelete =
                Stream.<List<Object>>of(Arrays.asList(2L, "Bob", 88L, "bob@example.com"))
                        .collect(Collectors.toSet());
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(rowsAfterDelete, queryRows(true));
                        });

        executeMysqlSql(
                "INSERT INTO "
                        + MYSQL_DATABASE
                        + "."
                        + SOURCE_TABLE
                        + " (uuid, name, score, email) "
                        + "VALUES (1, 'Alice', 95, 'alice@example.com')");
        await().atMost(CHANGE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            rethrowJobFailure(jobFuture);
                            Assertions.assertEquals(evolvedRows, queryRows(true));
                        });
    }

    /**
     * Cancels a batch-mode job after rows were appended past its last completed checkpoint, then
     * restores the job from that checkpoint. The rows left unflushed in the old buffered stream
     * must stay invisible, and the replay must make every remaining row visible exactly once.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Cancelling and restoring a job are only implemented by the SeaTunnel test container")
    public void testBatchModeRestoreAfterUncheckpointedAppends(TestContainer container)
            throws Exception {
        createBatchRestoreTable();
        String jobId = runBatchRestoreJobUntilCancelled(container);

        assertRestoredJobWritesEveryRowOnce(container, jobId);
    }

    /**
     * Restores one checkpoint twice. The first restored job finalizes the buffered stream recorded
     * in that checkpoint and is cancelled without completing a checkpoint of its own. The second
     * restored job retires the same stream again, so FlushRows and FinalizeWriteStream reach a
     * stream that is already finalized. The real service must answer in a way the sink accepts, and
     * every row must still become visible exactly once.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Cancelling and restoring a job are only implemented by the SeaTunnel test container")
    public void testBatchModeRestoreOfAlreadyFinalizedStream(TestContainer container)
            throws Exception {
        createBatchRestoreTable();
        String jobId = runBatchRestoreJobUntilCancelled(container);

        String firstRestoredJobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> firstRestoredRun =
                restoreBatchJob(
                        container, jobId, firstRestoredJobId, NO_CHECKPOINT_INTERVAL_MILLIS);
        try {
            await().atMost(BATCH_RESTORE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    countStreamRetirements(container) >= 1
                                            && "RUNNING"
                                                    .equals(
                                                            container.getJobStatus(
                                                                    firstRestoredJobId)));
            Assertions.assertEquals(
                    0L,
                    container.getCompletedCheckpointCount(firstRestoredJobId),
                    "The first restored job must not commit rows of its own");
        } finally {
            cancelBatchJob(container, firstRestoredJobId, firstRestoredRun);
        }

        assertRestoredJobWritesEveryRowOnce(container, jobId);

        long retirements = countStreamRetirements(container);
        Assertions.assertTrue(
                retirements >= 2,
                "The second restored job must retire the finalized stream again, but "
                        + retirements
                        + " retirements were logged");
    }

    /**
     * Runs the batch restore job until the server logs show rows appended past its committed
     * checkpoint, then cancels it. A later checkpoint can still cover those rows before the cancel
     * takes effect, so {@link #assertRetiredStreamHeldUncheckpointedRows} checks the restored
     * checkpoint afterwards.
     *
     * @return the id of the cancelled job
     */
    private String runBatchRestoreJobUntilCancelled(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String[] variables =
                batchRestoreVariables(CANCELLED_JOB_CHECKPOINT_INTERVAL_MILLIS)
                        .toArray(new String[0]);

        CompletableFuture<Container.ExecResult> firstRun =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(BATCH_RESTORE_CONF, jobId, variables);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        try {
            await().atMost(INITIAL_SNAPSHOT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> container.getCompletedCheckpointCount(jobId) > 0);
            await("rows appended past the committed checkpoint")
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(() -> hasUncheckpointedRows(container, jobId));
        } finally {
            cancelBatchJob(container, jobId, firstRun);
        }

        long rowsBeforeRestore = countRows();
        Assertions.assertTrue(
                rowsBeforeRestore > 0 && rowsBeforeRestore < BATCH_RESTORE_ROW_COUNT,
                "The job must be cancelled after the first commit and before every row is written,"
                        + " but "
                        + rowsBeforeRestore
                        + " rows were visible");
        return jobId;
    }

    private void assertRestoredJobWritesEveryRowOnce(TestContainer container, String sourceJobId)
            throws Exception {
        List<Long> idsBeforeRestore = readRowIds();
        // A job restored from a checkpoint runs under a new job id.
        String restoredJobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> restoredRun =
                restoreBatchJob(
                        container,
                        sourceJobId,
                        restoredJobId,
                        BATCH_RESTORE_CHECKPOINT_INTERVAL_MILLIS);
        try {
            await().atMost(BATCH_RESTORE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertEquals(BATCH_RESTORE_ROW_COUNT, countRows()));

            // Later checkpoints flush the same offsets again, which must not add rows.
            long checkpointsAtFullCount = container.getCompletedCheckpointCount(restoredJobId);
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    container.getCompletedCheckpointCount(restoredJobId)
                                            > checkpointsAtFullCount);

            // FakeSource draws a new random id for every row, replayed rows included, so ids can
            // only show that committed rows survive and that no row became visible twice.
            List<Long> idsAfterRestore = readRowIds();
            Assertions.assertEquals(BATCH_RESTORE_ROW_COUNT, idsAfterRestore.size());
            Assertions.assertEquals(
                    BATCH_RESTORE_ROW_COUNT,
                    new HashSet<>(idsAfterRestore).size(),
                    "A row became visible more than once");
            Assertions.assertTrue(
                    idsAfterRestore.containsAll(idsBeforeRestore),
                    "A row committed before the restore disappeared");
            assertRetiredStreamHeldUncheckpointedRows(container);
        } finally {
            container.cancelJob(restoredJobId);
            restoredRun.get(2, TimeUnit.MINUTES);
        }
    }

    /**
     * Proves that the restore ran inside the failure window: the retired stream held more rows than
     * the restored checkpoint flushed. BigQuery reports the row count of a stream only when it is
     * first finalized, so this can only be checked after the restore.
     */
    private void assertRetiredStreamHeldUncheckpointedRows(TestContainer container) {
        Map<String, Long> finalizedRows =
                maxLoggedValuePerStream(container, FINALIZED_STREAM_PATTERN);
        Assertions.assertEquals(1, finalizedRows.size(), "Exactly one stream must be retired");

        Map.Entry<String, Long> retiredStream = finalizedRows.entrySet().iterator().next();
        long flushedRows =
                maxLoggedValuePerStream(container, FLUSHED_STREAM_PATTERN)
                                .get(retiredStream.getKey())
                        + 1;
        Assertions.assertTrue(
                retiredStream.getValue() > flushedRows,
                "The cancelled job must leave rows past its checkpoint, but the stream held "
                        + retiredStream.getValue()
                        + " rows and the checkpoint covered "
                        + flushedRows);
    }

    private CompletableFuture<Container.ExecResult> restoreBatchJob(
            TestContainer container,
            String sourceJobId,
            String restoredJobId,
            long checkpointIntervalMillis) {
        List<String> variables = batchRestoreVariables(checkpointIntervalMillis);
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.restoreJobWithCheckpoint(
                                BATCH_RESTORE_CONF, sourceJobId, restoredJobId, variables);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void cancelBatchJob(
            TestContainer container, String jobId, CompletableFuture<Container.ExecResult> run)
            throws Exception {
        Container.ExecResult cancelResult = container.cancelJob(jobId);
        Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
        await().atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> "CANCELED".equals(container.getJobStatus(jobId)));
        run.get(2, TimeUnit.MINUTES);
    }

    private List<String> batchRestoreVariables(long checkpointIntervalMillis) {
        return Arrays.asList(
                "bigquery_project_id=" + projectId,
                "bigquery_dataset_id=" + datasetId,
                "bigquery_table_id=" + tableId.getTable(),
                "checkpoint_interval=" + checkpointIntervalMillis);
    }

    /** Tells whether a job appended more rows than its committed checkpoint made visible. */
    private boolean hasUncheckpointedRows(TestContainer container, String jobId) {
        long appendedRows = countAppendedRows(container, jobId);
        return maxLoggedValuePerStream(container, FLUSHED_STREAM_PATTERN).values().stream()
                .anyMatch(flushedOffset -> appendedRows > flushedOffset + 1);
    }

    /** Counts the rows a job appended. The restore config appends one row per call. */
    private long countAppendedRows(TestContainer container, String jobId) {
        String jobLogPrefix = "[" + jobId + "]";
        return Arrays.stream(container.getServerLogs().split("\n"))
                .filter(line -> line.contains(jobLogPrefix))
                .filter(line -> line.contains("Successfully appended 1 rows."))
                .count();
    }

    /** Collects the largest value the pattern logged for each stream of the current table. */
    private Map<String, Long> maxLoggedValuePerStream(TestContainer container, Pattern pattern) {
        String streamsOfTable = "/tables/" + tableId.getTable() + "/streams/";
        Map<String, Long> values = new HashMap<>();
        Matcher matcher = pattern.matcher(container.getServerLogs());
        while (matcher.find()) {
            String stream = matcher.group(1);
            if (stream.contains(streamsOfTable)) {
                values.merge(stream, Long.parseLong(matcher.group(2)), Math::max);
            }
        }
        return values;
    }

    /** Counts how often a stream of the current table was retired, whatever BigQuery answered. */
    private long countStreamRetirements(TestContainer container) {
        String streamsOfTable = "/tables/" + tableId.getTable() + "/streams/";
        return Arrays.stream(container.getServerLogs().split("\n"))
                .filter(line -> line.contains(streamsOfTable))
                .filter(
                        line ->
                                line.contains("Finalized restored Buffered write stream")
                                        || line.contains("was already finalized"))
                .count();
    }

    private void createBatchRestoreTable() throws InterruptedException {
        tableId =
                TableId.of(
                        projectId,
                        datasetId,
                        "seatunnel_batch_restore_" + UUID.randomUUID().toString().replace("-", ""));
        String sql =
                String.format(
                        "CREATE TABLE `%s.%s.%s` (id INT64, name STRING) OPTIONS ("
                                + "expiration_timestamp = "
                                + "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
                                + ")",
                        projectId, datasetId, tableId.getTable());
        bigQuery.query(QueryJobConfiguration.of(sql));
    }

    private long countRows() throws InterruptedException {
        String query =
                String.format(
                        "SELECT COUNT(*) FROM `%s.%s.%s`",
                        projectId, datasetId, tableId.getTable());
        TableResult result =
                bigQuery.query(
                        QueryJobConfiguration.newBuilder(query).setUseQueryCache(false).build());
        return result.iterateAll().iterator().next().get(0).getLongValue();
    }

    private List<Long> readRowIds() throws InterruptedException {
        String query =
                String.format(
                        "SELECT id FROM `%s.%s.%s`", projectId, datasetId, tableId.getTable());
        TableResult result =
                bigQuery.query(
                        QueryJobConfiguration.newBuilder(query).setUseQueryCache(false).build());
        return StreamSupport.stream(result.iterateAll().spliterator(), false)
                .map(row -> row.get(0).getLongValue())
                .collect(Collectors.toList());
    }

    private void createBigQueryTable() throws InterruptedException {
        String sql =
                String.format(
                        "CREATE TABLE `%s.%s.%s` ("
                                + "uuid INT64 NOT NULL, "
                                + "name STRING, "
                                + "score INT64, "
                                + "PRIMARY KEY (uuid) NOT ENFORCED"
                                + ") OPTIONS ("
                                + "max_staleness = INTERVAL 0 MINUTE, "
                                + "expiration_timestamp = "
                                + "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
                                + ")",
                        projectId, datasetId, tableId.getTable());
        bigQuery.query(QueryJobConfiguration.of(sql));
    }

    private void assertEmailColumnExists() {
        Table table = bigQuery.getTable(tableId);
        Assertions.assertNotNull(table, "BigQuery target table does not exist");
        Assertions.assertNotNull(table.getDefinition().getSchema());
        boolean emailExists =
                table.getDefinition().getSchema().getFields().stream()
                        .map(Field::getName)
                        .anyMatch("email"::equalsIgnoreCase);
        Assertions.assertTrue(emailExists, "BigQuery target table does not contain email");
    }

    private Set<List<Object>> queryRows(boolean includeEmail) throws InterruptedException {
        String columns = includeEmail ? "uuid, name, score, email" : "uuid, name, score";
        String query =
                String.format(
                        "SELECT %s FROM `%s.%s.%s`",
                        columns, projectId, datasetId, tableId.getTable());
        TableResult result =
                bigQuery.query(
                        QueryJobConfiguration.newBuilder(query).setUseQueryCache(false).build());
        return StreamSupport.stream(result.iterateAll().spliterator(), false)
                .map(
                        row -> {
                            List<Object> values =
                                    Arrays.<Object>asList(
                                            row.get(0).isNull() ? null : row.get(0).getLongValue(),
                                            row.get(1).isNull()
                                                    ? null
                                                    : row.get(1).getStringValue(),
                                            row.get(2).isNull() ? null : row.get(2).getLongValue());
                            if (!includeEmail) {
                                return values;
                            }
                            return Arrays.<Object>asList(
                                    values.get(0),
                                    values.get(1),
                                    values.get(2),
                                    row.get(3).isNull() ? null : row.get(3).getStringValue());
                        })
                .collect(Collectors.toSet());
    }

    private void executeMysqlSql(String sql) {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL_CONTAINER.getJdbcUrl(),
                                MYSQL_CONTAINER.getUsername(),
                                MYSQL_CONTAINER.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void rethrowJobFailure(CompletableFuture<Void> jobFuture) {
        if (jobFuture.isCompletedExceptionally()) {
            jobFuture.join();
        }
    }

    private static String requireEnvironment(String name) {
        String value = System.getenv(name);
        Assertions.assertNotNull(value, name + " must be set when BIGQUERY_E2E_REAL=true");
        Assertions.assertFalse(value.trim().isEmpty(), name + " must not be empty");
        return value.trim();
    }

    private static String requireIdentifier(String name, String value, Pattern pattern) {
        Assertions.assertTrue(pattern.matcher(value).matches(), name + " is invalid: " + value);
        return value;
    }
}
