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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergFilesCommitter;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

/**
 * E2E regression test for the fix that ensures the {@code IcebergAggregatedCommitter} produces
 * exactly one Iceberg snapshot per checkpoint, regardless of sink parallelism.
 *
 * <p>Tests verify:
 *
 * <ol>
 *   <li>The total row count in the Iceberg table equals the source row count — no duplicate rows.
 *   <li>Exactly one Iceberg snapshot is produced per checkpoint barrier regardless of how many
 *       parallel sink writers participated.
 *   <li>After a savepoint → restore cycle, already-committed checkpoints are not re-committed.
 * </ol>
 */
@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.SPARK},
        disabledReason =
                "Spark runs as spark-submit --master local (batch-style); checkpoint.interval"
                        + " has no effect and the per-checkpoint snapshot invariant is not"
                        + " observable")
@DisabledOnOs(OS.WINDOWS)
public class IcebergSinkParallelCommitIT extends TestSuiteBase {

    private static final String CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/hadoop-parallel-sink/";

    private static final int EXPECTED_ROW_COUNT = 100;

    private String zstdUrl() {
        return "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-5/zstd-jni-1.5.5-5.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_streaming_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_streaming_table/metadata");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_recovery_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_recovery_table/metadata");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_batch_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_batch_table/metadata");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_row_delta_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_parallel_row_delta_table/metadata");
                container.execInContainer("sh", "-c", "chmod -R 777 " + CATALOG_DIR);

                Container.ExecResult zstdResult =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib"
                                        + " && cd /tmp/seatunnel/plugins/Iceberg/lib"
                                        + " && wget "
                                        + zstdUrl());
                Assertions.assertEquals(0, zstdResult.getExitCode(), zstdResult.getStderr());
            };

    /**
     * Runs a streaming Iceberg sink job with parallelism=2 and verifies that the aggregated
     * committer produces exactly one Iceberg snapshot per checkpoint.
     *
     * <p>{@code split.read-interval} spreads data across multiple checkpoints so that the
     * per-checkpoint atomicity guarantee is observable: with parallelism=2, the pre-fix behaviour
     * would emit two snapshots per checkpoint (one per writer), while the fix must produce exactly
     * one.
     *
     * <p>FakeSource in STREAMING mode does not self-terminate after exhausting splits on any
     * engine. The job is therefore started asynchronously and the test polls until the snapshot
     * invariants are observable. The container teardown at the end of the test stops the job.
     *
     * <p><b>Flink 1.14+ note:</b> See {@link
     * #testBatchUpsertWithParallelismUsesRowDeltaAndProducesOneSnapshot} for why this test is
     * restricted to Flink 1.13 and the Zeta engine.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {
                TestContainerId.FLINK_1_14,
                TestContainerId.FLINK_1_15,
                TestContainerId.FLINK_1_16,
                TestContainerId.FLINK_1_17,
                TestContainerId.FLINK_1_18,
                TestContainerId.FLINK_1_20
            },
            type = {},
            disabledReason =
                    "Flink 1.14+ uses the Sink V2 API with per-subtask committers (same parallelism "
                            + "as the writer). In streaming mode each checkpoint barrier triggers an "
                            + "independent commit per subtask, producing N snapshots instead of one. "
                            + "Confirmed failing on Flink 1.15.3, 1.18.0, and 1.20.1. "
                            + "A WithPostCommitTopology global commit operator is needed in the flink-20 translation module.")
    public void testStreamingWithParallelismProducesOneSnapshotPerCheckpoint(
            TestContainer container) throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/iceberg/fake_to_iceberg_parallel_streaming.conf");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Table table = loadStreamingTable();

                            List<Snapshot> snapshots = new ArrayList<>();
                            table.snapshots().forEach(snapshots::add);

                            Assertions.assertTrue(
                                    snapshots.size() >= 2,
                                    "Expected at least 2 Iceberg snapshots (one per checkpoint)"
                                            + " but got "
                                            + snapshots.size()
                                            + " — data may have been flushed in a single"
                                            + " checkpoint");

                            Set<String> seenCheckpointIds = new HashSet<>();
                            for (Snapshot snapshot : snapshots) {
                                String checkpointId =
                                        snapshot.summary()
                                                .get(
                                                        IcebergFilesCommitter
                                                                .SNAPSHOT_PROPERTY_CHECKPOINT_ID);
                                if (checkpointId != null) {
                                    Assertions.assertTrue(
                                            seenCheckpointIds.add(checkpointId),
                                            "Checkpoint "
                                                    + checkpointId
                                                    + " produced more than one Iceberg snapshot"
                                                    + " — aggregated committer is not the sole"
                                                    + " commit path");
                                }
                            }

                            log.info(
                                    "Verified: {} snapshots, {} distinct checkpoint-ids",
                                    snapshots.size(),
                                    seenCheckpointIds.size());
                        });
    }

    /**
     * Verifies that after a savepoint → restore cycle, the aggregated committer does not re-commit
     * snapshots that were already persisted before the savepoint.
     *
     * <p>Test flow:
     *
     * <ol>
     *   <li>Start a streaming job with {@code parallelism=2}. {@code split.read-interval} keeps
     *       FakeSource alive long enough to trigger several checkpoints.
     *   <li>Wait 9 s — at least 3 checkpoints complete and their snapshots land in Iceberg.
     *   <li>Savepoint the job (stops it and persists the last committed state).
     *   <li>Record the set of {@code seatunnel.checkpoint-id} values already present in the table.
     *   <li>Restore the job from the savepoint. {@code IcebergAggregatedCommitter.restoreCommit()}
     *       must detect the already-committed checkpoint via the snapshot summary and skip it.
     *   <li>Let the restored job run and produce additional checkpoints, then verify:
     *       <ul>
     *         <li>Every snapshot in the table has a <em>unique</em> checkpoint-id — no checkpoint
     *             was committed more than once.
     *         <li>None of the checkpoint-ids present <em>before</em> the savepoint appear in a new
     *             snapshot after restore.
     *       </ul>
     * </ol>
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.SPARK_2_4},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "savepointJob/restoreJob are only supported on the Zeta engine")
    public void testRecoveryFromSavepointProducesNoDuplicateSnapshots(TestContainer container)
            throws Exception {
        final String recoveryConf = "/iceberg/fake_to_iceberg_parallel_streaming_recovery.conf";
        Long jobId = JobIdGenerator.newJobId();

        CompletableFuture<Container.ExecResult> firstRun =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(recoveryConf, String.valueOf(jobId));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        // checkpoint.interval=3s × 3 = 9 s
        Thread.sleep(9_000);

        Assertions.assertEquals(
                0,
                container.savepointJob(String.valueOf(jobId)).getExitCode(),
                "savepointJob should succeed");
        firstRun.get();

        Table tableAfterSavepoint = loadRecoveryTable();
        Set<String> checkpointIdsBefore = collectCheckpointIds(tableAfterSavepoint);
        Assertions.assertFalse(
                checkpointIdsBefore.isEmpty(),
                "At least one checkpoint must have been committed before the savepoint");
        log.info("Checkpoint-ids committed before savepoint: {}", checkpointIdsBefore);

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.restoreJob(recoveryConf, String.valueOf(jobId));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        // Wait long enough for the restored job to finish (4 splits × 3 s + buffer)
        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Table latest = loadRecoveryTable();
                            Set<String> allCheckpointIds = collectCheckpointIds(latest);

                            List<Snapshot> snapshots = new ArrayList<>();
                            latest.snapshots().forEach(snapshots::add);
                            Set<String> seen = new HashSet<>();
                            for (Snapshot snapshot : snapshots) {
                                String cpId =
                                        snapshot.summary()
                                                .get(
                                                        IcebergFilesCommitter
                                                                .SNAPSHOT_PROPERTY_CHECKPOINT_ID);
                                if (cpId != null) {
                                    Assertions.assertTrue(
                                            seen.add(cpId),
                                            "Checkpoint "
                                                    + cpId
                                                    + " produced more than one snapshot —"
                                                    + " restoreCommit() is not idempotent");
                                }
                            }

                            long duplicatesForOldCheckpoints =
                                    snapshots.stream()
                                            .map(
                                                    s ->
                                                            s.summary()
                                                                    .get(
                                                                            IcebergFilesCommitter
                                                                                    .SNAPSHOT_PROPERTY_CHECKPOINT_ID))
                                            .filter(checkpointIdsBefore::contains)
                                            .count();
                            Assertions.assertEquals(
                                    checkpointIdsBefore.size(),
                                    duplicatesForOldCheckpoints,
                                    "Each pre-savepoint checkpoint should appear in exactly one"
                                            + " snapshot after restore; got "
                                            + duplicatesForOldCheckpoints
                                            + " snapshot(s) for "
                                            + checkpointIdsBefore.size()
                                            + " checkpoint-id(s)");

                            log.info(
                                    "Recovery verified: {} total snapshots, {} distinct checkpoint-ids",
                                    snapshots.size(),
                                    allCheckpointIds.size());
                        });
    }

    /**
     * Runs a batch Iceberg sink job with parallelism=2 and verifies that:
     *
     * <ul>
     *   <li>Exactly {@value #EXPECTED_ROW_COUNT} rows are written (no duplicates across workers).
     *   <li>Exactly one Iceberg snapshot is produced — batch ends with a single {@code
     *       COMPLETED_POINT_TYPE} barrier, so the aggregated committer must produce exactly one
     *       commit regardless of how many parallel sink writers participated.
     * </ul>
     *
     * <p><b>Flink 1.14+ note:</b> See {@link
     * #testBatchUpsertWithParallelismUsesRowDeltaAndProducesOneSnapshot} for why this test is
     * restricted to Flink 1.13 and the Zeta engine.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_20},
            type = {},
            disabledReason =
                    "Flink 1.20 runs the Sink V2 committer at writer parallelism even in batch mode, "
                            + "causing each subtask to independently create its own Iceberg snapshot "
                            + "(confirmed: same condition fails in testBatchUpsertWith...). "
                            + "Flink 1.13, 1.15, and 1.18 are not affected in batch mode.")
    public void testBatchWithParallelismProducesOneSnapshotAndNoduplicates(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/iceberg/fake_to_iceberg_parallel_batch.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        Table table = loadBatchTable();

        List<Record> rows = readAllRows(table);
        Assertions.assertEquals(
                EXPECTED_ROW_COUNT,
                rows.size(),
                "Expected "
                        + EXPECTED_ROW_COUNT
                        + " rows but got "
                        + rows.size()
                        + " — possible duplicate rows from parallel writers");

        List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        Assertions.assertEquals(
                1,
                snapshots.size(),
                "Batch job should produce exactly one Iceberg snapshot but got "
                        + snapshots.size());
    }

    private Set<String> collectCheckpointIds(Table table) {
        Set<String> ids = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            String cpId =
                    snapshot.summary().get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID);
            if (cpId != null) {
                ids.add(cpId);
            }
        }
        return ids;
    }

    /**
     * Verifies the RowDelta commit path with parallel writers.
     *
     * <p>With {@code iceberg.table.upsert-mode-enabled=true} every INSERT row also emits an
     * equality-delete record, so {@code WriteResult.deleteFiles} is always non-empty. This forces
     * {@link org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergFilesCommitter}
     * to take the {@code RowDelta} branch instead of {@code AppendFiles}.
     *
     * <p>The test verifies:
     *
     * <ul>
     *   <li>The job succeeds — RowDelta commits work end-to-end with parallel workers.
     *   <li>Exactly one Iceberg snapshot is produced — the aggregated committer merges all workers
     *       into a single RowDelta commit regardless of parallelism.
     *   <li>The snapshot operation is {@code "overwrite"} — confirming the RowDelta path was taken.
     *   <li>The snapshot summary contains {@code seatunnel.checkpoint-id} — confirming the
     *       idempotency guard is active even on the RowDelta branch.
     * </ul>
     *
     * <p><b>Flink 1.14+ note:</b> Flink 1.14 introduced the Sink V2 API ({@code
     * org.apache.flink.api.connector.sink2}) which runs the committer at the same parallelism as
     * the writer (per-subtask), not as a single global operator. With {@code parallelism=2} each
     * subtask creates its own Iceberg snapshot, so the "exactly one snapshot" invariant cannot be
     * guaranteed without a {@code WithPostCommitTopology} global commit operator (not yet
     * implemented). This test is therefore restricted to Flink 1.13 (old Sink V1 API with a single
     * {@code GlobalCommitter}) and the Zeta engine.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_20},
            type = {},
            disabledReason =
                    "Flink 1.20 runs the Sink V2 committer at writer parallelism even in batch mode, "
                            + "causing each subtask to independently create its own Iceberg snapshot. "
                            + "Flink 1.13, 1.15, and 1.18 are not affected in batch mode. "
                            + "A WithPostCommitTopology global commit operator is required to fix this "
                            + "in the flink-20 translation module.")
    public void testBatchUpsertWithParallelismUsesRowDeltaAndProducesOneSnapshot(
            TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/iceberg/fake_to_iceberg_parallel_row_delta.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());

        Table table = loadRowDeltaTable();

        List<Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        Assertions.assertEquals(
                1,
                snapshots.size(),
                "Batch upsert job should produce exactly one Iceberg snapshot but got "
                        + snapshots.size());

        Snapshot snapshot = snapshots.get(0);
        Assertions.assertEquals(
                "overwrite",
                snapshot.operation(),
                "Upsert mode must use RowDelta (operation=overwrite), not AppendFiles");

        Assertions.assertNotNull(
                snapshot.summary().get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID),
                "RowDelta snapshot must carry seatunnel.checkpoint-id in its summary");
    }

    private Table loadRowDeltaTable() throws IOException {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_parallel_row_delta_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private Table loadBatchTable() throws IOException {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_parallel_batch_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private Table loadStreamingTable() throws IOException {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_parallel_streaming_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private Table loadRecoveryTable() throws IOException {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_parallel_recovery_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private List<Record> readAllRows(Table table) {
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                rows.add(record);
            }
        } catch (IOException e) {
            log.warn("Failed to read Iceberg table records", e);
        }
        return rows;
    }
}
