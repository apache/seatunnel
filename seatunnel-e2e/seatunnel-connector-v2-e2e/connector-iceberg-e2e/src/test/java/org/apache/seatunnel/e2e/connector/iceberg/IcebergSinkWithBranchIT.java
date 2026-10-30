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
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.catalog.IcebergCatalog;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergDropDataStrategy;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSink;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergFilesCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;

import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "")
@DisabledOnOs(OS.WINDOWS)
public class IcebergSinkWithBranchIT extends TestSuiteBase {

    private static final String CATALOG_DIR = "/tmp/seatunnel_mnt/iceberg/hadoop-sink/";

    private static final TableIdentifier SAVE_MODE_TABLE =
            TableIdentifier.of("seatunnel_namespace", "save_mode_table");

    private static final String commitBranch = "commit-branch";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // Iceberg's Hadoop catalog cannot reliably create the mounted warehouse's
                // metadata and data directories from the job containers.
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p " + CATALOG_DIR + "seatunnel_namespace/iceberg_sink_table/data");
                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p "
                                + CATALOG_DIR
                                + "seatunnel_namespace/iceberg_sink_table/metadata");
                container.execInContainer("sh", "-c", "chmod -R 777  " + CATALOG_DIR);

                DependencyJar.of(Zstd.class)
                        .copyTo(container, "/tmp/seatunnel/plugins/Iceberg/lib");
            };

    @TestTemplate
    public void testInsertAndCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult =
                container.executeJob("/iceberg/fake_to_iceberg_with_branch.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // check branch exists
                            Assertions.assertEquals(true, checkBranchExists());
                            // load from branch
                            Assertions.assertEquals(100, loadDataFromIcebergTableBranch().size());
                        });
    }

    /**
     * Exercises metadata reset and the first real checkpoint through the public sink lifecycle.
     * Fresh catalogs verify that the recreated branch and replacement data are persisted.
     *
     * @param warehouse isolated real Hadoop catalog warehouse
     * @throws Exception if catalog, writer, checkpoint commit or readback fails
     */
    @Test
    public void testHardResetPersistsBranchAndFirstCheckpoint(@TempDir Path warehouse)
            throws Exception {
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (HadoopCatalog initial = openSaveModeCatalog(warehouse)) {
            ReadonlyConfig appendConfig = saveModeConfig(warehouse, null, false);
            Table original = createSaveModeTable(initial);
            commitRow(createSink(appendConfig, original), 101L, 1, "old");
            original.refresh();
            long oldSnapshot = original.currentSnapshot().snapshotId();
            original.manageSnapshots()
                    .createBranch(commitBranch, oldSnapshot)
                    .createTag("old-tag", oldSnapshot)
                    .commit();
            String uuid = ((HasTableOperations) original).operations().current().uuid();
            Set<Long> oldSnapshots = snapshotIds(original);
            Map<String, String> properties = new HashMap<>(original.properties());
            ReadonlyConfig resetConfig = saveModeConfig(warehouse, commitBranch, true);
            IcebergSink sink = createSink(resetConfig, original);
            try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
                handler.open();
                handler.handleSaveMode();
            }

            try (HadoopCatalog reopened = openSaveModeCatalog(warehouse)) {
                Table reset = reopened.loadTable(SAVE_MODE_TABLE);
                Assertions.assertEquals(
                        uuid, ((HasTableOperations) reset).operations().current().uuid());
                Assertions.assertEquals(original.schema().asStruct(), reset.schema().asStruct());
                Assertions.assertEquals(original.spec(), reset.spec());
                Assertions.assertEquals(original.sortOrder(), reset.sortOrder());
                Assertions.assertEquals(original.location(), reset.location());
                Assertions.assertEquals(properties, reset.properties());
                Assertions.assertNull(reset.currentSnapshot());
                Assertions.assertEquals(Collections.singleton(commitBranch), reset.refs().keySet());
                Assertions.assertTrue(Collections.disjoint(oldSnapshots, snapshotIds(reset)));
                Assertions.assertTrue(readRows(reset, commitBranch).isEmpty());
            }

            commitRow(sink, 102L, 2, "new");
            long committedSnapshot;
            try (HadoopCatalog reopened = openSaveModeCatalog(warehouse)) {
                Table written = reopened.loadTable(SAVE_MODE_TABLE);
                committedSnapshot = written.refs().get(commitBranch).snapshotId();
                Assertions.assertEquals(
                        Collections.singletonList(Arrays.asList(2, "new")),
                        readRows(written, commitBranch));
                Assertions.assertEquals(
                        "102",
                        written.snapshot(committedSnapshot)
                                .summary()
                                .get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID));
                Assertions.assertTrue(Collections.disjoint(oldSnapshots, snapshotIds(written)));
            }
            // Recovery applies schema handling only; it must not execute DROP_DATA again.
            try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
                handler.open();
                handler.handleSchemaSaveModeWithRestore();
            }
            try (HadoopCatalog reopened = openSaveModeCatalog(warehouse)) {
                Table restored = reopened.loadTable(SAVE_MODE_TABLE);
                Assertions.assertEquals(
                        committedSnapshot, restored.refs().get(commitBranch).snapshotId());
                Assertions.assertEquals(
                        Collections.singletonList(Arrays.asList(2, "new")),
                        readRows(restored, commitBranch));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    /**
     * Omits the strategy option and targets a missing branch, retaining main and historical refs.
     * The first checkpoint must append to the persisted empty branch without inheriting main data.
     *
     * @param warehouse isolated real Hadoop catalog warehouse
     * @throws Exception if catalog, writer, checkpoint commit or readback fails
     */
    @Test
    public void testDefaultDeleteCommitCreatesMissingBranch(@TempDir Path warehouse)
            throws Exception {
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        try (HadoopCatalog initial = openSaveModeCatalog(warehouse)) {
            Table original = createSaveModeTable(initial);
            commitRow(createSink(saveModeConfig(warehouse, null, false), original), 201L, 1, "old");
            original.refresh();
            long mainSnapshot = original.currentSnapshot().snapshotId();
            original.manageSnapshots()
                    .createBranch("retained-branch", mainSnapshot)
                    .createTag("retained-tag", mainSnapshot)
                    .commit();
            Map<String, SnapshotRef> oldRefs = new HashMap<>(original.refs());
            Set<Long> oldSnapshots = snapshotIds(original);
            Assertions.assertFalse(original.refs().containsKey(commitBranch));
            ReadonlyConfig config = saveModeConfig(warehouse, commitBranch, false);
            Assertions.assertEquals(
                    IcebergDropDataStrategy.DELETE_COMMIT,
                    new IcebergSinkConfig(config).getDropDataStrategy());
            IcebergSink sink = createSink(config, original);
            try (SaveModeHandler handler = sink.getSaveModeHandler().get()) {
                handler.open();
                handler.handleSaveMode();
            }
            try (HadoopCatalog reopened = openSaveModeCatalog(warehouse)) {
                Table cleared = reopened.loadTable(SAVE_MODE_TABLE);
                Assertions.assertEquals(mainSnapshot, cleared.currentSnapshot().snapshotId());
                oldRefs.forEach(
                        (name, ref) -> Assertions.assertEquals(ref, cleared.refs().get(name)));
                Assertions.assertTrue(snapshotIds(cleared).containsAll(oldSnapshots));
                Assertions.assertTrue(cleared.refs().get(commitBranch).isBranch());
                Assertions.assertTrue(readRows(cleared, commitBranch).isEmpty());
                Assertions.assertEquals(
                        Collections.singletonList(Arrays.asList(1, "old")),
                        readRows(cleared, SnapshotRef.MAIN_BRANCH));
            }
            commitRow(sink, 202L, 2, "new");
            try (HadoopCatalog reopened = openSaveModeCatalog(warehouse)) {
                Table written = reopened.loadTable(SAVE_MODE_TABLE);
                Assertions.assertEquals(
                        Collections.singletonList(Arrays.asList(2, "new")),
                        readRows(written, commitBranch));
                Assertions.assertEquals(
                        Collections.singletonList(Arrays.asList(1, "old")),
                        readRows(written, SnapshotRef.MAIN_BRANCH));
                oldRefs.forEach(
                        (name, ref) -> Assertions.assertEquals(ref, written.refs().get(name)));
                Assertions.assertTrue(snapshotIds(written).containsAll(oldSnapshots));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    /**
     * Opens an independent catalog instance so assertions read committed filesystem metadata.
     *
     * @param warehouse isolated warehouse shared only by this test lifecycle
     * @return a catalog whose resources must be closed by the caller
     */
    private HadoopCatalog openSaveModeCatalog(Path warehouse) {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("save-mode-test", saveModeCatalogProperties(warehouse));
        return catalog;
    }

    private Map<String, String> saveModeCatalogProperties(Path warehouse) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", HADOOP.getType());
        properties.put("warehouse", warehouse.toUri().toString());
        properties.put("io-impl", HadoopStreamFileIO.class.getName());
        return properties;
    }

    private Table createSaveModeTable(HadoopCatalog catalog) {
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "value", Types.StringType.get()));
        return catalog.buildTable(SAVE_MODE_TABLE, schema)
                .withPartitionSpec(PartitionSpec.builderFor(schema).identity("id").build())
                .withSortOrder(SortOrder.builderFor(schema).asc("id").build())
                .withProperty("format-version", "2")
                .withProperty("write.format.default", "parquet")
                .withProperty("save-mode-sentinel", "preserved")
                .create();
    }

    /**
     * Uses DROP_DATA through the normal sink configuration; false deliberately omits the strategy.
     *
     * @param hardReset whether to configure the explicit opt-in reset strategy
     */
    private ReadonlyConfig saveModeConfig(Path warehouse, String branch, boolean hardReset) {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "save-mode-test");
        options.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        options.put(IcebergCommonOptions.KEY_TABLE.key(), "save_mode_table");
        options.put(IcebergCommonOptions.CATALOG_PROPS.key(), saveModeCatalogProperties(warehouse));
        options.put(IcebergSinkOptions.DATA_SAVE_MODE.key(), "DROP_DATA");
        if (branch != null) {
            options.put(IcebergSinkOptions.TABLES_DEFAULT_COMMIT_BRANCH.key(), branch);
        }
        if (hardReset) {
            options.put(IcebergSinkOptions.DROP_DATA_STRATEGY.key(), "HARD_METADATA_RESET");
        }
        return ReadonlyConfig.fromMap(options);
    }

    private IcebergSink createSink(ReadonlyConfig config, Table table) {
        IcebergCatalog catalog = new IcebergCatalog("save-mode-test", config);
        CatalogTable catalogTable =
                catalog.toCatalogTable(
                        table, TablePath.of("seatunnel_namespace", "save_mode_table"));
        return new IcebergSink(config, catalogTable);
    }

    /**
     * Writes a real Parquet row and commits its checkpoint using the same public APIs as the
     * engine. The writer currently ignores its context; no engine services are required for this
     * fixture.
     *
     * @param checkpointId positive checkpoint marker asserted in persisted snapshot metadata
     * @throws Exception if writing or checkpoint commit fails
     */
    private void commitRow(IcebergSink sink, long checkpointId, int id, String value)
            throws Exception {
        SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo> committer =
                sink.createAggregatedCommitter().get();
        committer.init();
        try {
            IcebergSinkWriter writer = sink.createWriter(null);
            try {
                writer.write(new SeaTunnelRow(new Object[] {id, value}));
                IcebergCommitInfo info = writer.prepareCommit(checkpointId).get();
                Assertions.assertTrue(
                        committer
                                .commit(
                                        Collections.singletonList(
                                                committer.combine(Collections.singletonList(info))))
                                .isEmpty());
            } finally {
                writer.close();
            }
        } finally {
            committer.close();
        }
    }

    private Set<Long> snapshotIds(Table table) {
        Set<Long> ids = new HashSet<>();
        table.snapshots().forEach(snapshot -> ids.add(snapshot.snapshotId()));
        return ids;
    }

    private List<List<Object>> readRows(Table table, String branch) throws IOException {
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterable<Record> records =
                IcebergGenerics.read(table)
                        .useSnapshot(table.refs().get(branch).snapshotId())
                        .build()) {
            for (Record record : records) {
                rows.add(Arrays.asList(record.getField("id"), record.getField("value").toString()));
            }
        }
        return rows;
    }

    private boolean checkBranchExists() {
        Table table = getTable();
        Map<String, SnapshotRef> refs = table.refs();
        if (refs.containsKey(commitBranch)) {
            return true;
        }
        return false;
    }

    private List<Object> loadDataFromIcebergTableBranch() {
        List<Object> results = new ArrayList<>();
        Table table = getTable();
        long branchSnapshotId = table.refs().get(commitBranch).snapshotId();
        try (CloseableIterable<Record> records =
                IcebergGenerics.read(table).useSnapshot(branchSnapshotId).build()) {
            records.forEach(results::add);
        } catch (IOException e) {
            log.error("Table scan branch error :", e);
        }
        return results;
    }

    public Table getTable() {

        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        // Parquet 1.16 needs Hadoop 3.3+ to open Hadoop files itself, see HadoopStreamFileIO
        catalogProps.put("io-impl", HadoopStreamFileIO.class.getName());
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_sink_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        IcebergTableLoader tableLoader =
                IcebergTableLoader.create(new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)));
        tableLoader.open();
        // from branch
        return tableLoader.loadTable();
    }
}
