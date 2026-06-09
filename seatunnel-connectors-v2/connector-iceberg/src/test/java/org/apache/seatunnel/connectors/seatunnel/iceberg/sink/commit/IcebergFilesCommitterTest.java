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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergFilesCommitterTest {

    @Mock private IcebergTableLoader tableLoader;
    @Mock private Table mockTable;

    private IcebergFilesCommitter committer;

    @BeforeEach
    void setUp() {
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", "hadoop");
        catalogProps.put("warehouse", "file:///tmp/test");
        Map<String, Object> configs = new HashMap<>();
        configs.put("catalog_name", "test");
        configs.put("namespace", "ns");
        configs.put("table", "t");
        configs.put("iceberg.catalog.config", catalogProps);
        IcebergSinkConfig config = new IcebergSinkConfig(ReadonlyConfig.fromMap(configs));
        committer = IcebergFilesCommitter.of(config, tableLoader);
    }

    @Test
    void testIsAlreadyCommittedFoundByCheckpointId() {
        Snapshot s = mockSnapshotWithCheckpointId("7", null);
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(s);

        assertTrue(committer.isAlreadyCommitted(7L, Collections.emptyList()));
    }

    @Test
    void testIsAlreadyCommittedCheckpointIdMismatch() {
        Snapshot s = mockSnapshotWithCheckpointId("5", null);
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(s);

        assertFalse(committer.isAlreadyCommitted(7L, Collections.emptyList()));
    }

    @Test
    void testIsAlreadyCommittedNoSnapshots() {
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(null);

        assertFalse(committer.isAlreadyCommitted(7L, Collections.emptyList()));
    }

    @Test
    void testIsAlreadyCommittedFindsMatchAmongMultipleSnapshots() {
        Snapshot s1 = mockSnapshotWithCheckpointId("3", null);
        Snapshot s2 = mockSnapshotWithCheckpointId("7", 1L);
        Snapshot s3 = mockSnapshotWithCheckpointId("11", 2L);
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(s3);
        when(mockTable.snapshot(2L)).thenReturn(s2);
        when(mockTable.snapshot(1L)).thenReturn(s1);

        assertTrue(committer.isAlreadyCommitted(7L, Collections.emptyList()));
        assertFalse(committer.isAlreadyCommitted(9L, Collections.emptyList()));
    }

    @Test
    void testIsAlreadyCommittedSkipsSnapshotsWithoutCheckpointIdProperty() {
        Snapshot sOurs = mockSnapshotWithCheckpointId("7", null);
        Snapshot sExternal = Mockito.mock(Snapshot.class);
        Mockito.when(sExternal.summary()).thenReturn(Collections.emptyMap());
        Mockito.when(sExternal.parentId()).thenReturn(1L);
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(sExternal);
        when(mockTable.snapshot(1L)).thenReturn(sOurs);

        assertTrue(
                committer.isAlreadyCommitted(7L, Collections.emptyList()),
                "Should traverse past snapshots without checkpoint-id and find the match");
    }

    @Test
    void testIsAlreadyCommittedUsesConfiguredBranchHead() {
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", "hadoop");
        catalogProps.put("warehouse", "file:///tmp/test");
        Map<String, Object> configs = new HashMap<>();
        configs.put("catalog_name", "test");
        configs.put("namespace", "ns");
        configs.put("table", "t");
        configs.put("iceberg.table.commit-branch", "my-branch");
        configs.put("iceberg.catalog.config", catalogProps);
        IcebergSinkConfig branchConfig = new IcebergSinkConfig(ReadonlyConfig.fromMap(configs));
        IcebergFilesCommitter branchCommitter = IcebergFilesCommitter.of(branchConfig, tableLoader);

        Snapshot branchHead = mockSnapshotWithCheckpointId("7", null);
        SnapshotRef ref = Mockito.mock(SnapshotRef.class);
        Mockito.when(ref.snapshotId()).thenReturn(42L);
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.refs()).thenReturn(Collections.singletonMap("my-branch", ref));
        when(mockTable.snapshot(42L)).thenReturn(branchHead);

        assertTrue(
                branchCommitter.isAlreadyCommitted(7L, Collections.emptyList()),
                "Should traverse the configured branch, not the main branch");
    }

    @Test
    void testIsAlreadyCommittedLegacyPathNoCurrentSnapshotReturnsFalse() {
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(null);

        WriteResult r = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        assertFalse(committer.isAlreadyCommitted(0L, Collections.singletonList(r)));
    }

    @Test
    void testIsAlreadyCommittedLegacyPathEmptyResultsReturnsFalse() {
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(mock(Snapshot.class));

        assertFalse(committer.isAlreadyCommitted(0L, Collections.emptyList()));
    }

    @Test
    void testIsAlreadyCommittedLegacyPathNullDataFilesReturnsFalse() {
        when(tableLoader.loadTable()).thenReturn(mockTable);
        when(mockTable.currentSnapshot()).thenReturn(mock(Snapshot.class));

        WriteResult r = new WriteResult(null, null, null);
        assertFalse(committer.isAlreadyCommitted(0L, Collections.singletonList(r)));
    }

    @Test
    void testIsAlreadyCommittedLegacyPathFileInManifestReturnsTrue() {
        Table realTable = createInMemoryTable();
        DataFile committed = buildDataFile("in-memory://bucket/data-1.parquet");
        realTable.newAppend().appendFile(committed).commit();

        when(tableLoader.loadTable()).thenReturn(realTable);

        WriteResult result =
                new WriteResult(
                        Collections.singletonList(committed), Collections.emptyList(), null);
        assertTrue(
                committer.isAlreadyCommitted(0L, Collections.singletonList(result)),
                "File already in Iceberg manifest should be detected as committed");
    }

    @Test
    void testIsAlreadyCommittedLegacyPathFileNotInManifestReturnsFalse() {
        Table realTable = createInMemoryTable();
        DataFile committed = buildDataFile("in-memory://bucket/data-1.parquet");
        realTable.newAppend().appendFile(committed).commit();

        when(tableLoader.loadTable()).thenReturn(realTable);

        DataFile pending = buildDataFile("in-memory://bucket/data-2.parquet");
        WriteResult result =
                new WriteResult(Collections.singletonList(pending), Collections.emptyList(), null);
        assertFalse(
                committer.isAlreadyCommitted(0L, Collections.singletonList(result)),
                "File not in any manifest should not be detected as committed");
    }

    @Test
    void testDoCommitSetsCheckpointIdInSnapshotSummary() {
        Table realTable = createInMemoryTable();
        when(tableLoader.loadTable()).thenReturn(realTable);
        when(tableLoader.getTableIdentifier()).thenReturn(TableIdentifier.of("ns", "tbl"));

        DataFile dataFile = buildDataFile("in-memory://bucket/data-commit.parquet");
        WriteResult result =
                new WriteResult(Collections.singletonList(dataFile), Collections.emptyList(), null);

        committer.doCommit(Collections.singletonList(result), 42L);

        Snapshot snapshot = realTable.currentSnapshot();
        assertNotNull(snapshot);
        assertEquals(
                "42",
                snapshot.summary().get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID),
                "doCommit must record the checkpoint-id in the snapshot summary");
    }

    @Test
    void testDoCommitCheckpointIdInSummaryEnablesIdempotencyCheck() {
        Table realTable = createInMemoryTable();
        when(tableLoader.loadTable()).thenReturn(realTable);
        when(tableLoader.getTableIdentifier()).thenReturn(TableIdentifier.of("ns", "tbl"));

        DataFile dataFile = buildDataFile("in-memory://bucket/data-idem.parquet");
        WriteResult result =
                new WriteResult(Collections.singletonList(dataFile), Collections.emptyList(), null);

        committer.doCommit(Collections.singletonList(result), 7L);

        assertTrue(
                committer.isAlreadyCommitted(7L, Collections.singletonList(result)),
                "After doCommit(checkpointId=7), isAlreadyCommitted(7) must return true");
        assertFalse(
                committer.isAlreadyCommitted(8L, Collections.singletonList(result)),
                "isAlreadyCommitted with a different checkpoint-id must return false");
    }

    private Snapshot mockSnapshotWithCheckpointId(String checkpointId, Long parentId) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Map<String, String> summary = new HashMap<>();
        summary.put(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID, checkpointId);
        Mockito.lenient().when(snapshot.summary()).thenReturn(summary);
        Mockito.lenient().when(snapshot.parentId()).thenReturn(parentId);
        return snapshot;
    }

    private Table createInMemoryTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("ns"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        return catalog.createTable(
                TableIdentifier.of("ns", "tbl"), schema, PartitionSpec.unpartitioned());
    }

    private DataFile buildDataFile(String path) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path)
                .withFileSizeInBytes(128)
                .withRecordCount(10)
                .build();
    }
}
