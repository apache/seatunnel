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

import org.apache.seatunnel.shade.com.google.common.collect.Iterables;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IcebergFilesCommitterTest {
    private Table table;
    private IcebergTableLoader loader;
    private IcebergFilesCommitter committer;

    @BeforeEach
    void setUp() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("ns"));
        table =
                catalog.createTable(
                        TableIdentifier.of("ns", "tbl"),
                        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                        PartitionSpec.unpartitioned());
        loader = mock(IcebergTableLoader.class);
        when(loader.loadTable()).thenReturn(table);
        when(loader.getTableIdentifier()).thenReturn(TableIdentifier.of("ns", "tbl"));
        committer = newCommitter(null);
    }

    @Test
    void ordinaryCommitIsReplaySafeAcrossFreshCommitters() {
        List<WriteResult> payload = results(dataFile("first"), dataFile("second"));
        committer.doCommit(payload, 7);
        long snapshot = table.currentSnapshot().snapshotId();
        newCommitter(null).doCommit(results(dataFile("second"), dataFile("first")), 7);
        assertEquals(snapshot, table.currentSnapshot().snapshotId());
        assertEquals(1, Iterables.size(table.snapshots()));
        assertEquals(
                "7",
                table.currentSnapshot()
                        .summary()
                        .get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID));
        assertNotNull(
                table.currentSnapshot()
                        .summary()
                        .get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_COMMIT_ID));
    }

    @Test
    void reusedCheckpointIdDoesNotDiscardAnotherJobsFiles() {
        committer.doCommit(results(dataFile("job-a")), 1);
        List<WriteResult> second = results(dataFile("job-b"));
        assertFalse(newCommitter(null).isAlreadyCommitted(1, second));
        newCommitter(null).doCommit(second, 1);
        assertEquals(2, Iterables.size(table.snapshots()));
        assertEquals(
                20,
                table.currentSnapshot().summary().containsKey("total-records")
                        ? Long.parseLong(table.currentSnapshot().summary().get("total-records"))
                        : -1);
        committer.doCommit(results(dataFile("job-a")), 1);
        assertEquals(2, Iterables.size(table.snapshots()));
    }

    @Test
    void unknownOrChangedCheckpointIdUsesPayloadNotNumber() {
        List<WriteResult> payload = results(dataFile("first"));
        committer.doCommit(payload, 0);
        newCommitter(null).doCommit(payload, 99);
        assertEquals(1, Iterables.size(table.snapshots()));
        newCommitter(null).doCommit(results(dataFile("second")), 0);
        assertEquals(2, Iterables.size(table.snapshots()));
    }

    @Test
    void regroupedWriterResultsAndMixedOldNewUnitsDoNotDuplicateFiles() {
        WriteResult a = results(dataFile("a")).get(0);
        WriteResult b = results(dataFile("b")).get(0);
        WriteResult c = results(dataFile("c")).get(0);
        committer.doCommit(Arrays.asList(a, b), 1);
        newCommitter(null).doCommit(Collections.singletonList(a), 1);
        assertEquals(1, Iterables.size(table.snapshots()));
        newCommitter(null).doCommit(Arrays.asList(b, c), 1);
        assertEquals(2, Iterables.size(table.snapshots()));
        assertEquals("30", table.currentSnapshot().summary().get("total-records"));
        newCommitter(null).doCommit(Arrays.asList(c, b, a), 1);
        assertEquals(2, Iterables.size(table.snapshots()));
    }

    @Test
    void legacyCheckpointNumberIsNotPublicationProof() {
        table.newAppend()
                .appendFile(dataFile("legacy"))
                .set(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID, "7")
                .commit();
        assertTrue(committer.isAlreadyCommitted(7, results(dataFile("legacy"))));
        assertFalse(committer.isAlreadyCommitted(7, results(dataFile("new-job"))));
        committer.doCommit(results(dataFile("new-job")), 7);
        assertEquals(2, Iterables.size(table.snapshots()));
    }

    @Test
    void legacyUnknownCheckpointRequiresEveryFile() {
        table.newAppend().appendFile(dataFile("legacy")).commit();
        assertTrue(committer.isAlreadyCommitted(0, results(dataFile("legacy"))));
        assertThrows(
                IllegalStateException.class,
                () -> committer.doCommit(results(dataFile("legacy"), dataFile("pending")), 0));
        assertEquals(1, Iterables.size(table.snapshots()));
    }

    @Test
    void legacyRegroupingSkipsPublishedUnitsButCommitsNewUnits() {
        table.newAppend()
                .appendFile(dataFile("a"))
                .appendFile(dataFile("b"))
                .set(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID, "1")
                .commit();
        committer.doCommit(
                Arrays.asList(results(dataFile("b")).get(0), results(dataFile("c")).get(0)), 1);
        assertEquals(2, Iterables.size(table.snapshots()));
        assertEquals("30", table.currentSnapshot().summary().get("total-records"));
    }

    @Test
    void missingManifestFailsClosedInsteadOfPublishingAgain() {
        Table unreadable = mock(Table.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(loader.loadTable()).thenReturn(unreadable);
        when(unreadable.currentSnapshot()).thenReturn(snapshot);
        when(snapshot.summary())
                .thenReturn(Map.of(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID, "1"));
        when(snapshot.addedDataFiles(null))
                .thenThrow(new UncheckedIOException(new IOException("missing manifest")));
        assertThrows(
                UncheckedIOException.class,
                () -> committer.doCommit(results(dataFile("pending")), 1));
        verify(unreadable, never()).newAppend();
    }

    @Test
    void positiveCheckpointsDoNotScanLiveFilesAfterExpiration() {
        Table expiredHistory = mock(Table.class);
        Snapshot head = mock(Snapshot.class);
        when(loader.loadTable()).thenReturn(expiredHistory);
        when(expiredHistory.currentSnapshot()).thenReturn(head);
        when(head.summary())
                .thenReturn(Map.of(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID, "8"));
        when(head.parentId()).thenReturn(42L);
        for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
            assertFalse(
                    committer.isAlreadyCommitted(
                            checkpoint, results(dataFile("fresh-" + checkpoint))));
        }
        verify(head, never()).allManifests(null);
        verify(head, never()).addedDataFiles(null);
        verify(head, never()).addedDeleteFiles(null);
    }

    @Test
    void fingerprintedCheckpointReuseDoesNotReadHistoricalManifests() {
        Table published = mock(Table.class);
        Snapshot head = mock(Snapshot.class);
        when(loader.loadTable()).thenReturn(published);
        when(published.currentSnapshot()).thenReturn(head);
        when(head.parentId()).thenReturn(null);
        when(head.summary())
                .thenReturn(
                        Map.of(
                                IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID,
                                "1",
                                IcebergFilesCommitter.SNAPSHOT_PROPERTY_COMMIT_ID,
                                "v1:other-writer-result"));
        assertFalse(committer.isAlreadyCommitted(0, results(dataFile("fresh-zero-id"))));
        assertFalse(committer.isAlreadyCommitted(1, results(dataFile("fresh"))));
        verify(head, never()).allManifests(null);
        verify(head, never()).addedDataFiles(null);
        verify(head, never()).addedDeleteFiles(null);
    }

    @Test
    void markerRemainsUsefulAfterFilesAreRemovedFromLiveTable() {
        DataFile original = dataFile("original");
        List<WriteResult> payload = results(original);
        committer.doCommit(payload, 1);
        table.newDelete().deleteFile(original).commit();
        long deleted = table.currentSnapshot().snapshotId();
        newCommitter(null).doCommit(payload, 1);
        assertEquals(deleted, table.currentSnapshot().snapshotId());
    }

    @Test
    void legacyLiveFilesRemainRecognizableAfterAddingSnapshotExpires() {
        DataFile original = dataFile("legacy");
        table.newAppend().appendFile(original).commit();
        long expired = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(dataFile("later")).commit();
        table.expireSnapshots().expireSnapshotId(expired).cleanExpiredFiles(false).commit();
        assertNull(table.snapshot(expired));
        long current = table.currentSnapshot().snapshotId();
        assertTrue(committer.isAlreadyCommitted(0, results(original)));
        committer.doCommit(results(original), 0);
        assertEquals(current, table.currentSnapshot().snapshotId());
        committer.doCommit(results(dataFile("new-after-expiration")), 1);
        assertEquals("30", table.currentSnapshot().summary().get("total-records"));
    }

    @Test
    void branchDoesNotUseAnotherBranchsPublication() {
        committer.doCommit(results(dataFile("base")), 1);
        table.manageSnapshots().createBranch("other").commit();
        List<WriteResult> payload = results(dataFile("branch"));
        committer.doCommit(payload, 2);
        IcebergFilesCommitter other = newCommitter("other");
        assertFalse(other.isAlreadyCommitted(2, payload));
        other.doCommit(payload, 2);
        assertTrue(other.isAlreadyCommitted(2, payload));
        assertEquals(3, Iterables.size(table.snapshots()));
    }

    @Test
    void deletionVectorsInSamePuffinFileAreDifferentPayloads() {
        table.updateProperties().set("format-version", "3").commit();
        DataFile a = dataFile("a");
        DataFile b = dataFile("b");
        table.newAppend().appendFile(a).appendFile(b).commit();
        DeleteFile first = vector(a, 4);
        List<WriteResult> firstPayload =
                Collections.singletonList(
                        new WriteResult(
                                Collections.emptyList(), Collections.singletonList(first), null));
        committer.doCommit(firstPayload, 1);
        DeleteFile second = vector(b, 104);
        List<WriteResult> secondPayload =
                Collections.singletonList(
                        new WriteResult(
                                Collections.emptyList(), Collections.singletonList(second), null));
        assertFalse(newCommitter(null).isAlreadyCommitted(1, secondPayload));
        newCommitter(null).doCommit(secondPayload, 1);
        newCommitter(null).doCommit(firstPayload, 1);
        assertEquals(3, Iterables.size(table.snapshots()));
    }

    @Test
    void emptyPayloadIsNeverProofOfPublication() {
        committer.doCommit(results(dataFile("first")), 7);
        assertFalse(committer.isAlreadyCommitted(7, Collections.emptyList()));
        committer.doCommit(Collections.singletonList(new WriteResult(null, null, null)), 7);
        assertEquals(1, Iterables.size(table.snapshots()));
    }

    private IcebergFilesCommitter newCommitter(String branch) {
        Map<String, Object> config = new HashMap<>();
        config.put("catalog_name", "test");
        config.put("namespace", "ns");
        config.put("table", "tbl");
        config.put(
                "iceberg.catalog.config",
                Map.of("type", "hadoop", "warehouse", "file:///tmp/test"));
        if (branch != null) {
            config.put("iceberg.table.commit-branch", branch);
        }
        return IcebergFilesCommitter.of(
                new IcebergSinkConfig(ReadonlyConfig.fromMap(config)), loader);
    }

    private static List<WriteResult> results(DataFile... files) {
        return Collections.singletonList(
                new WriteResult(Arrays.asList(files), Collections.emptyList(), null));
    }

    private static DataFile dataFile(String name) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("in-memory://bucket/" + name + ".parquet")
                .withFileSizeInBytes(128)
                .withRecordCount(10)
                .build();
    }

    private static DeleteFile vector(DataFile dataFile, long offset) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("in-memory://bucket/shared.puffin")
                .withFormat("puffin")
                .withReferencedDataFile(dataFile.path())
                .withContentOffset(offset)
                .withContentSizeInBytes(100)
                .withFileSizeInBytes(400)
                .withRecordCount(1)
                .build();
    }
}
