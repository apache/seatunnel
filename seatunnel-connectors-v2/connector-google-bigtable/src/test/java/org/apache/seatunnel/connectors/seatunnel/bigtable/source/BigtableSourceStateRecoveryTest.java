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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep regression tests for Bigtable source checkpoint / recovery semantics introduced in #11144.
 */
class BigtableSourceStateRecoveryTest {

    /**
     * Bytes from the pre-#11144 {@code BigtableSourceState} (assignedSplits only, same
     * serialVersionUID) for split {@code bigtable_source_split_0} with row range [start, end).
     */
    private static final String LEGACY_ASSIGNED_ONLY_STATE_BASE64 =
            "rO0ABXNyAE1vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5iaWd0YWJsZS5zb3VyY2UuQmlndGFibGVTb3VyY2VTdGF0ZQAAAAAAAAABAgABTAAOYXNzaWduZWRTcGxpdHN0AA9MamF2YS91dGlsL1NldDt4cHNyABFqYXZhLnV0aWwuSGFzaFNldLpEhZWWuLc0AwAAeHB3DAAAABA/QAAAAAAAAXNyAE1vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5iaWd0YWJsZS5zb3VyY2UuQmlndGFibGVTb3VyY2VTcGxpdAAAAAAAAAABAgADTAAJZW5kUm93S2V5dAASTGphdmEvbGFuZy9TdHJpbmc7TAAHc3BsaXRJZHEAfgAGTAALc3RhcnRSb3dLZXlxAH4ABnhwdAADZW5kdAAXYmlndGFibGVfc291cmNlX3NwbGl0XzB0AAVzdGFydHg=";

    private static final BigtableParameters PARAMETERS =
            BigtableParameters.builder()
                    .projectId("test-project")
                    .instanceId("test-instance")
                    .table("test-table")
                    .build();

    /**
     * After eager open(), the first snapshot already contains discovered pending splits (assigned
     * still empty). Restoring that state must reassign the pending split without re-running
     * discovery.
     */
    @Test
    void testCheckpointAfterEagerOpenRestoresPendingSplitsWithoutRediscovery() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator enumerator = newEnumerator(context);
        enumerator.open();

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertTrue(checkpoint.getAssignedSplits().isEmpty());
        // open() eagerly discovers splits, so pendingSplits is non-empty at first snapshot.
        assertEquals(1, checkpoint.getPendingSplits().size());

        BigtableSourceSplitEnumerator restored = restoreEnumerator(context, checkpoint);
        restored.open();

        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(1, context.getAssignedSplitCount(0));
        assertEquals("bigtable_source_split_0", context.getLastAssignedSplit(0).splitId());
    }

    /**
     * Restoring a genuinely empty enumerator checkpoint (both assigned and pending empty) must
     * still discover splits on {@code open()}. This covers a barrier that raced open()'s RPC and
     * produced an empty snapshot, as well as restore from a pre-discovery historical checkpoint.
     */
    @Test
    void testTrulyEmptyCheckpointRestoreStillDiscoversSplitsOnOpen() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableSourceState emptyState =
                new BigtableSourceState(Collections.emptySet(), Collections.emptySet());

        BigtableSourceSplitEnumerator restored = restoreEnumerator(context, emptyState);
        // Constructor must treat empty-empty as not initialized so open() rediscovers.
        restored.open();
        assertEquals(1, restored.currentUnassignedSplitSize());

        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(1, context.getAssignedSplitCount(0));
        assertEquals("bigtable_source_split_0", context.getLastAssignedSplit(0).splitId());
        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    /**
     * Documents Daniel's blocker: without pendingSplits in checkpoint state, a returned split whose
     * ID is already in assignedSplits is dropped after restore.
     */
    @Test
    void testReturnedSplitDroppedWhenCheckpointOmitsPending() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator enumerator = newEnumerator(context);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);

        context.unregisterReaderForTest(0);
        enumerator.addSplitsBack(Collections.singletonList(split), 0);
        assertEquals(1, enumerator.currentUnassignedSplitSize());

        // Simulate pre-fix snapshot: only assignedSplits, pending lost.
        BigtableSourceState buggyCheckpoint =
                new BigtableSourceState(
                        new HashSet<>(Collections.singleton(split)), Collections.emptySet());

        TestingContext restoreContext = new TestingContext(1);
        BigtableSourceSplitEnumerator restored = restoreEnumerator(restoreContext, buggyCheckpoint);
        restored.open();

        restoreContext.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(
                0,
                restoreContext.getAssignedSplitCount(0),
                "Without pendingSplits in checkpoint, returned split is dropped on restore");
    }

    /**
     * Checkpoints written before #11144 only serialized assignedSplits. After Java deserialization
     * into the new class shape, pendingSplits is null; restore must not fail.
     */
    @Test
    void testLegacyCheckpointWithNullPendingSplitsRestoresEnumerator() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "start", "end");
        BigtableSourceState legacyState =
                new BigtableSourceState(
                        new HashSet<>(Collections.singleton(split)), Collections.emptySet());
        java.lang.reflect.Field pendingField =
                BigtableSourceState.class.getDeclaredField("pendingSplits");
        pendingField.setAccessible(true);
        pendingField.set(legacyState, null);

        assertTrue(legacyState.getPendingSplits().isEmpty());

        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator restored = restoreEnumerator(context, legacyState);
        restored.open();

        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    /**
     * Round-trip bytes produced by the pre-#11144 single-field {@code BigtableSourceState} shape
     * (same {@code serialVersionUID}) must deserialize into the new class without restore failure.
     */
    @Test
    void testDeserializePrePatchBigtableSourceStateBytes() throws Exception {
        byte[] legacyBytes = Base64.getDecoder().decode(LEGACY_ASSIGNED_ONLY_STATE_BASE64);

        BigtableSourceState deserialized;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(legacyBytes))) {
            deserialized = (BigtableSourceState) ois.readObject();
        }

        assertEquals(1, deserialized.getAssignedSplits().size());
        assertTrue(deserialized.getPendingSplits().isEmpty());

        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator restored = restoreEnumerator(context, deserialized);
        restored.open();

        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    /**
     * End-to-end reader + enumerator failover: in-flight reader split flows back via addSplitsBack.
     */
    @Test
    void testReaderEnumeratorFailoverHandoff() throws Exception {
        TestingContext enumContext = new TestingContext(1);
        BigtableSourceSplitEnumerator enumerator = newEnumerator(enumContext);
        enumerator.open();
        enumContext.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit enumeratorSplit = enumContext.getLastAssignedSplit(0);

        BigtableSourceReader reader = createReaderWithMockedEmptyStream();
        reader.addSplits(Collections.singletonList(enumeratorSplit));
        reader.handleNoMoreSplits();

        final AtomicReference<List<BigtableSourceSplit>> readerCheckpoint = new AtomicReference<>();
        mockEmptyReadStream(reader, () -> readerCheckpoint.set(reader.snapshotState(1L)));

        Collector<SeaTunnelRow> collector = mockCollector();
        reader.pollNext(collector);

        List<BigtableSourceSplit> returnedSplits = readerCheckpoint.get();
        assertNotNull(returnedSplits);
        assertEquals(1, returnedSplits.size());

        enumContext.unregisterReaderForTest(0);
        enumerator.addSplitsBack(returnedSplits, 0);

        BigtableSourceState enumCheckpoint = enumerator.snapshotState(2L);
        assertTrue(enumCheckpoint.getPendingSplits().contains(enumeratorSplit));

        TestingContext restoreContext = new TestingContext(1);
        BigtableSourceSplitEnumerator restoredEnum =
                restoreEnumerator(restoreContext, enumCheckpoint);
        restoredEnum.open();
        restoreContext.registerReaderForTest(0);
        restoredEnum.registerReader(0);

        assertEquals(1, restoreContext.getAssignedSplitCount(0));
        assertEquals(enumeratorSplit.splitId(), restoreContext.getLastAssignedSplit(0).splitId());
    }

    /** Concurrent reader snapshot and addSplits must remain thread-safe (no torn exceptions). */
    @Test
    void testConcurrentReaderSnapshotAndAddSplits() throws Exception {
        BigtableSourceReader reader = createReaderWithMockedEmptyStream();
        BigtableSourceSplit splitA = new BigtableSourceSplit(0, "a", "m");
        BigtableSourceSplit splitB = new BigtableSourceSplit(1, "m", "z");

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<List<BigtableSourceSplit>> snapshots = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < 100; i++) {
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            snapshots.add(reader.snapshotState(1L));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            reader.addSplits(Collections.singletonList(splitA));
                            reader.addSplits(Collections.singletonList(splitB));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(100, snapshots.size());
        for (List<BigtableSourceSplit> snapshot : snapshots) {
            assertNotNull(snapshot);
        }
    }

    @Test
    void testBigtableSourceStateJavaSerializationRoundTrip() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "start", "end");
        BigtableSourceState original =
                new BigtableSourceState(
                        new HashSet<>(Collections.singleton(split)),
                        new HashSet<>(Collections.singleton(split)));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(original);
        }

        BigtableSourceState restored;
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            restored = (BigtableSourceState) ois.readObject();
        }

        assertEquals(1, restored.getAssignedSplits().size());
        assertEquals(1, restored.getPendingSplits().size());
        assertEquals(
                "bigtable_source_split_0",
                restored.getAssignedSplits().iterator().next().splitId());
        restored.getAssignedSplits().clear();
        assertFalse(
                original.getAssignedSplits().isEmpty(),
                "Defensive copy must isolate mutations on assignedSplits");
    }

    @Test
    void testAddSplitsBackWithRegisteredReaderImmediatelyReassigns() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator enumerator = newEnumerator(context);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);

        context.clearAssignments();
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        assertEquals(0, enumerator.currentUnassignedSplitSize());
        assertEquals(1, context.getAssignedSplitCount(0));

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertTrue(checkpoint.getPendingSplits().isEmpty());
        assertTrue(checkpoint.getAssignedSplits().contains(split));
    }

    private static BigtableSourceSplitEnumerator newEnumerator(
            SourceSplitEnumerator.Context<BigtableSourceSplit> context) {
        return new BigtableSourceSplitEnumerator(context, PARAMETERS, null, emptySampleClient());
    }

    private static BigtableSourceSplitEnumerator restoreEnumerator(
            SourceSplitEnumerator.Context<BigtableSourceSplit> context,
            BigtableSourceState sourceState) {
        return new BigtableSourceSplitEnumerator(
                context, PARAMETERS, sourceState, emptySampleClient());
    }

    /**
     * Empty samples force the single-split fallback so this class keeps its one-split restore
     * assertions.
     */
    private static BigtableClient emptySampleClient() {
        BigtableClient client = mock(BigtableClient.class);
        when(client.sampleRowKeys()).thenReturn(Collections.emptyList());
        return client;
    }

    @SuppressWarnings("unchecked")
    private static BigtableSourceReader createReaderWithMockedEmptyStream() throws Exception {
        BigtableClient mockClient = mock(BigtableClient.class);
        BigtableDataClient mockDataClient = mock(BigtableDataClient.class);
        when(mockClient.getDataClient()).thenReturn(mockDataClient);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"rowkey"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE
                        });

        BigtableSourceReader reader =
                new BigtableSourceReader(
                        PARAMETERS, mock(SourceReader.Context.class), rowType, mockClient);
        reader.open();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private static void mockEmptyReadStream(BigtableSourceReader reader, Runnable duringForEach)
            throws Exception {
        BigtableClient mockClient = mock(BigtableClient.class);
        BigtableDataClient mockDataClient = mock(BigtableDataClient.class);
        when(mockClient.getDataClient()).thenReturn(mockDataClient);

        ServerStream<com.google.cloud.bigtable.data.v2.models.Row> fakeStream =
                mock(ServerStream.class);
        Mockito.doAnswer(
                        invocation -> {
                            if (duringForEach != null) {
                                duringForEach.run();
                            }
                            return null;
                        })
                .when(fakeStream)
                .forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);

        // Re-bind mocked data client on the already-opened reader
        java.lang.reflect.Field clientField =
                BigtableSourceReader.class.getDeclaredField("bigtableClient");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);
    }

    private static Collector<SeaTunnelRow> mockCollector() {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        return collector;
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<BigtableSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders = new HashSet<>();
        private final Map<Integer, List<BigtableSourceSplit>> assignments = new HashMap<>();

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        void registerReaderForTest(int subtaskId) {
            registeredReaders.add(subtaskId);
        }

        void unregisterReaderForTest(int subtaskId) {
            registeredReaders.remove(subtaskId);
        }

        void clearAssignments() {
            assignments.clear();
        }

        int getAssignedSplitCount(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
        }

        BigtableSourceSplit getLastAssignedSplit(int subtaskId) {
            List<BigtableSourceSplit> splits = assignments.get(subtaskId);
            return splits.get(splits.size() - 1);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<BigtableSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
