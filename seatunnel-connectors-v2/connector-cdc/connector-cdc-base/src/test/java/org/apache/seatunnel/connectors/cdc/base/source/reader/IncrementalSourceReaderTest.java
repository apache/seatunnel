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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsBySplits;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncrementalSourceReaderTest {

    private static final TableId KEPT_TABLE =
            new TableId("alpha_online", null, "account_histories");
    private static final TableId REMOVED_TABLE =
            new TableId("alpha_online", null, "account_interests");

    @Test
    void testFetcherFailureBeforeHandoffRestoresUnemittedBatch() throws Exception {
        byte[] checkpoint;
        try (CheckpointHarness harness = new CheckpointHarness(false)) {
            harness.startAt(0);
            harness.awaitPublishedBatch();
            harness.failFetcher();

            // A successful snapshot after fetcher failure is safe if it still precedes A/B.
            checkpoint = harness.snapshot();
            assertEquals(0, checkpointPosition(checkpoint));
            assertTrue(harness.emitted.isEmpty());
            assertFetcherFailure(harness);
        }
        assertRestoredRecords(checkpoint, Arrays.asList(1, 2));
    }

    @Test
    void testFetcherFailureWithPartiallyEmittedFetchRestoresRemainingRecords() throws Exception {
        byte[] checkpoint;
        try (CheckpointHarness harness = new CheckpointHarness(true)) {
            harness.startAt(0);
            harness.awaitPublishedBatch();
            harness.reader.pollNext(harness.output);
            assertEquals(Collections.singletonList(1), harness.emitted);
            harness.failFetcher();

            checkpoint = harness.snapshot();
            assertEquals(1, checkpointPosition(checkpoint));

            // currentFetch can still be drained; its records have not become checkpointed early.
            harness.reader.pollNext(harness.output);
            assertEquals(Arrays.asList(1, 2), harness.emitted);
            assertFetcherFailure(harness);
        }
        assertRestoredRecords(checkpoint, Collections.singletonList(2));
    }

    @Test
    void testFetcherFailureDuringSnapshotRestoresOnlyEmittedPosition() throws Exception {
        byte[] checkpoint;
        try (CheckpointHarness harness = new CheckpointHarness(true)) {
            harness.startAt(0);
            harness.awaitPublishedBatch();
            harness.reader.pollNext(harness.output);
            // Metadata is captured after SourceReaderBase has copied the live split states.
            Mockito.when(harness.schema.getProducedType())
                    .thenAnswer(
                            invocation -> {
                                harness.failFetcher();
                                return Collections.emptyList();
                            });

            checkpoint = harness.snapshot();
            assertEquals(1, checkpointPosition(checkpoint));
            assertEquals(Collections.singletonList(1), harness.emitted);
        }
        assertRestoredRecords(checkpoint, Collections.singletonList(2));
    }

    @Test
    void testHealthySnapshotDoesNotCheckpointQueuedRecords() throws Exception {
        byte[] queuedCheckpoint;
        byte[] emittedCheckpoint;
        try (CheckpointHarness harness = new CheckpointHarness(false)) {
            harness.startAt(0);
            harness.awaitPublishedBatch();
            queuedCheckpoint = harness.snapshot();
            assertEquals(0, checkpointPosition(queuedCheckpoint));
            harness.reader.pollNext(harness.output);
            assertEquals(Arrays.asList(1, 2), harness.emitted);
            emittedCheckpoint = harness.snapshot();
            assertEquals(2, checkpointPosition(emittedCheckpoint));
            assertEquals(0, checkpointPosition(queuedCheckpoint));
        }
        assertRestoredRecords(queuedCheckpoint, Arrays.asList(1, 2));
        assertRestoredRecords(emittedCheckpoint, Collections.emptyList());
    }

    @Test
    void testCheckpointLockExcludesSnapshotDuringBatchHandoff() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch handoffStarted = new CountDownLatch(1);
        CountDownLatch finishHandoff = new CountDownLatch(1);
        CountDownLatch snapshotAttempted = new CountDownLatch(1);
        AtomicReference<Thread> pollingThread = new AtomicReference<>();
        AtomicReference<Thread> snapshotThread = new AtomicReference<>();
        byte[] checkpoint;
        try (CheckpointHarness harness = new CheckpointHarness(false)) {
            harness.startAt(0);
            harness.awaitPublishedBatch();
            Collector<Integer> blockingOutput =
                    new Collector<Integer>() {
                        @Override
                        public void collect(Integer record) {
                            if (record == 1) {
                                handoffStarted.countDown();
                                await(finishHandoff);
                            }
                            harness.output.collect(record);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return harness.output.getCheckpointLock();
                        }
                    };
            Future<?> poll =
                    executor.submit(
                            () -> {
                                pollingThread.set(Thread.currentThread());
                                harness.reader.pollNext(blockingOutput);
                                return null;
                            });
            await(handoffStarted);
            harness.failFetcher();
            Future<byte[]> snapshot =
                    executor.submit(
                            () -> {
                                snapshotThread.set(Thread.currentThread());
                                snapshotAttempted.countDown();
                                return harness.snapshot();
                            });
            await(snapshotAttempted);
            ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                ThreadInfo info = threads.getThreadInfo(snapshotThread.get().getId());
                if (info != null
                        && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockOwnerId() == pollingThread.get().getId()
                        && info.getLockInfo().getIdentityHashCode()
                                == System.identityHashCode(harness.output.getCheckpointLock())) {
                    break;
                }
                assertTrue(
                        System.nanoTime() < deadline,
                        "Snapshot did not block on the collector checkpoint lock: " + info);
                assertFalse(Thread.currentThread().isInterrupted());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            finishHandoff.countDown();
            poll.get(10, TimeUnit.SECONDS);
            checkpoint = snapshot.get(10, TimeUnit.SECONDS);
            assertEquals(Arrays.asList(1, 2), harness.emitted);
            assertEquals(2, checkpointPosition(checkpoint));
        } finally {
            finishHandoff.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertRestoredRecords(checkpoint, Collections.emptyList());
    }

    private static void assertFetcherFailure(CheckpointHarness harness) {
        RuntimeException failure =
                assertThrows(RuntimeException.class, () -> harness.reader.pollNext(harness.output));
        assertEquals("One or more fetchers have encountered exception", failure.getMessage());
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertSame(harness.failure, cause);
    }

    private static int checkpointPosition(byte[] checkpoint) throws IOException {
        SourceSplitBase split = new DefaultSerializer<SourceSplitBase>().deserialize(checkpoint);
        return Integer.parseInt(
                split.asIncrementalSplit().getStartupOffset().getOffset().get("pos"));
    }

    private static void assertRestoredRecords(byte[] checkpoint, List<Integer> expected)
            throws Exception {
        try (CheckpointHarness restored = new CheckpointHarness(false)) {
            SourceSplitBase split =
                    new DefaultSerializer<SourceSplitBase>().deserialize(checkpoint);
            restored.reader.addSplits(Collections.singletonList(split));
            restored.awaitPublishedBatch();
            assertEquals(checkpointPosition(checkpoint), checkpointPosition(restored.snapshot()));
            restored.reader.pollNext(restored.output);
            assertEquals(expected, restored.emitted);
            assertEquals(2, checkpointPosition(restored.snapshot()));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(
                    latch.await(10, TimeUnit.SECONDS), "Timed out waiting for reader/fetcher gate");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted waiting for reader/fetcher gate", e);
        }
    }

    private static final class CheckpointHarness implements AutoCloseable {
        private static final String SPLIT_ID = "incremental-split-0";
        private final CountDownLatch publishedBatch = new CountDownLatch(1);
        private final CountDownLatch releaseFetcher = new CountDownLatch(1);
        private final CountDownLatch fetcherClosed = new CountDownLatch(1);
        private final AtomicInteger candidatePosition = new AtomicInteger();
        private final IOException failure = new IOException("injected fatal fetch failure");
        private volatile boolean failing;
        private final List<Integer> emitted = new ArrayList<>();
        private final Collector<Integer> output =
                new Collector<Integer>() {
                    @Override
                    public void collect(Integer record) {
                        assertTrue(Thread.holdsLock(getCheckpointLock()));
                        emitted.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        private final DebeziumDeserializationSchema<Integer> schema;
        private final IncrementalSourceReader<Integer, SourceConfig> reader;

        @SuppressWarnings("unchecked")
        private CheckpointHarness(boolean separateRecordGroups) throws Exception {
            SourceReader.Context context =
                    Mockito.mock(SourceReader.Context.class, Mockito.RETURNS_DEEP_STUBS);
            schema = Mockito.mock(DebeziumDeserializationSchema.class);
            Mockito.doAnswer(
                            invocation -> {
                                SourceRecord record = invocation.getArgument(0);
                                Collector<Integer> collector = invocation.getArgument(1);
                                collector.collect(
                                        ((Number) record.sourceOffset().get("pos")).intValue());
                                return null;
                            })
                    .when(schema)
                    .deserialize(Mockito.any(), Mockito.any());
            OffsetFactory offsets = Mockito.mock(OffsetFactory.class);
            Mockito.when(offsets.specific(Mockito.anyMap()))
                    .thenAnswer(
                            invocation -> {
                                Map<String, String> offset = invocation.getArgument(0);
                                return new TestOffset(Integer.parseInt(offset.get("pos")));
                            });
            IncrementalSourceSplitReader<SourceConfig> splitReader =
                    Mockito.mock(IncrementalSourceSplitReader.class);
            Mockito.doAnswer(
                            invocation -> {
                                SplitsChange<SourceSplitBase> change = invocation.getArgument(0);
                                candidatePosition.set(
                                        Integer.parseInt(
                                                change.splits()
                                                        .iterator()
                                                        .next()
                                                        .asIncrementalSplit()
                                                        .getStartupOffset()
                                                        .getOffset()
                                                        .get("pos")));
                                return null;
                            })
                    .when(splitReader)
                    .handleSplitsChanges(Mockito.any());
            Mockito.when(splitReader.fetch())
                    .thenAnswer(
                            invocation -> {
                                List<SourceRecords> groups = new ArrayList<>();
                                List<SourceRecord> records = new ArrayList<>();
                                for (int position = candidatePosition.get() + 1;
                                        position <= 2;
                                        position++) {
                                    records.add(dataRecord(position));
                                    if (separateRecordGroups) {
                                        groups.add(new SourceRecords(new ArrayList<>(records)));
                                        records.clear();
                                    }
                                }
                                if (!separateRecordGroups) {
                                    groups.add(new SourceRecords(records));
                                }
                                candidatePosition.set(2);
                                return new RecordsBySplits<>(
                                        Collections.singletonMap(SPLIT_ID, groups),
                                        Collections.emptySet());
                            })
                    .thenAnswer(
                            invocation -> {
                                // FetchTask has enqueued the previous result before requesting the
                                // next batch.
                                publishedBatch.countDown();
                                await(releaseFetcher);
                                if (failing) {
                                    throw failure;
                                }
                                return new RecordsBySplits<>(
                                        Collections.emptyMap(), Collections.emptySet());
                            });
            Mockito.doAnswer(
                            invocation -> {
                                releaseFetcher.countDown();
                                return null;
                            })
                    .when(splitReader)
                    .wakeUp();
            Mockito.doAnswer(
                            invocation -> {
                                // SplitFetcher records its fatal error before closing the split
                                // reader.
                                fetcherClosed.countDown();
                                return null;
                            })
                    .when(splitReader)
                    .close();
            reader =
                    new IncrementalSourceReader<>(
                            Mockito.mock(DataSourceDialect.class),
                            new ArrayBlockingQueue<>(2),
                            () -> splitReader,
                            new IncrementalSourceRecordEmitter<>(schema, offsets, context),
                            new SourceReaderOptions(ReadonlyConfig.fromMap(Collections.emptyMap())),
                            context,
                            Mockito.mock(SourceConfig.class),
                            schema);
        }

        private void startAt(int position) {
            reader.addSplits(
                    Collections.singletonList(
                            new IncrementalSplit(
                                    SPLIT_ID,
                                    Collections.singletonList(KEPT_TABLE),
                                    new TestOffset(position),
                                    null,
                                    Collections.emptyList())));
        }

        private void awaitPublishedBatch() {
            await(publishedBatch);
            assertEquals(2, candidatePosition.get());
        }

        private void failFetcher() {
            failing = true;
            releaseFetcher.countDown();
            await(fetcherClosed);
        }

        private byte[] snapshot() throws IOException {
            // Engine-style checkpoint exclusion; Flink also serializes poll/snapshot in its
            // mailbox.
            synchronized (output.getCheckpointLock()) {
                List<SourceSplitBase> splits = reader.snapshotState(1L);
                assertEquals(1, splits.size());
                return new DefaultSerializer<SourceSplitBase>().serialize(splits.get(0));
            }
        }

        @Override
        public void close() {
            reader.close();
            await(fetcherClosed);
        }
    }

    private static SourceRecord dataRecord(int position) {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("test.Envelope")
                        .field("op", Schema.STRING_SCHEMA)
                        .build();
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("pos", position),
                "test",
                null,
                null,
                valueSchema,
                new Struct(valueSchema).put("op", "c"));
    }

    private static final class TestOffset extends Offset {
        private TestOffset(int position) {
            offset = Collections.singletonMap("pos", Integer.toString(position));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    longOffsetValue(offset, "pos"), longOffsetValue(other.getOffset(), "pos"));
        }
    }

    @Test
    void testAddSplitsEnqueuesPrunedRestoredIncrementalSplit() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.singletonList(KEPT_TABLE));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(1, state.size());
            Assertions.assertEquals(
                    Collections.singletonList(KEPT_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsKeepsRestoredSplitWhenDiscoveryReturnsEmpty() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.emptyList());
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(
                    Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsKeepsRestoredSplitWhenDiscoveryFails() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenThrow(new RuntimeException("database unavailable"));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(
                    Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsDiscoversCapturedTablesOnlyOncePerBatch() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.singletonList(KEPT_TABLE));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(
                    Arrays.asList(
                            restoredIncrementalSplit(),
                            restoredIncrementalSplit("incremental-split-1")));

            Mockito.verify(dialect, Mockito.times(1)).discoverDataCollections(sourceConfig);
        } finally {
            reader.close();
        }
    }

    private static IncrementalSourceReader<Object, SourceConfig> createReader(
            DataSourceDialect<SourceConfig> dialect,
            SourceConfig sourceConfig,
            SourceReader.Context context) {
        @SuppressWarnings("unchecked")
        IncrementalSourceSplitReader<SourceConfig> splitReader =
                Mockito.mock(IncrementalSourceSplitReader.class);
        CountDownLatch wakeUp = new CountDownLatch(1);
        try {
            Mockito.when(splitReader.fetch())
                    .thenAnswer(
                            invocation -> {
                                wakeUp.await();
                                return null;
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Mockito.doAnswer(
                        invocation -> {
                            wakeUp.countDown();
                            return null;
                        })
                .when(splitReader)
                .wakeUp();

        @SuppressWarnings("unchecked")
        RecordEmitter<SourceRecords, Object, SourceSplitStateBase> recordEmitter =
                Mockito.mock(RecordEmitter.class);
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> deserializationSchema =
                Mockito.mock(DebeziumDeserializationSchema.class);

        return new IncrementalSourceReader<>(
                dialect,
                new ArrayBlockingQueue<RecordsWithSplitIds<SourceRecords>>(2),
                () -> splitReader,
                recordEmitter,
                new SourceReaderOptions(ReadonlyConfig.fromMap(Collections.emptyMap())),
                context,
                sourceConfig,
                deserializationSchema);
    }

    private static IncrementalSplit restoredIncrementalSplit() {
        return restoredIncrementalSplit("incremental-split-0");
    }

    private static IncrementalSplit restoredIncrementalSplit(String splitId) {
        Map<TableId, byte[]> historyTableChanges = new HashMap<>();
        historyTableChanges.put(KEPT_TABLE, new byte[] {1});
        historyTableChanges.put(REMOVED_TABLE, new byte[] {2});
        return new IncrementalSplit(
                splitId,
                Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                null,
                null,
                Collections.emptyList(),
                Arrays.asList(catalogTable(KEPT_TABLE), catalogTable(REMOVED_TABLE)),
                historyTableChanges);
    }

    private static CatalogTable catalogTable(TableId tableId) {
        TablePath tablePath = TablePath.of(tableId.catalog(), tableId.table());
        return CatalogTable.of(
                TableIdentifier.of("test", tablePath),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    @Test
    void restoreCheckpointStateRestoresTablesAndHistoryFromNewCheckpointFormat() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        CatalogTable checkpointTable = Mockito.mock(CatalogTable.class);
        // Stub the table path because restoreCheckpointState logs restored table paths and
        // dereferences CatalogTable#getTablePath unconditionally.
        Mockito.when(checkpointTable.getTablePath())
                .thenReturn(TablePath.of("catalog", "database", "new_table"));
        List<CatalogTable> checkpointTables = Collections.singletonList(checkpointTable);
        Map<TableId, byte[]> historyTableChanges =
                Collections.singletonMap(
                        new TableId("catalog", "database", "new_table"), new byte[] {1});
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointTables,
                        historyTableChanges);

        IncrementalSourceReader.restoreCheckpointState(incrementalSplit, schema);

        Mockito.verify(schema).restoreCheckpointProducedType(checkpointTables);
        Mockito.verify(schema).restoreCheckpointHistoryTableChanges(historyTableChanges);
    }

    @Test
    void restoreCheckpointStateIgnoresEmptyLegacyState() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList());

        IncrementalSourceReader.restoreCheckpointState(incrementalSplit, schema);

        Mockito.verifyNoInteractions(schema);
    }

    @Test
    void restoreCheckpointStateRestoresLegacyCheckpointDataType() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        SeaTunnelRowType checkpointRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(new TableId("catalog", "database", "customers")),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointRowType);

        IncrementalSourceReader.restoreCheckpointState(split, schema);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CatalogTable>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(schema).restoreCheckpointProducedType(captor.capture());
        List<CatalogTable> restoredTables = captor.getValue();
        assertEquals(1, restoredTables.size());
        assertArrayEquals(
                checkpointRowType.getFieldNames(),
                restoredTables.get(0).getSeaTunnelRowType().getFieldNames());
        assertEquals(
                "catalog.database.customers", restoredTables.get(0).getTablePath().getFullName());
    }

    @Test
    void restoreCheckpointStateRestoresLegacyMultipleCheckpointTables() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        Map<String, SeaTunnelRowType> rowTypeMap = new LinkedHashMap<>();
        rowTypeMap.put(
                "catalog.database.customers",
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE}));
        rowTypeMap.put(
                "orders",
                new SeaTunnelRowType(
                        new String[] {"order_id"}, new SeaTunnelDataType[] {BasicType.LONG_TYPE}));
        MultipleRowType checkpointRowType = new MultipleRowType(rowTypeMap);
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointRowType);

        IncrementalSourceReader.restoreCheckpointState(split, schema);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CatalogTable>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(schema).restoreCheckpointProducedType(captor.capture());
        List<CatalogTable> restoredTables = captor.getValue();
        assertEquals(2, restoredTables.size());
        assertEquals(
                "catalog.database.customers", restoredTables.get(0).getTablePath().getFullName());
        assertEquals("orders", restoredTables.get(1).getTablePath().getFullName());
    }

    @Test
    void restoreCheckpointStateSkipsLegacyCheckpointDataTypeWithoutRecoverableTableIdentity() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        SeaTunnelRowType checkpointRowType =
                new SeaTunnelRowType(
                        new String[] {"id"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE
                        });
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointRowType);

        IncrementalSourceReader.restoreCheckpointState(split, schema);

        Mockito.verifyNoInteractions(schema);
    }
}
