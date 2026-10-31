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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SnapshotSplitState;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.util.LoggingContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(15)
class IncrementalSourceScanFetcherTest {
    private static final Schema KEY =
            SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    private static final Schema VALUE =
            SchemaBuilder.struct()
                    .name("test.Value")
                    .field("payload", Schema.STRING_SCHEMA)
                    .field("op", Schema.STRING_SCHEMA)
                    .build();
    private static final SnapshotSplit SPLIT =
            new SnapshotSplit("split-0", new TableId("db", null, "t"), null, null, null);

    @Test
    void reconcilesBoundedBatchesAndHoldsOneAtomicOutputUntilReleased() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestTask task =
                    fixture.start(
                            Arrays.asList(
                                    watermark(WatermarkKind.LOW),
                                    row(1, "first", "r"),
                                    row(2, "second", "r"),
                                    row(3, "third", "r"),
                                    watermark(WatermarkKind.HIGH),
                                    row(2, "updated", "u"),
                                    row(1, "delete", "d"),
                                    row(1, "reinserted", "c"),
                                    row(3, "delete", "d"),
                                    row(4, "fourth", "c")));
            fixture.drainBeforeEnd(task);
            Path directory = fixture.buffer.spillDirectory();
            assertTrue(Files.exists(directory));
            assertEquals(0, fixture.normalized.get());
            assertFalse(fixture.fetcher.isFinished());
            task.release.countDown();
            SourceRecords output = fixture.awaitOutput();
            assertEquals(5, output.getSourceRecordList().size());
            assertTrue(WatermarkEvent.isLowWatermarkEvent(output.getSourceRecordList().get(0)));
            assertTrue(WatermarkEvent.isHighWatermarkEvent(output.getSourceRecordList().get(4)));
            List<Integer> ids = new ArrayList<>();
            List<String> payloads = new ArrayList<>();
            for (Iterator<SourceRecord> iterator = output.iterator(); iterator.hasNext(); ) {
                SourceRecord record = iterator.next();
                if (!WatermarkEvent.isWatermarkEvent(record)) {
                    assertEquals(Long.valueOf(999L), record.timestamp());
                    ids.add(((Struct) record.key()).getInt32("id"));
                    payloads.add(((Struct) record.value()).getString("payload"));
                }
            }
            assertEquals(Arrays.asList(2, 1, 4), ids);
            assertEquals(Arrays.asList("updated", "reinserted", "fourth"), payloads);
            assertFalse(fixture.fetcher.pollSplitRecords().hasNext());
            assertFalse(fixture.fetcher.isFinished());
            assertThrows(IllegalStateException.class, () -> fixture.fetcher.submitTask(task));
            output.close();
            assertFalse(Files.exists(directory));
            assertNull(fixture.fetcher.pollSplitRecords());
            assertTrue(task.finished.await(5, TimeUnit.SECONDS));
            assertTrue(fixture.fetcher.isFinished());
            TestTask next =
                    fixture.start(
                            Arrays.asList(
                                    watermark(WatermarkKind.LOW), watermark(WatermarkKind.HIGH)));
            next.release.countDown();
            try (SourceRecords second = fixture.awaitOutput()) {
                assertEquals(2, second.getSourceRecordList().size());
            }
        }
    }

    @Test
    void actualEmitterReleasesSpillAfterSuccessfulEmission() throws Exception {
        try (Fixture fixture = new Fixture()) {
            SourceRecords output = fixture.completeOneRow();
            Path directory = fixture.buffer.spillDirectory();
            DebeziumDeserializationSchema<Object> deserializer =
                    mock(DebeziumDeserializationSchema.class);
            SnapshotSplitState state = fixture.emit(output, deserializer);
            verify(deserializer).deserialize(any(SourceRecord.class), any(Collector.class));
            assertNotNull(state.getLowWatermark());
            assertNotNull(state.getHighWatermark());
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void emitterReleasesSpillAfterDeserializerFailure() throws Exception {
        try (Fixture fixture = new Fixture()) {
            SourceRecords output = fixture.completeOneRow();
            Path directory = fixture.buffer.spillDirectory();
            DebeziumDeserializationSchema<Object> deserializer =
                    mock(DebeziumDeserializationSchema.class);
            IOException failure = new IOException("deserializer failure");
            doThrow(failure).when(deserializer).deserialize(any(), any());
            assertSame(
                    failure,
                    assertThrows(IOException.class, () -> fixture.emit(output, deserializer)));
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void emitterReleasesSpillAfterLazyFormatterFailure() throws Exception {
        try (Fixture fixture = new Fixture()) {
            SourceRecords output = fixture.completeOneRow();
            Path directory = fixture.buffer.spillDirectory();
            doThrow(new IllegalStateException("formatter failure"))
                    .when(fixture.context)
                    .formatMessageTimestamp(anyCollection());
            assertThrows(
                    IllegalStateException.class,
                    () -> fixture.emit(output, mock(DebeziumDeserializationSchema.class)));
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void emitterReleasesSpillAfterLazyReadFailure() throws Exception {
        try (Fixture fixture = new Fixture()) {
            SourceRecords output = fixture.completeOneRow();
            Path directory = fixture.buffer.spillDirectory();
            doReturn(
                            new AbstractCollection<SourceRecord>() {
                                @Override
                                public int size() {
                                    return 1;
                                }

                                @Override
                                public Iterator<SourceRecord> iterator() {
                                    return new Iterator<SourceRecord>() {
                                        @Override
                                        public boolean hasNext() {
                                            return true;
                                        }

                                        @Override
                                        public SourceRecord next() {
                                            throw new UncheckedIOException(
                                                    new IOException("read failure"));
                                        }
                                    };
                                }
                            })
                    .when(fixture.buffer)
                    .values();
            assertThrows(
                    UncheckedIOException.class,
                    () -> fixture.emit(output, mock(DebeziumDeserializationSchema.class)));
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void sealFailurePublishesNoOutputAndCleansSpill() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestTask task = fixture.start(oneRow());
            fixture.drainBeforeEnd(task);
            Path directory = fixture.buffer.spillDirectory();
            doThrow(new IOException("flush failure")).when(fixture.buffer).seal();
            task.release.countDown();
            assertThrows(UncheckedIOException.class, fixture::awaitOutput);
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void taskFailureBeforeEndCleansSpill() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestTask task = fixture.start(oneRow());
            fixture.drainBeforeEnd(task);
            Path directory = fixture.buffer.spillDirectory();
            task.failure = new IOException("snapshot read failure");
            task.release.countDown();
            assertThrows(RuntimeException.class, fixture::awaitOutput);
            assertFalse(Files.exists(directory));
        }
    }

    @Test
    void closeBeforeEndAndPendingOutputBothReleaseSpill() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestTask task = fixture.start(oneRow());
            fixture.drainBeforeEnd(task);
            Path directory = fixture.buffer.spillDirectory();
            fixture.fetcher.close();
            assertFalse(Files.exists(directory));
            assertTrue(task.finished.await(5, TimeUnit.SECONDS));
        }
        try (Fixture fixture = new Fixture()) {
            SourceRecords output = fixture.completeOneRow();
            Path directory = fixture.buffer.spillDirectory();
            fixture.fetcher.close();
            assertFalse(Files.exists(directory));
            assertThrows(IllegalStateException.class, output::iterator);
            output.close();
        }
    }

    private static List<SourceRecord> oneRow() {
        return Arrays.asList(
                watermark(WatermarkKind.LOW), row(1, "first", "r"), watermark(WatermarkKind.HIGH));
    }

    private static SourceRecord row(int id, String payload, String op) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.singletonMap("pos", 1L),
                "topic",
                KEY,
                new Struct(KEY).put("id", id),
                VALUE,
                new Struct(VALUE).put("payload", payload).put("op", op));
    }

    private static SourceRecord watermark(WatermarkKind kind) {
        Offset offset = mock(Offset.class);
        when(offset.getOffset()).thenReturn(Collections.singletonMap("pos", "1"));
        return WatermarkEvent.create(
                Collections.emptyMap(), "topic", SPLIT.splitId(), kind, offset);
    }

    private static final class Fixture implements AutoCloseable {
        private final ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(1))
                        .maxQueueSize(4)
                        .maxBatchSize(2)
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("test", "snapshot", "reader"))
                        .build();
        private final FetchTask.Context context = mock(FetchTask.Context.class);
        private final SnapshotBuffer buffer = spy(new SnapshotBuffer(0, 0));
        private final AtomicInteger normalized = new AtomicInteger();
        private final IncrementalSourceScanFetcher fetcher;

        private Fixture() {
            when(context.getQueue()).thenReturn(queue);
            when(context.isExactlyOnce()).thenReturn(true);
            when(context.isDataChangeRecord(any())).thenReturn(true);
            when(context.isRecordBetween(any(), any(), any())).thenReturn(true);
            when(context.formatMessageTimestamp(anyCollection()))
                    .thenAnswer(
                            invocation -> {
                                Collection<SourceRecord> rows = invocation.getArgument(0);
                                assertEquals(1, rows.size());
                                normalized.incrementAndGet();
                                SourceRecord record = rows.iterator().next();
                                return Collections.singletonList(
                                        record.newRecord(
                                                record.topic(),
                                                record.kafkaPartition(),
                                                record.keySchema(),
                                                record.key(),
                                                record.valueSchema(),
                                                record.value(),
                                                999L));
                            });
            doAnswer(
                            invocation -> {
                                Map<Struct, SourceRecord> rows = invocation.getArgument(0);
                                SourceRecord record = invocation.getArgument(1);
                                if ("d".equals(((Struct) record.value()).getString("op"))) {
                                    rows.remove(record.key());
                                } else {
                                    rows.put((Struct) record.key(), record);
                                }
                                return null;
                            })
                    .when(context)
                    .rewriteOutputBuffer(anyMap(), any());
            // A fresh owner is needed for the following split after the first owner is released.
            AtomicInteger owners = new AtomicInteger();
            fetcher =
                    new IncrementalSourceScanFetcher(
                            context,
                            0,
                            () ->
                                    owners.getAndIncrement() == 0
                                            ? buffer
                                            : new SnapshotBuffer(0, 0));
        }

        private TestTask start(List<SourceRecord> records) {
            TestTask task = new TestTask(queue, records);
            fetcher.submitTask(task);
            return task;
        }

        private void drainBeforeEnd(TestTask task) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (task.beforeEnd.getCount() != 0) {
                assertTrue(System.nanoTime() < deadline, "producer did not reach END gate");
                assertFalse(fetcher.pollSplitRecords().hasNext());
            }
            for (int i = 0; i < 3; i++) {
                assertFalse(fetcher.pollSplitRecords().hasNext());
            }
        }

        private SourceRecords awaitOutput() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Iterator<SourceRecords> output = fetcher.pollSplitRecords();
                assertNotNull(output, "split finished without atomic output");
                if (output.hasNext()) {
                    SourceRecords records = output.next();
                    assertFalse(output.hasNext());
                    return records;
                }
            }
            throw new AssertionError("snapshot output timed out");
        }

        private SourceRecords completeOneRow() throws Exception {
            TestTask task = start(oneRow());
            drainBeforeEnd(task);
            task.release.countDown();
            SourceRecords result = awaitOutput();
            return result;
        }

        private SnapshotSplitState emit(
                SourceRecords output, DebeziumDeserializationSchema<Object> deserializer)
                throws Exception {
            OffsetFactory offsets = mock(OffsetFactory.class);
            when(offsets.specific(anyMap())).thenReturn(mock(Offset.class));
            SourceReader.Context readerContext =
                    mock(SourceReader.Context.class, RETURNS_DEEP_STUBS);
            IncrementalSourceRecordEmitter<Object> emitter =
                    new IncrementalSourceRecordEmitter<>(deserializer, offsets, readerContext);
            SnapshotSplitState state = new SnapshotSplitState(SPLIT);
            emitter.emitRecord(output, mock(Collector.class), state);
            return state;
        }

        @Override
        public void close() {
            fetcher.close();
        }
    }

    private static final class TestTask implements FetchTask<SourceSplitBase> {
        private final ChangeEventQueue<DataChangeEvent> queue;
        private final List<SourceRecord> records;
        private final CountDownLatch beforeEnd = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile boolean running = true;
        private volatile boolean cancelled;
        private volatile IOException failure;

        private TestTask(ChangeEventQueue<DataChangeEvent> queue, List<SourceRecord> records) {
            this.queue = queue;
            this.records = records;
        }

        @Override
        public void execute(Context context) throws Exception {
            try {
                for (SourceRecord record : records) {
                    queue.enqueue(new DataChangeEvent(record));
                }
                beforeEnd.countDown();
                release.await();
                if (failure != null) {
                    throw failure;
                }
                if (!cancelled) {
                    queue.enqueue(new DataChangeEvent(watermark(WatermarkKind.END)));
                }
            } finally {
                running = false;
                finished.countDown();
            }
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void shutdown() {
            cancelled = true;
            release.countDown();
        }

        @Override
        public SourceSplitBase getSplit() {
            return SPLIT;
        }
    }
}
