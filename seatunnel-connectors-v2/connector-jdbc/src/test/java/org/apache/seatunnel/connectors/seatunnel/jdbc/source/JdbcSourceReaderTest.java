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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableCloseStatus;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class JdbcSourceReaderTest {

    @Test
    void shouldRecoverDroppedTableFinishedDeliveryWithoutReaderReregistration() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        SourceSplitEnumerator.Context<JdbcSourceSplit> enumContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceSplitEnumerator enumerator =
                createEnumerator(
                        enumContext,
                        new JdbcSourceState(
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                Collections.singletonMap(table, 1),
                                Collections.singletonMap(table, Collections.singleton(0))));
        AtomicInteger deliveries = new AtomicInteger();
        AtomicReference<JdbcSourceReader> target = new AtomicReference<>();
        Mockito.doAnswer(
                        invocation -> {
                            if (deliveries.getAndIncrement() > 0) {
                                // Also replay the reply: this must not queue two downstream close
                                // markers.
                                SourceEvent reply = invocation.getArgument(1);
                                target.get().handleSourceEvent(reply);
                                target.get().handleSourceEvent(reply);
                            }
                            return null;
                        })
                .when(enumContext)
                .sendEventToSourceReader(Mockito.eq(0), Mockito.any());
        AtomicBoolean ended = new AtomicBoolean();
        TestSourceReaderContext context =
                new TestSourceReaderContext() {
                    @Override
                    public void sendSourceEventToEnumerator(SourceEvent event) {
                        enumerator.handleSourceEvent(0, event);
                    }

                    @Override
                    public void signalNoMoreElement() {
                        ended.set(true);
                    }
                };
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, format);
        target.set(reader);
        reader.addSplits(
                Collections.singletonList(
                        new JdbcSourceSplit(table, "split-0", "s", null, null, null, null)));
        reader.handleNoMoreSplits();
        TestCollector collector = new TestCollector();
        reader.pollNext(collector); // read the split; lose the first reverse event
        reader.pollNext(collector); // idle reader asks the enumerator for status
        reader.pollNext(collector); // emit the reply exactly once
        Assertions.assertEquals(1, collector.closeTableEvents.size());
        Assertions.assertFalse(ended.get());
        reader.pollNext(collector);
        Assertions.assertTrue(ended.get());
        Assertions.assertEquals(2, deliveries.get());
    }

    @Test
    void shouldFailOnPermanentStatusLossButAllowSlowSiblingWithHeartbeats() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        AtomicLong clock = new AtomicLong();
        AtomicBoolean respond = new AtomicBoolean(true);
        AtomicReference<JdbcSourceReader> target = new AtomicReference<>();
        TestSourceReaderContext context =
                new TestSourceReaderContext() {
                    @Override
                    public void sendSourceEventToEnumerator(SourceEvent event) {
                        if (respond.get()) {
                            target.get()
                                    .handleSourceEvent(
                                            new JdbcTableCloseStatus.Reply(Collections.emptyMap()));
                        }
                    }
                };
        JdbcSourceReader reader =
                new JdbcSourceReader(
                        context,
                        Mockito.mock(JdbcInputFormat.class),
                        () -> clock.getAndAdd(JdbcSourceReader.STATUS_RETRY_NANOS));
        target.set(reader);
        reader.addSplits(Collections.singletonList(JdbcSourceSplit.forCloseTableState(table, 0)));
        reader.handleNoMoreSplits();
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 2 * JdbcSourceReader.MAX_UNANSWERED_STATUS_REQUESTS; i++) {
            reader.pollNext(collector);
        }
        respond.set(false);
        for (int i = 0; i < JdbcSourceReader.MAX_UNANSWERED_STATUS_REQUESTS; i++) {
            reader.pollNext(collector);
        }
        java.io.IOException timeout =
                Assertions.assertThrows(
                        java.io.IOException.class, () -> reader.pollNext(collector));
        Assertions.assertTrue(timeout.getMessage().contains(table.toString()));
        Assertions.assertTrue(collector.closeTableEvents.isEmpty());
    }

    @Test
    void shouldNotDoubleCountSplitReportAcrossCheckpointRestore() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        SourceSplitEnumerator.Context<JdbcSourceSplit> ctx =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceSplitEnumerator enumerator =
                createEnumerator(
                        ctx,
                        new JdbcSourceState(
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                Collections.singletonMap(table, 2),
                                Collections.singletonMap(table, Collections.singleton(0))));
        enumerator.handleSourceEvent(0, new JdbcSplitFinishedEvent(table, "first"));
        JdbcSourceSplitEnumerator restored = createEnumerator(ctx, enumerator.snapshotState(1L));
        restored.handleSourceEvent(0, new JdbcSplitFinishedEvent(table, "first"));
        Mockito.verify(ctx, Mockito.never())
                .sendEventToSourceReader(Mockito.anyInt(), Mockito.any());
        restored.handleSourceEvent(0, new JdbcSplitFinishedEvent(table, "second"));
        Mockito.verify(ctx)
                .sendEventToSourceReader(Mockito.eq(0), Mockito.isA(JdbcTableFinishedEvent.class));
    }

    @Test
    void shouldFinishWithoutClosingFromUntrackedLegacyCheckpointEvenAfterAnotherRestore()
            throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        SourceSplitEnumerator.Context<JdbcSourceSplit> ctx =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceSplitEnumerator legacy =
                createEnumerator(
                        ctx, new JdbcSourceState(Collections.emptyList(), Collections.emptyMap()));
        JdbcSourceSplitEnumerator recovered = createEnumerator(ctx, legacy.snapshotState(1L));
        AtomicBoolean ended = new AtomicBoolean();
        JdbcSourceReader reader =
                new JdbcSourceReader(
                        new TestSourceReaderContext() {
                            @Override
                            public void sendSourceEventToEnumerator(SourceEvent event) {
                                recovered.handleSourceEvent(0, event);
                            }

                            @Override
                            public void signalNoMoreElement() {
                                ended.set(true);
                            }
                        },
                        Mockito.mock(JdbcInputFormat.class));
        Mockito.doAnswer(
                        invocation -> {
                            reader.handleSourceEvent(invocation.getArgument(1));
                            return null;
                        })
                .when(ctx)
                .sendEventToSourceReader(Mockito.eq(0), Mockito.any());
        reader.addSplits(Collections.singletonList(JdbcSourceSplit.forCloseTableState(table, 0)));
        reader.handleNoMoreSplits();
        TestCollector collector = new TestCollector();
        reader.pollNext(collector);
        reader.pollNext(collector);
        Assertions.assertTrue(ended.get());
        Assertions.assertTrue(collector.closeTableEvents.isEmpty());
    }

    private static JdbcSourceSplitEnumerator createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> ctx, JdbcSourceState restored) {
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build();
        return new JdbcSourceSplitEnumerator(ctx, config, Collections.emptyMap(), restored);
    }

    @Test
    void shouldNotReportSplitFinishedWhenReadingFails() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(false);
        Mockito.when(format.nextRecord()).thenThrow(new IllegalStateException("incomplete split"));
        JdbcSourceReader reader = new JdbcSourceReader(context, format);
        TablePath table = TablePath.of("db", "schema", "t");
        reader.addSplits(
                Collections.singletonList(
                        new JdbcSourceSplit(table, "split-0", "select 1", null, null, null, null)));

        Assertions.assertThrows(
                IllegalStateException.class, () -> reader.pollNext(new TestCollector()));
        Mockito.verify(format).close();
        Assertions.assertTrue(
                context.sourceEvents.isEmpty(), "a failed read is not a finished split");
    }

    @Test
    void shouldNotEndBetweenTableAcknowledgementAndQueueingMarker() throws Exception {
        CountDownLatch constructingMarker = new CountDownLatch(1);
        CountDownLatch releaseMarker = new CountDownLatch(1);
        AtomicBoolean ended = new AtomicBoolean();
        TestSourceReaderContext context =
                new TestSourceReaderContext() {
                    @Override
                    public int getIndexOfSubtask() {
                        constructingMarker.countDown();
                        try {
                            Assertions.assertTrue(releaseMarker.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        return 0;
                    }

                    @Override
                    public void signalNoMoreElement() {
                        ended.set(true);
                    }
                };
        JdbcSourceReader reader =
                new JdbcSourceReader(context, Mockito.mock(JdbcInputFormat.class));
        TablePath table = TablePath.of("db", "schema", "t");
        reader.addSplits(Collections.singletonList(JdbcSourceSplit.forCloseTableState(table, 0)));
        reader.handleNoMoreSplits();
        FutureTask<Void> delivery =
                new FutureTask<>(
                        () -> {
                            reader.handleSourceEvent(new JdbcTableFinishedEvent(table, 1));
                            return null;
                        });
        new Thread(delivery).start();
        TestCollector collector = new TestCollector();
        try {
            Assertions.assertTrue(constructingMarker.await(5, TimeUnit.SECONDS));
            reader.pollNext(collector);
            Assertions.assertFalse(ended.get(), "end-of-input must not overtake the close marker");
        } finally {
            releaseMarker.countDown();
            delivery.get(5, TimeUnit.SECONDS);
        }
        reader.pollNext(collector);
        Assertions.assertEquals(1, collector.closeTableEvents.size());
        reader.pollNext(collector);
        Assertions.assertTrue(ended.get());
    }

    @Test
    void testPollNextEmitsPendingCloseTableEventBeforeReadingNextSplit() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        TestCollector collector = new TestCollector();

        reader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 1));
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.closeTableEvents.size());
        Assertions.assertEquals(tablePath, collector.closeTableEvents.get(0).getTablePath());
        Assertions.assertEquals(0, collector.closeTableEvents.get(0).getSourceSubtaskId());
        Assertions.assertEquals(1, collector.closeTableEvents.get(0).getExpectedSourceEventCount());
        Mockito.verifyNoInteractions(inputFormat);
    }

    @Test
    void testPollNextReportsSplitFinishedEventAfterSplitConsumed() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(inputFormat.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-0", "select 1", null, null, null, null);

        reader.addSplits(Collections.singletonList(split));
        reader.pollNext(new TestCollector());

        Assertions.assertEquals(1, context.sourceEvents.size());
        Assertions.assertInstanceOf(JdbcSplitFinishedEvent.class, context.sourceEvents.get(0));
        Assertions.assertEquals(
                tablePath, ((JdbcSplitFinishedEvent) context.sourceEvents.get(0)).getTablePath());
        Mockito.verify(inputFormat).open(split);
        Mockito.verify(inputFormat).close();
    }

    @Test
    void testSnapshotStateRestoresPendingCloseTableEvent() throws Exception {
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceReader reader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        reader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 2));

        List<JdbcSourceSplit> snapshot = reader.snapshotState(1L);

        Assertions.assertEquals(1, snapshot.size());
        Assertions.assertTrue(snapshot.get(0).isPendingCloseTableEvent());

        JdbcSourceReader restoredReader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        restoredReader.addSplits(snapshot);
        TestCollector collector = new TestCollector();

        restoredReader.pollNext(collector);

        Assertions.assertEquals(1, collector.closeTableEvents.size());
        Assertions.assertEquals(tablePath, collector.closeTableEvents.get(0).getTablePath());
        Assertions.assertEquals(0, collector.closeTableEvents.get(0).getSourceSubtaskId());
        Assertions.assertEquals(2, collector.closeTableEvents.get(0).getExpectedSourceEventCount());
    }

    @Test
    void testSnapshotStateRoundTripsWaitingGlobalCloseTable() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(inputFormat.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-0", "select 1", null, null, null, null);

        reader.addSplits(Collections.singletonList(split));
        reader.pollNext(new TestCollector());

        List<JdbcSourceSplit> snapshot = reader.snapshotState(1L);
        Assertions.assertEquals(1, snapshot.size());
        Assertions.assertTrue(snapshot.get(0).isCloseTableMarker());
        Assertions.assertFalse(snapshot.get(0).isPendingCloseTableEvent());

        JdbcSourceReader restoredReader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        restoredReader.addSplits(snapshot);
        List<JdbcSourceSplit> restoredSnapshot = restoredReader.snapshotState(2L);

        Assertions.assertEquals(1, restoredSnapshot.size());
        Assertions.assertTrue(restoredSnapshot.get(0).isCloseTableMarker());
        Assertions.assertFalse(restoredSnapshot.get(0).isPendingCloseTableEvent());
    }

    private static class TestSourceReaderContext
            implements org.apache.seatunnel.api.source.SourceReader.Context {
        private final List<SourceEvent> sourceEvents = new ArrayList<>();

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public org.apache.seatunnel.api.source.Boundedness getBoundedness() {
            return org.apache.seatunnel.api.source.Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
            sourceEvents.add(sourceEvent);
        }

        @Override
        public org.apache.seatunnel.api.common.metrics.MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public org.apache.seatunnel.api.event.EventListener getEventListener() {
            return null;
        }
    }

    private static final class TestCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<CloseTableEvent> closeTableEvents = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public void collect(CloseTableEvent event) {
            closeTableEvents.add(event);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
