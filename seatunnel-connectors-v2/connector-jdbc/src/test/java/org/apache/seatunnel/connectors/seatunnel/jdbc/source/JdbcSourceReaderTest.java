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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
    void stalePositivePushCannotOvertakeARealRestoredSplitWithoutAMarker() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        JdbcSourceSplit split =
                new JdbcSourceSplit(table, "split-0", "select 1", null, null, null, null);
        SourceSplitEnumerator.Context<JdbcSourceSplit> ctx =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceSplitEnumerator restoredEnum =
                createEnumerator(
                        ctx,
                        new JdbcSourceState(
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                Collections.singletonMap(table, 1),
                                Collections.singletonMap(table, Collections.singleton(0))));
        AtomicBoolean ended = new AtomicBoolean();
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(false, true);
        Mockito.when(format.nextRecord()).thenReturn(new SeaTunnelRow(new Object[] {1}));
        AtomicLong clock = new AtomicLong();
        JdbcSourceReader restored =
                new JdbcSourceReader(
                        new TestSourceReaderContext() {
                            @Override
                            public void sendSourceEventToEnumerator(SourceEvent event) {
                                restoredEnum.handleSourceEvent(0, event);
                            }

                            @Override
                            public void signalNoMoreElement() {
                                ended.set(true);
                            }
                        },
                        format,
                        () -> clock.getAndAdd(JdbcSourceReader.STATUS_RETRY_NANOS));
        Mockito.doAnswer(
                        call -> {
                            restored.handleSourceEvent(call.getArgument(1));
                            return null;
                        })
                .when(ctx)
                .sendEventToSourceReader(Mockito.eq(0), Mockito.any());
        restored.addSplits(Collections.singletonList(split));
        restored.handleNoMoreSplits();
        // Old task routed this to the replacement's deterministic TaskLocation.
        restored.handleSourceEvent(new JdbcTableFinishedEvent(table, 1));
        TestCollector out = new TestCollector();
        for (int poll = 0; !ended.get() && poll < 5; poll++) {
            restored.pollNext(out);
            Assertions.assertTrue(
                    out.closeTableEvents.isEmpty(), "stale push must never retire the table");
        }
        Mockito.verify(format).open(split);
        Mockito.verify(format).nextRecord();
        Assertions.assertTrue(ended.get());
    }

    @Test
    void shouldRecoverDroppedTableFinishedDeliveryWithoutReaderReregistration() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        SourceSplitEnumerator.Context<JdbcSourceSplit> enumContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceSplitEnumerator enumerator = createFreshEnumerator(enumContext, table, 1);
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
                                            new JdbcTableCloseStatus.Reply(
                                                    ((JdbcTableCloseStatus.Request) event)
                                                            .getReaderAttempt(),
                                                    Collections.emptyMap()));
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

    @Test
    void restoredReadersMustFinishWhenEnumeratorSnapshotPrecedesTheLastSplitReport()
            throws Exception {
        verifySplitCompletionAcrossSkewedCheckpoint(false);
    }

    @Test
    void restoredReadyMarkersMustNotRetireWritersFromAnUnprovenCheckpointCut() throws Exception {
        verifySplitCompletionAcrossSkewedCheckpoint(true);
    }

    private static void verifySplitCompletionAcrossSkewedCheckpoint(boolean captureReadyMarkers)
            throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        SourceSplitEnumerator.Context<JdbcSourceSplit> liveContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        JdbcSourceReader[] liveReaders = new JdbcSourceReader[2];
        AtomicReference<JdbcSourceSplitEnumerator> live = new AtomicReference<>();
        for (int i = 0; i < 2; i++) {
            liveReaders[i] = testReader(i, live, new AtomicBoolean());
        }
        Mockito.doAnswer(
                        call -> {
                            int id = call.getArgument(0);
                            liveReaders[id].addSplits(call.getArgument(1));
                            return null;
                        })
                .when(liveContext)
                .assignSplit(Mockito.anyInt(), Mockito.anyList());
        AtomicInteger published = new AtomicInteger();
        Mockito.doAnswer(
                        call -> {
                            int id = call.getArgument(0);
                            SourceEvent event = call.getArgument(1);
                            if (event instanceof JdbcTableFinishedEvent) {
                                Assertions.assertEquals(
                                        2,
                                        ((JdbcTableFinishedEvent) event)
                                                .getExpectedCloseEventCount());
                                published.incrementAndGet();
                                if (captureReadyMarkers) liveReaders[id].handleSourceEvent(event);
                            } else {
                                liveReaders[id].handleSourceEvent(event);
                            }
                            return null;
                        })
                .when(liveContext)
                .sendEventToSourceReader(Mockito.anyInt(), Mockito.any());
        live.set(createFreshEnumerator(liveContext, table, 2));
        liveReaders[0].pollNext(new TestCollector());
        // Zeta snapshots the enumerator, then asks the readers to snapshot. The second
        // report is accepted live, but absent from the persisted enumerator state.
        JdbcSourceState beforeLastReport = live.get().snapshotState(42L);
        Assertions.assertEquals(
                1, beforeLastReport.getUnfinishedSplitsPerTableOrEmpty().get(table));
        liveReaders[1].pollNext(new TestCollector());
        Assertions.assertEquals(2, published.get());
        if (captureReadyMarkers) {
            for (JdbcSourceReader reader : liveReaders) reader.pollNext(new TestCollector());
        }
        SourceSplitEnumerator.Context<JdbcSourceSplit> afterContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        AtomicReference<JdbcSourceSplitEnumerator> after =
                new AtomicReference<>(createEnumerator(afterContext, beforeLastReport));
        JdbcSourceReader[] restored = new JdbcSourceReader[2];
        AtomicBoolean[] ended = {new AtomicBoolean(), new AtomicBoolean()};
        for (int i = 0; i < 2; i++) {
            List<JdbcSourceSplit> state = liveReaders[i].snapshotState(42L);
            Assertions.assertEquals(1, state.size());
            Assertions.assertTrue(state.get(0).isCloseTableMarker());
            Assertions.assertEquals(captureReadyMarkers, state.get(0).isPendingCloseTableEvent());
            restored[i] = testReader(i, after, ended[i]);
            restored[i].addSplits(state);
            restored[i].handleNoMoreSplits();
        }
        Mockito.doAnswer(
                        call -> {
                            int id = call.getArgument(0);
                            restored[id].handleSourceEvent(call.getArgument(1));
                            return null;
                        })
                .when(afterContext)
                .sendEventToSourceReader(Mockito.anyInt(), Mockito.any());
        for (int i = 0; i < 2; i++) {
            TestCollector out = new TestCollector();
            for (int poll = 0; !ended[i].get() && poll < 4; poll++) restored[i].pollNext(out);
            Assertions.assertTrue(
                    ended[i].get(),
                    "restored reader " + i + " must not wait for a report no reader owns");
            Assertions.assertTrue(
                    out.closeTableEvents.isEmpty(),
                    "a recovered marker is not permission to retire a writer");
        }
    }

    private static JdbcSourceReader testReader(
            int id, AtomicReference<JdbcSourceSplitEnumerator> enumerator, AtomicBoolean ended)
            throws Exception {
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        AtomicLong clock = new AtomicLong();
        return new JdbcSourceReader(
                new TestSourceReaderContext() {
                    @Override
                    public int getIndexOfSubtask() {
                        return id;
                    }

                    @Override
                    public void signalNoMoreElement() {
                        ended.set(true);
                    }

                    @Override
                    public void sendSourceEventToEnumerator(SourceEvent event) {
                        enumerator.get().handleSourceEvent(id, event);
                    }
                },
                format,
                () -> clock.getAndAdd(JdbcSourceReader.STATUS_RETRY_NANOS));
    }

    /**
     * Only the database chunk planner is mocked. Hash assignment, tracking and delivery are real.
     */
    private static JdbcSourceSplitEnumerator createFreshEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> ctx, TablePath table, int parallelism)
            throws Exception {
        JdbcSourceTable sourceTable = Mockito.mock(JdbcSourceTable.class);
        ChunkSplitter splitter = Mockito.mock(ChunkSplitter.class);
        List<JdbcSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < parallelism; i++)
            splits.add(
                    new JdbcSourceSplit(table, "split-" + i, "select 1", null, null, null, null));
        Mockito.when(splitter.generateSplits(sourceTable)).thenReturn(splits);
        Mockito.when(ctx.currentParallelism()).thenReturn(parallelism);
        Mockito.when(ctx.registeredReaders())
                .thenReturn(
                        parallelism == 1
                                ? Collections.singleton(0)
                                : new HashSet<>(Arrays.asList(0, 1)));
        JdbcSourceConfig config =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build();
        JdbcSourceSplitEnumerator enumerator;
        try (MockedStatic<ChunkSplitter> factory = Mockito.mockStatic(ChunkSplitter.class)) {
            factory.when(() -> ChunkSplitter.create(config)).thenReturn(splitter);
            enumerator =
                    new JdbcSourceSplitEnumerator(
                            ctx, config, Collections.singletonMap(table, sourceTable), null);
        }
        enumerator.run();
        return enumerator;
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
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, format);
        TablePath table = TablePath.of("db", "schema", "t");
        reader.addSplits(
                Collections.singletonList(
                        new JdbcSourceSplit(table, "split-0", "s", null, null, null, null)));
        reader.pollNext(new TestCollector());
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
        JdbcTableCloseStatus.Request proof =
                (JdbcTableCloseStatus.Request)
                        context.sourceEvents.get(context.sourceEvents.size() - 1);
        reader.handleSourceEvent(
                new JdbcTableCloseStatus.Reply(
                        proof.getReaderAttempt(), Collections.singletonMap(table, 1)));
        reader.pollNext(collector);
        Assertions.assertEquals(1, collector.closeTableEvents.size());
        reader.pollNext(collector);
        Assertions.assertTrue(ended.get());
    }

    @Test
    void testVerifiedCloseIsEmittedBeforeReadingAnUnrelatedQueuedSplit() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, format);
        TablePath first = TablePath.of("db", "schema", "one");
        TablePath second = TablePath.of("db", "schema", "two");
        JdbcSourceSplit a = new JdbcSourceSplit(first, "a", "s", null, null, null, null);
        JdbcSourceSplit b = new JdbcSourceSplit(second, "b", "s", null, null, null, null);
        reader.addSplits(Arrays.asList(a, b));
        TestCollector out = new TestCollector();
        reader.pollNext(out); // finish A with B still queued
        reader.handleSourceEvent(new JdbcTableFinishedEvent(first, 1));
        reader.pollNext(out); // request proof without consuming B
        JdbcTableCloseStatus.Request proof =
                (JdbcTableCloseStatus.Request) context.sourceEvents.get(1);
        reader.handleSourceEvent(
                new JdbcTableCloseStatus.Reply(
                        proof.getReaderAttempt(), Collections.singletonMap(first, 1)));
        reader.pollNext(out);
        Assertions.assertEquals(1, out.closeTableEvents.size());
        Assertions.assertEquals(first, out.closeTableEvents.get(0).getTablePath());
        Mockito.verify(format, Mockito.never()).open(b);
        reader.pollNext(out);
        Mockito.verify(format).open(b);
    }

    @Test
    void anAttemptMatchedResponseMustNotOvertakeQueuedOrReassignedLocalWork() throws Exception {
        TablePath table = TablePath.of("db", "schema", "t");
        TestSourceReaderContext ctx = new TestSourceReaderContext();
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        AtomicLong clock = new AtomicLong();
        JdbcSourceReader reader =
                new JdbcSourceReader(
                        ctx, format, () -> clock.getAndAdd(JdbcSourceReader.STATUS_RETRY_NANOS));
        JdbcSourceSplit first = new JdbcSourceSplit(table, "a", "s", null, null, null, null);
        JdbcSourceSplit replay = new JdbcSourceSplit(table, "b", "s", null, null, null, null);
        reader.addSplits(Collections.singletonList(first));
        TestCollector out = new TestCollector();
        reader.handleSourceEvent(new JdbcTableFinishedEvent(table, 1));
        reader.pollNext(out);
        JdbcTableCloseStatus.Request req = (JdbcTableCloseStatus.Request) ctx.sourceEvents.get(0);
        JdbcTableCloseStatus.Reply positive =
                new JdbcTableCloseStatus.Reply(
                        req.getReaderAttempt(), Collections.singletonMap(table, 1));
        reader.handleSourceEvent(positive);
        reader.pollNext(out);
        Mockito.verify(format).open(first);
        Assertions.assertTrue(out.closeTableEvents.isEmpty());
        reader.handleSourceEvent(positive); // now valid but still only queued
        reader.addSplits(Collections.singletonList(replay));
        reader.pollNext(out);
        Mockito.verify(format).open(replay);
        Assertions.assertTrue(out.closeTableEvents.isEmpty());
        reader.handleSourceEvent(positive);
        reader.pollNext(out);
        Assertions.assertEquals(1, out.closeTableEvents.size());
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> reader.addSplits(Collections.singletonList(replay)));
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
    void testRestoredReadyMarkerRequiresACurrentEnumeratorReply() throws Exception {
        TablePath tablePath = TablePath.of("db", "schema", "table");
        TestSourceReaderContext originalContext = new TestSourceReaderContext();
        JdbcInputFormat format = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(format.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(originalContext, format);
        reader.addSplits(
                Collections.singletonList(
                        new JdbcSourceSplit(tablePath, "a", "s", null, null, null, null)));
        reader.pollNext(new TestCollector());
        reader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 2));
        reader.pollNext(new TestCollector());
        JdbcTableCloseStatus.Request original =
                (JdbcTableCloseStatus.Request) originalContext.sourceEvents.get(1);
        reader.handleSourceEvent(
                new JdbcTableCloseStatus.Reply(
                        original.getReaderAttempt(), Collections.singletonMap(tablePath, 2)));

        List<JdbcSourceSplit> snapshot = reader.snapshotState(1L);

        Assertions.assertEquals(1, snapshot.size());
        Assertions.assertTrue(snapshot.get(0).isPendingCloseTableEvent());

        TestSourceReaderContext restoredContext = new TestSourceReaderContext();
        JdbcSourceReader restoredReader =
                new JdbcSourceReader(restoredContext, Mockito.mock(JdbcInputFormat.class));
        restoredReader.addSplits(snapshot);
        restoredReader.handleNoMoreSplits();
        TestCollector collector = new TestCollector();
        restoredReader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 2));
        restoredReader.handleSourceEvent(
                new JdbcTableCloseStatus.Reply(
                        java.util.UUID.randomUUID(), Collections.singletonMap(tablePath, 2)));
        restoredReader.pollNext(collector);
        Assertions.assertTrue(collector.closeTableEvents.isEmpty());
        JdbcTableCloseStatus.Request probe =
                (JdbcTableCloseStatus.Request) restoredContext.sourceEvents.get(0);
        restoredReader.handleSourceEvent(
                new JdbcTableCloseStatus.Reply(
                        probe.getReaderAttempt(), Collections.singletonMap(tablePath, 0)));
        restoredReader.pollNext(collector);
        Assertions.assertTrue(collector.closeTableEvents.isEmpty());
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
