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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.error.RowErrorEvent;
import org.apache.seatunnel.api.common.error.RowErrorPhase;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.context.SinkWriterContext;
import org.apache.seatunnel.engine.server.task.error.EngineMultiTableRowErrorHandler;
import org.apache.seatunnel.engine.server.task.error.EngineRowErrorCollector;
import org.apache.seatunnel.engine.server.task.error.ErrorHandler;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlerMode;
import org.apache.seatunnel.engine.server.task.error.ErrorHandlingSinkWriter;
import org.apache.seatunnel.engine.server.task.error.ErrorSinkRowWriter;
import org.apache.seatunnel.engine.server.task.error.RowErrorContext;
import org.apache.seatunnel.engine.server.task.error.StageErrorConfig;
import org.apache.seatunnel.engine.server.trace.StainTraceConstants;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ERROR_RECORDS_DROPPED;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_ERROR_RECORDS_ROUTED;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_RECORDS_IN;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;

class SinkFlowLifeCycleErrorOutcomeTest {

    private static final TaskLocation TASK_LOCATION =
            new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 2L, 0);

    @Test
    void routedErrorDoesNotReportMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        SinkWriter<SeaTunnelRow, String, String> failingWriter = new TestSinkWriter(true);
        ErrorHandlingSinkWriter<SeaTunnelRow, String, String> writer =
                new ErrorHandlingSinkWriter<>(
                        failingWriter, handler, (error, row, ctx) -> true, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, writer, handler);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_DROPPED + "#7").getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_DROPPED));
    }

    @Test
    void droppedErrorDoesNotReportMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).build());
        ErrorHandlingSinkWriter<SeaTunnelRow, String, String> writer =
                new ErrorHandlingSinkWriter<>(
                        new TestSinkWriter(true), handler, (error, row, ctx) -> true, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, writer, handler);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_ERROR_RECORDS_DROPPED + "#7").getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_ERROR_DROPPED));
    }

    @Test
    void successfulWriteStillReportsMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).build());
        ErrorHandlingSinkWriter<SeaTunnelRow, String, String> writer =
                new ErrorHandlingSinkWriter<>(
                        new TestSinkWriter(false), handler, (error, row, ctx) -> true, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, writer, handler);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_DROPPED + "#7").getCount());
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_DROPPED));
    }

    @Test
    void collectorReportedErrorDoesNotReportMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, new CollectorReportingWriter(collector), handler);
        setField(flow, "stageRowErrorCollector", collector);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
    }

    @Test
    void collectorDroppedErrorDoesNotReportMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new DroppingErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, new CollectorReportingWriter(collector), handler);
        setField(flow, "stageRowErrorCollector", collector);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_ERROR_RECORDS_DROPPED + "#7").getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_ERROR_DROPPED));
    }

    @Test
    void delayedCollectorErrorDuringFlushDoesNotReportMainSinkSuccess() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        DelayedCollectorReportingWriter delayedWriter =
                new DelayedCollectorReportingWriter(collector, true);
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, delayedWriter, handler);
        SinkWriterContext context = sinkWriterContext(metrics, collector);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(delayedWriter::timerFlush);
        setField(flow, "writerContext", context);
        setField(flow, "stageRowErrorCollector", collector);
        setField(flow, "deferTerminalWriteOutcomes", true);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_RECORDS_IN + "#7").getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));

        flow.received(new Record<>(FlushSignal.of(1L, 2L)));

        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(1L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
    }

    @Test
    void deferredSuccessfulWriteReportsMainSinkSuccessAfterFlush() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        DelayedCollectorReportingWriter delayedWriter =
                new DelayedCollectorReportingWriter(collector, false);
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, delayedWriter, handler);
        SinkWriterContext context = sinkWriterContext(metrics, collector);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(delayedWriter::timerFlush);
        setField(flow, "writerContext", context);
        setField(flow, "stageRowErrorCollector", collector);
        setField(flow, "deferTerminalWriteOutcomes", true);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));
        Assertions.assertEquals(0L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_WRITE_DONE));

        flow.received(new Record<>(FlushSignal.of(1L, 2L)));

        Assertions.assertEquals(1L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(0L, metrics.counter(SINK_ERROR_RECORDS_ROUTED + "#7").getCount());
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
        Assertions.assertFalse(hasStage(row, StainTraceStage.SINK_ERROR_ROUTED));
    }

    @Test
    void collectorReportedSuccessDoesNotRemainDeferredUntilFlush() throws Exception {
        SeaTunnelMetricsContext metrics = new SeaTunnelMetricsContext();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                createFlow(metrics, new SuccessReportingWriter(collector), handler);
        SinkWriterContext context = sinkWriterContext(metrics, collector);
        context.enableDeferredTerminalWriteOutcomes();
        setField(flow, "writerContext", context);
        setField(flow, "stageRowErrorCollector", collector);
        setField(flow, "deferTerminalWriteOutcomes", true);
        SeaTunnelRow row = tracedRow();

        flow.received(new Record<>(row));

        Assertions.assertEquals(1L, metrics.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(0, pendingTerminalWriteRowsSize(flow));
        Assertions.assertTrue(hasStage(row, StainTraceStage.SINK_WRITE_DONE));
    }

    @Test
    void multiTableHandlerConsumesPendingCollectorOutcomeBeforeSuccess() {
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        AtomicInteger outcomes = new AtomicInteger();
        EngineMultiTableRowErrorHandler multiTableHandler =
                new EngineMultiTableRowErrorHandler(
                        handler,
                        null,
                        "test",
                        (row, outcome) -> outcomes.incrementAndGet(),
                        collector);
        SeaTunnelRow row = tracedRow();

        collector.collect(new RowErrorEvent(RowErrorPhase.WRITE, null, row, new IOException()));

        Assertions.assertTrue(multiTableHandler.consumeCollectedRowErrorOutcome(row));
        Assertions.assertEquals(1, outcomes.get());
        Assertions.assertTrue(collector.drainTerminalOutcomes(true).isEmpty());
    }

    @Test
    void multiTableHandlerConsumesAlreadyRecordedCollectorOutcomeBeforeSuccess() {
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        AtomicInteger outcomes = new AtomicInteger();
        EngineMultiTableRowErrorHandler multiTableHandler =
                new EngineMultiTableRowErrorHandler(
                        handler,
                        null,
                        "test",
                        (row, outcome) -> outcomes.incrementAndGet(),
                        collector);
        SeaTunnelRow row = tracedRow();

        multiTableHandler.beginCollectedRowErrorOutcomeProbe(row);
        collector.collect(new RowErrorEvent(RowErrorPhase.WRITE, null, row, new IOException()));
        Assertions.assertEquals(1, collector.drainTerminalOutcomes(true).size());

        Assertions.assertTrue(multiTableHandler.consumeCollectedRowErrorOutcome(row));
        Assertions.assertEquals(0, outcomes.get());
        Assertions.assertFalse(multiTableHandler.consumeCollectedRowErrorOutcome(row));
    }

    @Test
    void recordedCollectorOutcomesOnlyTrackActiveProbes() throws Exception {
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        AtomicInteger outcomes = new AtomicInteger();
        EngineMultiTableRowErrorHandler multiTableHandler =
                new EngineMultiTableRowErrorHandler(
                        handler,
                        null,
                        "test",
                        (row, outcome) -> outcomes.incrementAndGet(),
                        collector);
        SeaTunnelRow probedInFlightRow = new SeaTunnelRow(new Object[] {0});

        multiTableHandler.beginCollectedRowErrorOutcomeProbe(probedInFlightRow);
        collector.collectWriteSuccess(probedInFlightRow);
        collector.drainTerminalOutcomes(true);

        for (int i = 1; i <= 10_000; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {i});
            collector.collectWriteSuccess(row);
            collector.drainTerminalOutcomes(true);
        }

        Assertions.assertEquals(1, pendingTerminalOutcomeProbesSize(collector));
        Assertions.assertTrue(multiTableHandler.consumeCollectedRowErrorOutcome(probedInFlightRow));
        Assertions.assertEquals(0, outcomes.get());
        Assertions.assertEquals(0, pendingTerminalOutcomeProbesSize(collector));

        SeaTunnelRow freshRow = new SeaTunnelRow(new Object[] {10_001});
        multiTableHandler.beginCollectedRowErrorOutcomeProbe(freshRow);
        Assertions.assertFalse(multiTableHandler.consumeCollectedRowErrorOutcome(freshRow));
    }

    @Test
    void unrelatedDrainsDoNotRecordNeverCollectedRow() throws Exception {
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        EngineMultiTableRowErrorHandler multiTableHandler =
                new EngineMultiTableRowErrorHandler(
                        handler, null, "test", (row, outcome) -> {}, collector);
        SeaTunnelRow ordinaryInFlightRow = new SeaTunnelRow(new Object[] {"ordinary"});

        multiTableHandler.beginCollectedRowErrorOutcomeProbe(ordinaryInFlightRow);
        for (int i = 0; i <= 10_000; i++) {
            collector.collectWriteSuccess(new SeaTunnelRow(new Object[] {i}));
            collector.drainTerminalOutcomes(true);
        }

        Assertions.assertEquals(1, pendingTerminalOutcomeProbesSize(collector));
        Assertions.assertFalse(
                multiTableHandler.consumeCollectedRowErrorOutcome(ordinaryInFlightRow));
        Assertions.assertEquals(0, pendingTerminalOutcomeProbesSize(collector));
    }

    @Test
    void multiTableHandlerConsumesCollectorReportedSuccess() throws Exception {
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        new NoopErrorSinkWriter());
        EngineRowErrorCollector collector = new EngineRowErrorCollector(handler, "test");
        AtomicInteger writtenOutcomes = new AtomicInteger();
        EngineMultiTableRowErrorHandler multiTableHandler =
                new EngineMultiTableRowErrorHandler(
                        handler,
                        null,
                        "test",
                        (row, outcome) -> {
                            if (outcome == ErrorHandlingSinkWriter.WriteOutcome.WRITTEN) {
                                writtenOutcomes.incrementAndGet();
                            }
                        },
                        collector);
        SeaTunnelRow row = tracedRow();

        collector.collectWriteSuccess(row);

        Assertions.assertTrue(multiTableHandler.consumeCollectedRowErrorOutcome(row));
        Assertions.assertEquals(1, writtenOutcomes.get());
    }

    @SuppressWarnings("unchecked")
    private static SinkFlowLifeCycle<SeaTunnelRow, String, String, String> createFlow(
            SeaTunnelMetricsContext metrics,
            SinkWriter<SeaTunnelRow, String, String> writer,
            ErrorHandler<SeaTunnelRow> handler)
            throws Exception {
        SeaTunnelSink<SeaTunnelRow, String, String, String> sink =
                Mockito.mock(SeaTunnelSink.class);
        Mockito.when(sink.getWriteCatalogTable()).thenReturn(Optional.empty());
        SinkAction<SeaTunnelRow, String, String, String> action =
                new SinkAction<>(7L, "sink", sink, Collections.emptySet(), Collections.emptySet());
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.getTaskID()).thenReturn(2L);
        Mockito.when(task.isObservabilityEnabled()).thenReturn(true);
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> flow =
                new SinkFlowLifeCycle<>(
                        action,
                        TASK_LOCATION,
                        0,
                        task,
                        null,
                        false,
                        new CompletableFuture<>(),
                        metrics);
        setField(flow, "writer", writer);
        setField(flow, "stageErrorHandler", handler);
        setField(flow, "stainTraceMaxEntriesPerTrace", 32);
        setField(flow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("truncated"));
        return flow;
    }

    private static SeaTunnelRow tracedRow() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.getOptions()
                .put(
                        StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY,
                        StainTracePayload.init(11L, 22L));
        return row;
    }

    private static boolean hasStage(SeaTunnelRow row, StainTraceStage stage) {
        byte[] payload =
                (byte[]) row.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY);
        return StainTracePayload.readEntries(payload).stream()
                .anyMatch(entry -> entry.stageCode == (stage.getCode() & 0xFF));
    }

    private static SinkWriterContext sinkWriterContext(
            SeaTunnelMetricsContext metrics, EngineRowErrorCollector collector) {
        return new SinkWriterContext(1, 0, metrics, event -> {}, collector);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static int pendingTerminalWriteRowsSize(Object target) throws Exception {
        Field field = target.getClass().getDeclaredField("pendingTerminalWriteRows");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(target)).size();
    }

    private static int pendingTerminalOutcomeProbesSize(EngineRowErrorCollector collector)
            throws Exception {
        Field field =
                EngineRowErrorCollector.class.getDeclaredField("pendingTerminalOutcomeProbes");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(collector)).size();
    }

    private static final class TestSinkWriter implements SinkWriter<SeaTunnelRow, String, String> {
        private final boolean fail;

        private TestSinkWriter(boolean fail) {
            this.fail = fail;
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            if (fail) {
                throw new IOException("row error");
            }
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static final class CollectorReportingWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final EngineRowErrorCollector collector;

        private CollectorReportingWriter(EngineRowErrorCollector collector) {
            this.collector = collector;
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            try {
                collector.collect(
                        new RowErrorEvent(
                                RowErrorPhase.WRITE, null, element, new IOException("row error")));
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static final class SuccessReportingWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final EngineRowErrorCollector collector;

        private SuccessReportingWriter(EngineRowErrorCollector collector) {
            this.collector = collector;
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            try {
                collector.collectWriteSuccess(element);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static final class DelayedCollectorReportingWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final EngineRowErrorCollector collector;
        private final boolean failOnFlush;
        private final List<SeaTunnelRow> pendingRows = new ArrayList<>();

        private DelayedCollectorReportingWriter(
                EngineRowErrorCollector collector, boolean failOnFlush) {
            this.collector = collector;
            this.failOnFlush = failOnFlush;
        }

        @Override
        public void write(SeaTunnelRow element) {
            pendingRows.add(element);
        }

        private void timerFlush() throws Exception {
            if (!failOnFlush) {
                pendingRows.clear();
                return;
            }
            for (SeaTunnelRow row : new ArrayList<>(pendingRows)) {
                collector.collect(
                        new RowErrorEvent(
                                RowErrorPhase.FLUSH, null, row, new IOException("row error")));
            }
            pendingRows.clear();
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static final class NoopErrorSinkWriter implements ErrorSinkRowWriter<SeaTunnelRow> {
        @Override
        public void write(RowErrorContext context, SeaTunnelRow row, Throwable error) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static final class DroppingErrorSinkWriter implements ErrorSinkRowWriter<SeaTunnelRow> {
        @Override
        public void write(RowErrorContext context, SeaTunnelRow row, Throwable error) {}

        @Override
        public boolean writeAndCheckAccepted(
                RowErrorContext context, SeaTunnelRow row, Throwable error) {
            return false;
        }

        @Override
        public void close() {}
    }
}
