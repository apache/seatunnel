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

package org.apache.seatunnel.engine.server.trace;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeQPSMeter;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelSourceCollector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Covers end-to-end stain trace behavior across source, queue, transform, and sink stages. */
public class StainTraceFlowTest {

    @Test
    void testEndToEndStagesAndSingleEventReport() throws Exception {
        long nowMs = 1_700_000_001_000L;
        TestMetricsContext metricsContext = new TestMetricsContext();
        List<Event> reported = new ArrayList<>();
        TaskExecutionContext taskExecutionContext = mockTaskExecutionContext(reported);

        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(metricsContext, taskExecutionContext);
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());
        setField(sinkFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(sinkFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        TransformFlowLifeCycle<SeaTunnelRow> transformFlow = createTransformFlow(sinkFlow);
        setField(transformFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(transformFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        IntermediateQueueFlowLifeCycle<?> queueFlow = createQueueFlow();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setStainTraceEnabled(true);
        engineConfig.setStainTraceSampleRate(1);
        engineConfig.setStainTraceMaxTracesPerSecondPerWorker(1000);
        engineConfig.setStainTraceMaxEntriesPerTrace(32);

        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelSourceCollector<SeaTunnelRow> sourceCollector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(queueFlow),
                        metricsContext,
                        org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy.builder()
                                .build(),
                        rowType,
                        Collections.singletonList(TablePath.DEFAULT),
                        sourceTask,
                        engineConfig,
                        () -> nowMs);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(TablePath.DEFAULT.getFullName());
        sourceCollector.collect(row);
        queueFlow.collect(new ForwardCollector(transformFlow));

        Assertions.assertEquals(1, reported.size());
        Assertions.assertTrue(reported.get(0) instanceof StainTraceEvent);
        StainTraceEvent event = (StainTraceEvent) reported.get(0);

        Assertions.assertEquals(
                event.getTraceId(), StainTracePayload.readTraceId(event.getPayload()));
        Assertions.assertEquals(TablePath.DEFAULT.getFullName(), event.getTableId());

        byte[] payload = event.getPayload();
        Assertions.assertTrue(StainTracePayload.isValid(payload));
        Assertions.assertEquals(
                Arrays.asList(
                        StainTraceStage.SOURCE_EMIT.getCode(),
                        StainTraceStage.QUEUE_IN.getCode(),
                        StainTraceStage.QUEUE_OUT.getCode(),
                        StainTraceStage.TRANSFORM_IN.getCode(),
                        StainTraceStage.TRANSFORM_OUT.getCode(),
                        StainTraceStage.SINK_WRITE_DONE.getCode()),
                readStageCodes(payload));
    }

    @Test
    void testFilteredRowDoesNotReportEvent() throws Exception {
        long nowMs = 1_700_000_002_000L;
        TestMetricsContext metricsContext = new TestMetricsContext();
        List<Event> reported = new ArrayList<>();
        TaskExecutionContext taskExecutionContext = mockTaskExecutionContext(reported);

        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(metricsContext, taskExecutionContext);
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());
        setField(sinkFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(sinkFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        SeaTunnelMapTransform<SeaTunnelRow> filter = Mockito.mock(SeaTunnelMapTransform.class);
        Mockito.when(filter.map(Mockito.any())).thenReturn(null);
        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "t",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.singletonList(filter));
        TransformFlowLifeCycle<SeaTunnelRow> transformFlow =
                new TransformFlowLifeCycle<>(
                        action,
                        mockTask(300L, null),
                        new ForwardCollector(sinkFlow),
                        new CompletableFuture<>());
        setField(transformFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(transformFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        IntermediateQueueFlowLifeCycle<?> queueFlow = createQueueFlow();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setStainTraceEnabled(true);
        engineConfig.setStainTraceSampleRate(1);
        engineConfig.setStainTraceMaxTracesPerSecondPerWorker(1000);
        engineConfig.setStainTraceMaxEntriesPerTrace(32);

        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelSourceCollector<SeaTunnelRow> sourceCollector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(queueFlow),
                        metricsContext,
                        org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy.builder()
                                .build(),
                        rowType,
                        Collections.singletonList(TablePath.DEFAULT),
                        sourceTask,
                        engineConfig,
                        () -> nowMs);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(TablePath.DEFAULT.getFullName());
        sourceCollector.collect(row);
        queueFlow.collect(new ForwardCollector(transformFlow));

        Assertions.assertTrue(reported.isEmpty());
        byte[] payload = StainTraceUtils.getPayloadOrNull(row);
        Assertions.assertNotNull(payload);
        Assertions.assertEquals(
                Arrays.asList(
                        StainTraceStage.SOURCE_EMIT.getCode(),
                        StainTraceStage.QUEUE_IN.getCode(),
                        StainTraceStage.QUEUE_OUT.getCode(),
                        StainTraceStage.TRANSFORM_IN.getCode()),
                readStageCodes(payload));
    }

    @Test
    void testSamplingRateEffectiveInEndToEndFlow() throws Exception {
        long nowMs = 1_700_000_003_000L;
        TestMetricsContext metricsContext = new TestMetricsContext();
        List<Event> reported = new ArrayList<>();
        TaskExecutionContext taskExecutionContext = mockTaskExecutionContext(reported);

        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(metricsContext, taskExecutionContext);
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());
        setField(sinkFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(sinkFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        TransformFlowLifeCycle<SeaTunnelRow> transformFlow = createTransformFlow(sinkFlow);
        setField(transformFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(transformFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        IntermediateQueueFlowLifeCycle<?> queueFlow = createQueueFlow();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setStainTraceEnabled(true);
        engineConfig.setStainTraceSampleRate(2);
        engineConfig.setStainTraceMaxTracesPerSecondPerWorker(1000);
        engineConfig.setStainTraceMaxEntriesPerTrace(32);

        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelSourceCollector<SeaTunnelRow> sourceCollector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(queueFlow),
                        metricsContext,
                        org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy.builder()
                                .build(),
                        rowType,
                        Collections.singletonList(TablePath.DEFAULT),
                        sourceTask,
                        engineConfig,
                        () -> nowMs);

        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {1});
        row1.setTableId(TablePath.DEFAULT.getFullName());
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {2});
        row2.setTableId(TablePath.DEFAULT.getFullName());
        sourceCollector.collect(row1);
        sourceCollector.collect(row2);
        queueFlow.collect(new ForwardCollector(transformFlow));

        Assertions.assertEquals(1, reported.size());
        Assertions.assertTrue(reported.get(0) instanceof StainTraceEvent);
        assertStageOrder((StainTraceEvent) reported.get(0));
    }

    @Test
    void testWorkerBudgetEffectiveInEndToEndFlow() throws Exception {
        long nowMs = 1_700_000_004_000L;
        TestMetricsContext metricsContext = new TestMetricsContext();
        List<Event> reported = new ArrayList<>();
        TaskExecutionContext taskExecutionContext = mockTaskExecutionContext(reported);

        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(metricsContext, taskExecutionContext);
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());
        setField(sinkFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(sinkFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        TransformFlowLifeCycle<SeaTunnelRow> transformFlow = createTransformFlow(sinkFlow);
        setField(transformFlow, "stainTraceMaxEntriesPerTrace", 32);
        setField(transformFlow, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));

        IntermediateQueueFlowLifeCycle<?> queueFlow = createQueueFlow();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setStainTraceEnabled(true);
        engineConfig.setStainTraceSampleRate(1);
        engineConfig.setStainTraceMaxTracesPerSecondPerWorker(1);
        engineConfig.setStainTraceMaxEntriesPerTrace(32);

        SeaTunnelTask sourceTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(sourceTask.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        SeaTunnelSourceCollector<SeaTunnelRow> sourceCollector =
                new SeaTunnelSourceCollector<>(
                        new Object(),
                        Collections.singletonList(queueFlow),
                        metricsContext,
                        org.apache.seatunnel.core.starter.flowcontrol.FlowControlStrategy.builder()
                                .build(),
                        rowType,
                        Collections.singletonList(TablePath.DEFAULT),
                        sourceTask,
                        engineConfig,
                        () -> nowMs);

        for (int i = 0; i < 10; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {i});
            row.setTableId(TablePath.DEFAULT.getFullName());
            sourceCollector.collect(row);
        }
        queueFlow.collect(new ForwardCollector(transformFlow));

        Assertions.assertEquals(1, reported.size());
        Assertions.assertTrue(reported.get(0) instanceof StainTraceEvent);
        assertStageOrder((StainTraceEvent) reported.get(0));
    }

    private static void assertStageOrder(StainTraceEvent event) {
        Assertions.assertEquals(
                Arrays.asList(
                        StainTraceStage.SOURCE_EMIT.getCode(),
                        StainTraceStage.QUEUE_IN.getCode(),
                        StainTraceStage.QUEUE_OUT.getCode(),
                        StainTraceStage.TRANSFORM_IN.getCode(),
                        StainTraceStage.TRANSFORM_OUT.getCode(),
                        StainTraceStage.SINK_WRITE_DONE.getCode()),
                readStageCodes(event.getPayload()));
    }

    private static SinkFlowLifeCycle<SeaTunnelRow, String, String, String> createSinkFlow(
            MetricsContext metricsContext, TaskExecutionContext executionContext) {
        SeaTunnelSink<SeaTunnelRow, String, String, String> sink =
                Mockito.mock(SeaTunnelSink.class);
        SinkWriter<SeaTunnelRow, String, String> writer = Mockito.mock(SinkWriter.class);
        try {
            Mockito.when(sink.createWriter(Mockito.any())).thenReturn(writer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Mockito.when(sink.getWriteCatalogTable()).thenReturn(Optional.empty());
        Mockito.when(sink.getCommitInfoSerializer()).thenReturn(Optional.empty());
        Mockito.when(sink.getWriterStateSerializer()).thenReturn(Optional.empty());
        try {
            Mockito.when(sink.createCommitter()).thenReturn(Optional.empty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SinkAction<SeaTunnelRow, String, String, String> sinkAction =
                new SinkAction<>(1L, "s", sink, Collections.emptySet(), Collections.emptySet());

        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 2L), 1L, 0);
        SeaTunnelTask runningTask = mockTask(400L, executionContext);
        return new SinkFlowLifeCycle<>(
                sinkAction,
                taskLocation,
                0,
                runningTask,
                new TaskLocation(new TaskGroupLocation(1L, 1, 2L), 2L, 0),
                false,
                new CompletableFuture<>(),
                metricsContext);
    }

    private static TransformFlowLifeCycle<SeaTunnelRow> createTransformFlow(
            SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow) {
        SeaTunnelMapTransform<SeaTunnelRow> mapTransform =
                Mockito.mock(SeaTunnelMapTransform.class);
        Mockito.when(mapTransform.map(Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "t",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.singletonList(mapTransform));

        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(runningTask.getTaskID()).thenReturn(300L);

        return new TransformFlowLifeCycle<>(
                action, runningTask, new ForwardCollector(sinkFlow), new CompletableFuture<>());
    }

    private static IntermediateQueueFlowLifeCycle<?> createQueueFlow() throws Exception {
        SeaTunnelTask queueTask = mockTask(200L, null);
        BlockingQueue<Record<?>> queue = new LinkedBlockingQueue<>();
        IntermediateBlockingQueue blockingQueue =
                new IntermediateBlockingQueue(
                        queue,
                        new ThreadSafeCounter("total-qsize"),
                        new ThreadSafeCounter("qsize"),
                        new ThreadSafeCounter("put-blocked-ns"));
        setField(blockingQueue, "stainTraceMaxEntriesPerTrace", 32);
        setField(blockingQueue, "stainTraceEntriesTruncatedTotal", new ThreadSafeCounter("c"));
        return new IntermediateQueueFlowLifeCycle<>(
                queueTask, new CompletableFuture<>(), blockingQueue);
    }

    private static SeaTunnelTask mockTask(long taskId, TaskExecutionContext executionContext) {
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.getTaskID()).thenReturn(taskId);
        if (executionContext != null) {
            Mockito.when(task.getExecutionContext()).thenReturn(executionContext);
        }
        return task;
    }

    private static TaskExecutionContext mockTaskExecutionContext(List<Event> events) {
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.doAnswer(
                        invocation -> {
                            events.add(invocation.getArgument(0));
                            return null;
                        })
                .when(taskExecutionService)
                .reportEvent(Mockito.any());
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(taskExecutionContext.getTaskExecutionService())
                .thenReturn(taskExecutionService);
        return taskExecutionContext;
    }

    private static List<Byte> readStageCodes(byte[] payload) {
        Assertions.assertTrue(StainTracePayload.isValid(payload));
        int countOffset = 4 + 2 + 8 + 8;
        int entryCount = ((payload[countOffset] & 0xFF) << 8) | (payload[countOffset + 1] & 0xFF);
        List<Byte> codes = new ArrayList<>(entryCount);
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int base = StainTracePayload.HEADER_LENGTH;
        for (int i = 0; i < entryCount; i++) {
            codes.add(buffer.get(base + i * StainTracePayload.ENTRY_LENGTH));
        }
        return codes;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    /** Collector that keeps forwarded records so the tests can assert the transformed outputs. */
    private static class ForwardCollector implements Collector<Record<?>> {
        private final org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle<Record<?>>
                next;

        private ForwardCollector(
                org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle<Record<?>>
                        next) {
            this.next = next;
        }

        @Override
        public void collect(Record<?> record) {
            try {
                next.received(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {}
    }

    /** Minimal metrics context that records counter values used by the stain trace flow tests. */
    private static class TestMetricsContext implements MetricsContext {
        @Override
        public Counter counter(String name) {
            return new ThreadSafeCounter(name);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return new ThreadSafeQPSMeter(name);
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
