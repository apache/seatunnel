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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.apache.seatunnel.api.common.metrics.MetricNames.FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL;

/**
 * Tests flush signal metrics across queue and sink stages. Verifies that counters are correctly
 * incremented on success/failure paths.
 */
public class FlushSignalMetricsFlowTest {

    private static final TaskLocation TASK_LOCATION =
            new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 1);

    @Test
    void testQueueFlushSignalSuccessMetrics() throws Exception {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        SeaTunnelTask task = mockTask();
        Mockito.when(task.getMetricsContext()).thenReturn(metricsContext);

        BlockingQueue<Record<?>> backing = new ArrayBlockingQueue<>(16);
        IntermediateBlockingQueue queue =
                new IntermediateBlockingQueue(
                        backing, new ThreadSafeCounter("queueSize"), metricsContext);

        IntermediateQueueFlowLifeCycle<?> flow =
                new IntermediateQueueFlowLifeCycle<>(task, new CompletableFuture<>(), queue);

        Record<?> flushRecord = new Record<>(FlushSignal.of(1L, 100L));
        queue.received(flushRecord);

        Counter successCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL);
        Counter failureCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL);

        Assertions.assertNotNull(successCounter);
        Assertions.assertEquals(1L, successCounter.getCount());
        Assertions.assertEquals(0L, failureCounter.getCount());
    }

    @Test
    void testQueueFlushSignalFailureWhenFull() throws Exception {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        SeaTunnelTask task = mockTask();
        Mockito.when(task.getMetricsContext()).thenReturn(metricsContext);

        BlockingQueue<Record<?>> backing = new ArrayBlockingQueue<>(1);
        IntermediateBlockingQueue queue =
                new IntermediateBlockingQueue(
                        backing, new ThreadSafeCounter("queueSize"), metricsContext);

        IntermediateQueueFlowLifeCycle<?> flow =
                new IntermediateQueueFlowLifeCycle<>(task, new CompletableFuture<>(), queue);

        // Fill the queue
        backing.offer(new Record<>(new SeaTunnelRow(new Object[] {1})));

        // This flush signal should fail to enqueue
        Record<?> flushRecord = new Record<>(FlushSignal.of(1L, 100L));
        queue.received(flushRecord);

        Counter successCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL);
        Counter failureCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL);

        Assertions.assertEquals(0L, successCounter.getCount());
        Assertions.assertEquals(1L, failureCounter.getCount());
    }

    @Test
    void testQueueFlushSignalMultipleIncrements() throws Exception {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        SeaTunnelTask task = mockTask();
        Mockito.when(task.getMetricsContext()).thenReturn(metricsContext);

        BlockingQueue<Record<?>> backing = new ArrayBlockingQueue<>(16);
        IntermediateBlockingQueue queue =
                new IntermediateBlockingQueue(
                        backing, new ThreadSafeCounter("queueSize"), metricsContext);

        IntermediateQueueFlowLifeCycle<?> flow =
                new IntermediateQueueFlowLifeCycle<>(task, new CompletableFuture<>(), queue);

        for (int i = 0; i < 5; i++) {
            queue.received(new Record<>(FlushSignal.of(1L, 100L)));
        }

        Counter successCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL);
        Assertions.assertEquals(5L, successCounter.getCount());
    }

    @Test
    void testIntermediateQueueFlowLifeCycleCountersInitialized() {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        SeaTunnelTask task = mockTask();
        Mockito.when(task.getMetricsContext()).thenReturn(metricsContext);

        BlockingQueue<Record<?>> backing = new ArrayBlockingQueue<>(16);
        IntermediateBlockingQueue queue =
                new IntermediateBlockingQueue(
                        backing, new ThreadSafeCounter("queueSize"), metricsContext);

        IntermediateQueueFlowLifeCycle<?> flow =
                new IntermediateQueueFlowLifeCycle<>(task, new CompletableFuture<>(), queue);

        Assertions.assertNotNull(flow.getFlushSignalQueueSuccessTotal());
        Assertions.assertNotNull(flow.getFlushSignalQueueFailureTotal());
        Assertions.assertEquals(0L, flow.getFlushSignalQueueSuccessTotal().getCount());
        Assertions.assertEquals(0L, flow.getFlushSignalQueueFailureTotal().getCount());
    }

    @Test
    void testFlushSignalSkippedWhenPrepareClose() throws Exception {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        SeaTunnelTask task = mockTask();
        Mockito.when(task.getMetricsContext()).thenReturn(metricsContext);

        BlockingQueue<Record<?>> backing = new ArrayBlockingQueue<>(16);
        IntermediateBlockingQueue queue =
                new IntermediateBlockingQueue(
                        backing, new ThreadSafeCounter("queueSize"), metricsContext);

        IntermediateQueueFlowLifeCycle<?> flow =
                new IntermediateQueueFlowLifeCycle<>(task, new CompletableFuture<>(), queue);

        flow.setPrepareClose(true);

        queue.received(new Record<>(FlushSignal.of(1L, 100L)));

        Counter successCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_SUCCESS_TOTAL);
        Counter failureCounter = metricsContext.counter(FLUSH_SIGNAL_QUEUE_FAILURE_TOTAL);

        // When prepareClose, signal is dropped without incrementing either counter
        Assertions.assertEquals(0L, successCounter.getCount());
        Assertions.assertEquals(0L, failureCounter.getCount());
        Assertions.assertTrue(backing.isEmpty());
    }

    private static SeaTunnelTask mockTask() {
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.getTaskLocation()).thenReturn(TASK_LOCATION);
        Mockito.when(task.getTaskID()).thenReturn(1L);
        return task;
    }
}
