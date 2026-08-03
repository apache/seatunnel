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

package org.apache.seatunnel.engine.server.task.group.queue;

import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class IntermediateBlockingQueueSignalTest {

    private static final TaskLocation TASK_LOCATION =
            new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 1);

    private BlockingQueue<Record<?>> backing;
    private ThreadSafeCounter totalSizeCounter;
    private ThreadSafeCounter sizeCounter;
    private IntermediateBlockingQueue queue;
    private IntermediateQueueFlowLifeCycle<?> flow;

    @BeforeEach
    void setUp() {
        backing = new ArrayBlockingQueue<>(4);
        totalSizeCounter = new ThreadSafeCounter("totalQueueSize");
        sizeCounter = new ThreadSafeCounter("intermediateQueueSize");
        queue =
                new IntermediateBlockingQueue(
                        backing,
                        totalSizeCounter,
                        sizeCounter,
                        new ThreadSafeCounter("putBlockedNs"),
                        new ThreadSafeCounter("flushSignalQueueSuccess"),
                        new ThreadSafeCounter("flushSignalQueueFailure"));

        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.getTaskLocation()).thenReturn(TASK_LOCATION);
        Mockito.when(task.isObservabilityEnabled()).thenReturn(true);

        flow = Mockito.mock(IntermediateQueueFlowLifeCycle.class);
        final boolean[] prepareClose = {false};
        Mockito.when(flow.getPrepareClose()).thenAnswer(inv -> prepareClose[0]);
        Mockito.doAnswer(
                        inv -> {
                            prepareClose[0] = inv.getArgument(0);
                            return null;
                        })
                .when(flow)
                .setPrepareClose(Mockito.anyBoolean());

        queue.setRunningTask(task);
        queue.setIntermediateQueueFlowLifeCycle(flow);
    }

    @Test
    void signalIsEnqueuedAndDeliveredToDownstream() throws Exception {
        queue.received(new Record<>(FlushSignal.of(1L, 42L)));

        Assertions.assertEquals(1, backing.size());
        Assertions.assertEquals(1L, totalSizeCounter.getCount());
        Assertions.assertEquals(1L, sizeCounter.getCount());

        RecordingCollector downstream = new RecordingCollector();
        queue.collect(downstream);

        Assertions.assertEquals(0, backing.size(), "queue should be drained after collect");
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
        Assertions.assertEquals(
                0L, sizeCounter.getCount(), "size counter should return to zero after drain");
        Assertions.assertEquals(1, downstream.collected.size(), "signal must reach downstream");
        Assertions.assertTrue(downstream.collected.get(0).getData() instanceof FlushSignal);
    }

    @Test
    void signalIsDroppedWhenQueueIsFullAndCounterStaysAccurate() throws Exception {
        for (int i = 0; i < 4; i++) {
            backing.add(new Record<>(new SeaTunnelRow(new Object[] {i})));
            sizeCounter.inc();
        }
        Assertions.assertEquals(4, backing.size());
        Assertions.assertEquals(4L, sizeCounter.getCount());

        queue.received(new Record<>(FlushSignal.of(1L, 42L)));

        Assertions.assertEquals(
                4, backing.size(), "signal must not be enqueued when queue is full");
        Assertions.assertEquals(
                4L, sizeCounter.getCount(), "size counter must not be incremented on drop");
    }

    @Test
    void signalIsDroppedInPrepareCloseWithoutMetricInc() throws Exception {
        flow.setPrepareClose(true);

        queue.received(new Record<>(FlushSignal.of(1L, 42L)));

        Assertions.assertTrue(backing.isEmpty(), "signal must be dropped in prepareClose");
        Assertions.assertEquals(0L, sizeCounter.getCount());
    }

    @Test
    void dataRecordFollowsBlockingPutPath() throws Exception {
        queue.received(new Record<>(new SeaTunnelRow(new Object[] {"v"})));

        Assertions.assertEquals(1, backing.size());
        Assertions.assertEquals(1L, sizeCounter.getCount());

        RecordingCollector downstream = new RecordingCollector();
        queue.collect(downstream);

        Assertions.assertEquals(1, downstream.collected.size());
        Assertions.assertEquals(0L, sizeCounter.getCount());
    }

    @Test
    void prepareCloseBarrierDrainsSubsequentDataRecords() throws Exception {
        CheckpointBarrier closeBarrier =
                new CheckpointBarrier(
                        1L,
                        System.currentTimeMillis(),
                        CheckpointType.COMPLETED_POINT_TYPE,
                        Collections.emptySet(),
                        Collections.emptySet());
        queue.received(new Record<>(closeBarrier));
        Mockito.verify(flow).setPrepareClose(true);

        queue.received(new Record<>(new SeaTunnelRow(new Object[] {"after-close"})));

        Assertions.assertEquals(1, backing.size(), "only the barrier should remain in queue");
        Assertions.assertEquals(1L, sizeCounter.getCount());
    }

    @Test
    void totalSizeDoesNotBecomeNegativeWhenDequeuePrecedesEnqueueMetricUpdate() throws Exception {
        DelayedPutQueue delayedBacking = new DelayedPutQueue(4);
        totalSizeCounter = new ThreadSafeCounter("totalQueueSize");
        sizeCounter = new ThreadSafeCounter("intermediateQueueSize");
        IntermediateBlockingQueue.QueueSizeTracker queueSizeTracker =
                new IntermediateBlockingQueue.QueueSizeTracker();
        IntermediateBlockingQueue producerQueue =
                new IntermediateBlockingQueue(
                        delayedBacking,
                        totalSizeCounter,
                        sizeCounter,
                        new ThreadSafeCounter("putBlockedNs"),
                        new ThreadSafeCounter("flushSignalQueueSuccess"),
                        new ThreadSafeCounter("flushSignalQueueFailure"),
                        queueSizeTracker);
        queue =
                new IntermediateBlockingQueue(
                        delayedBacking,
                        totalSizeCounter,
                        sizeCounter,
                        new ThreadSafeCounter("putBlockedNs"),
                        new ThreadSafeCounter("flushSignalQueueSuccess"),
                        new ThreadSafeCounter("flushSignalQueueFailure"),
                        queueSizeTracker);
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.isObservabilityEnabled()).thenReturn(false);
        producerQueue.setRunningTask(task);
        producerQueue.setIntermediateQueueFlowLifeCycle(flow);
        queue.setRunningTask(task);
        queue.setIntermediateQueueFlowLifeCycle(flow);

        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        Thread producer =
                new Thread(
                        () -> {
                            try {
                                producerQueue.received(
                                        new Record<>(new SeaTunnelRow(new Object[] {"value"})));
                            } catch (Throwable e) {
                                producerFailure.set(e);
                            }
                        });
        producer.start();

        try {
            Assertions.assertTrue(delayedBacking.awaitPublished());
            queue.collect(new RecordingCollector());

            Assertions.assertEquals(0L, totalSizeCounter.getCount());
        } finally {
            delayedBacking.releasePut();
            producer.join(TimeUnit.SECONDS.toMillis(5));
        }

        Assertions.assertFalse(producer.isAlive());
        Assertions.assertNull(producerFailure.get());
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
    }

    @Test
    void dequeuedRecordUpdatesSizeWhenPrepareCloseDropsIt() throws Exception {
        queue.received(new Record<>(new SeaTunnelRow(new Object[] {"value"})));
        flow.setPrepareClose(true);

        RecordingCollector downstream = new RecordingCollector();
        queue.collect(downstream);

        Assertions.assertTrue(downstream.collected.isEmpty());
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
        Assertions.assertEquals(0L, sizeCounter.getCount());
    }

    @Test
    void interruptedPutDoesNotChangeQueueMetrics() {
        BlockingQueue<Record<?>> interruptedBacking = new InterruptingPutQueue(1);
        queue =
                new IntermediateBlockingQueue(
                        interruptedBacking,
                        totalSizeCounter,
                        sizeCounter,
                        new ThreadSafeCounter("putBlockedNs"),
                        new ThreadSafeCounter("flushSignalQueueSuccess"),
                        new ThreadSafeCounter("flushSignalQueueFailure"));
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        Mockito.when(task.isObservabilityEnabled()).thenReturn(false);
        queue.setRunningTask(task);
        queue.setIntermediateQueueFlowLifeCycle(flow);

        RuntimeException failure =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                queue.received(
                                        new Record<>(
                                                new SeaTunnelRow(new Object[] {"interrupted"}))));

        Assertions.assertInstanceOf(InterruptedException.class, failure.getCause());
        Assertions.assertTrue(interruptedBacking.isEmpty());
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
        Assertions.assertEquals(0L, sizeCounter.getCount());
    }

    @Test
    void collectorFailureStillUpdatesQueueMetrics() throws Exception {
        queue.received(new Record<>(new SeaTunnelRow(new Object[] {"value"})));

        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        queue.collect(
                                new Collector<Record<?>>() {
                                    @Override
                                    public void collect(Record<?> record) {
                                        throw new IllegalStateException("collector failure");
                                    }

                                    @Override
                                    public void close() {}
                                }));

        Assertions.assertTrue(backing.isEmpty());
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
        Assertions.assertEquals(0L, sizeCounter.getCount());
    }

    @Test
    void independentQueuesCanShareTotalSizeCounter() throws Exception {
        BlockingQueue<Record<?>> secondBacking = new ArrayBlockingQueue<>(4);
        ThreadSafeCounter secondSizeCounter = new ThreadSafeCounter("secondQueueSize");
        IntermediateBlockingQueue secondQueue =
                new IntermediateBlockingQueue(
                        secondBacking,
                        totalSizeCounter,
                        secondSizeCounter,
                        new ThreadSafeCounter("secondPutBlockedNs"),
                        new ThreadSafeCounter("flushSignalQueueSuccess"),
                        new ThreadSafeCounter("flushSignalQueueFailure"));
        secondQueue.setRunningTask(queue.getRunningTask());
        secondQueue.setIntermediateQueueFlowLifeCycle(flow);

        queue.received(new Record<>(new SeaTunnelRow(new Object[] {"first"})));
        secondQueue.received(new Record<>(new SeaTunnelRow(new Object[] {"second"})));

        Assertions.assertEquals(2L, totalSizeCounter.getCount());
        queue.collect(new RecordingCollector());
        Assertions.assertEquals(1L, totalSizeCounter.getCount());
        secondQueue.collect(new RecordingCollector());
        Assertions.assertEquals(0L, totalSizeCounter.getCount());
        Assertions.assertEquals(0L, sizeCounter.getCount());
        Assertions.assertEquals(0L, secondSizeCounter.getCount());
    }

    private static class DelayedPutQueue extends ArrayBlockingQueue<Record<?>> {
        private final CountDownLatch published = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private DelayedPutQueue(int capacity) {
            super(capacity);
        }

        @Override
        public void put(Record<?> record) throws InterruptedException {
            super.put(record);
            published.countDown();
            release.await();
        }

        private boolean awaitPublished() throws InterruptedException {
            return published.await(5, TimeUnit.SECONDS);
        }

        private void releasePut() {
            release.countDown();
        }
    }

    private static class InterruptingPutQueue extends ArrayBlockingQueue<Record<?>> {
        private InterruptingPutQueue(int capacity) {
            super(capacity);
        }

        @Override
        public void put(Record<?> record) throws InterruptedException {
            throw new InterruptedException("test interruption");
        }
    }

    private static class RecordingCollector implements Collector<Record<?>> {
        private final List<Record<?>> collected = new ArrayList<>();

        @Override
        public void collect(Record<?> record) {
            collected.add(record);
        }

        @Override
        public void close() {}
    }
}
