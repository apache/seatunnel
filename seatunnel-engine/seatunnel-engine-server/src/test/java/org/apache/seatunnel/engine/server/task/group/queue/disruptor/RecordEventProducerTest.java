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

package org.apache.seatunnel.engine.server.task.group.queue.disruptor;

import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.signal.FlushSignal;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

class RecordEventProducerTest {

    private static final TaskLocation TASK_LOCATION =
            new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 1);

    @SuppressWarnings("rawtypes")
    private static IntermediateQueueFlowLifeCycle createFlow(SeaTunnelTask task) {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        Mockito.doReturn(TASK_LOCATION).when(task).getTaskLocation();
        Mockito.doReturn(metricsContext).when(task).getMetricsContext();
        Mockito.doReturn(1L).when(task).getTaskID();

        IntermediateBlockingQueue blockingQueue =
                new IntermediateBlockingQueue(
                        new LinkedBlockingQueue<>(),
                        new ThreadSafeCounter("qsize"),
                        metricsContext);

        return new IntermediateQueueFlowLifeCycle(task, new CompletableFuture<>(), blockingQueue);
    }

    private static RingBuffer<RecordEvent> createRingBuffer() {
        return RingBuffer.create(
                ProducerType.SINGLE, RecordEvent::new, 4, new YieldingWaitStrategy());
    }

    @Test
    void signalIsPublishedWhenCapacityAvailable() {
        RingBuffer<RecordEvent> ringBuffer = createRingBuffer();
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        IntermediateQueueFlowLifeCycle flow = createFlow(task);

        long seqBefore = ringBuffer.getCursor();

        RecordEventProducer.onData(new Record<>(FlushSignal.of(1L, 42L)), ringBuffer, flow);

        long seqAfter = ringBuffer.getCursor();
        Assertions.assertEquals(seqBefore + 1, seqAfter, "signal should advance cursor by 1");
        RecordEvent event = ringBuffer.get(seqAfter);
        Assertions.assertInstanceOf(
                FlushSignal.class,
                event.getRecord().getData(),
                "published event must carry FlushSignal");
    }

    @Test
    void signalIsDroppedSilentlyWhenRingBufferIsFull() {
        RingBuffer<RecordEvent> ringBuffer = createRingBuffer();
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        IntermediateQueueFlowLifeCycle flow = createFlow(task);

        Sequence gatingSeq = new Sequence(-1L);
        ringBuffer.addGatingSequences(gatingSeq);
        for (int i = 0; i < 4; i++) {
            ringBuffer.publishEvent(
                    (e, s) -> e.setRecord(new Record<>(new SeaTunnelRow(new Object[] {"x"}))));
        }
        Assertions.assertEquals(0L, ringBuffer.remainingCapacity());
        long cursorBefore = ringBuffer.getCursor();

        Assertions.assertDoesNotThrow(
                () ->
                        RecordEventProducer.onData(
                                new Record<>(FlushSignal.of(1L, 42L)), ringBuffer, flow));

        Assertions.assertEquals(
                cursorBefore,
                ringBuffer.getCursor(),
                "cursor must not advance when signal was dropped");
    }

    @Test
    void dataRecordIsDroppedInPrepareCloseAndNotPublished() {
        RingBuffer<RecordEvent> ringBuffer = createRingBuffer();
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        IntermediateQueueFlowLifeCycle flow = createFlow(task);

        flow.setPrepareClose(true);
        long cursorBefore = ringBuffer.getCursor();

        RecordEventProducer.onData(
                new Record<>(new SeaTunnelRow(new Object[] {"v"})), ringBuffer, flow);

        Assertions.assertEquals(
                cursorBefore,
                ringBuffer.getCursor(),
                "data record must be dropped during prepareClose");
    }

    @Test
    void barrierIsAlwaysPublishedAndFlipsPrepareCloseForFinalCheckpoint() {
        RingBuffer<RecordEvent> ringBuffer = createRingBuffer();
        SeaTunnelTask task = Mockito.mock(SeaTunnelTask.class);
        IntermediateQueueFlowLifeCycle flow = createFlow(task);

        CheckpointBarrier closeBarrier =
                new CheckpointBarrier(
                        1L,
                        System.currentTimeMillis(),
                        CheckpointType.COMPLETED_POINT_TYPE,
                        Collections.emptySet(),
                        Collections.emptySet());

        RecordEventProducer.onData(new Record<>(closeBarrier), ringBuffer, flow);

        Assertions.assertTrue(flow.getPrepareClose(), "prepareClose should be true after barrier");
        Mockito.verify(task).ack(closeBarrier);
    }
}
