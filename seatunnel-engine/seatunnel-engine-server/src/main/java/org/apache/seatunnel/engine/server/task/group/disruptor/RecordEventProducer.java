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

package org.apache.seatunnel.engine.server.task.group.disruptor;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.lmax.disruptor.RingBuffer;

public class RecordEventProducer {

    public final RingBuffer<RecordEvent> ringBuffer;

    public final IntermediateQueueFlowLifeCycle intermediateQueueFlowLifeCycle;

    public RecordEventProducer(RingBuffer<RecordEvent> ringBuffer, IntermediateQueueFlowLifeCycle intermediateQueueFlowLifeCycle) {
        this.ringBuffer = ringBuffer;
        this.intermediateQueueFlowLifeCycle = intermediateQueueFlowLifeCycle;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void onData(Record<?> record) {

        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            intermediateQueueFlowLifeCycle.runningTask.ack(barrier);
            if (barrier.prepareClose()) {
                intermediateQueueFlowLifeCycle.prepareClose = true;
            }
        } else {
            if (intermediateQueueFlowLifeCycle.prepareClose) {
                return;
            }
        }

        long sequence = ringBuffer.next();
        try {
            RecordEvent recordEvent = ringBuffer.get(sequence);
            recordEvent.setRecord(record);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
