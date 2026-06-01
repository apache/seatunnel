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

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;
import org.apache.seatunnel.engine.server.trace.StainTraceUtils;

import com.lmax.disruptor.RingBuffer;

/** Publishes records into the Disruptor-backed intermediate queue while marking queue-in stages. */
public class RecordEventProducer {

    /**
     * Pre-processes barriers and stain trace metadata before handing the record to the ring buffer.
     */
    public static void onData(
            Record<?> record,
            RingBuffer<RecordEvent> ringBuffer,
            IntermediateQueueFlowLifeCycle intermediateQueueFlowLifeCycle) {

        if (record.getData() instanceof Barrier) {
            CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
            intermediateQueueFlowLifeCycle.getRunningTask().ack(barrier);
            if (barrier.prepareClose(
                    intermediateQueueFlowLifeCycle.getRunningTask().getTaskLocation())) {
                intermediateQueueFlowLifeCycle.setPrepareClose(true);
            }
        } else {
            if (intermediateQueueFlowLifeCycle.getPrepareClose()) {
                return;
            }
        }

        long sequence = ringBuffer.next();
        try {
            RecordEvent recordEvent = ringBuffer.get(sequence);
            recordEvent.setRecord(record);
            if (record.getData() instanceof SeaTunnelRow) {
                SeaTunnelRow row = (SeaTunnelRow) record.getData();
                if (StainTraceUtils.hasPayload(row)) {
                    StainTraceUtils.appendIfPresent(
                            row,
                            StainTraceStage.QUEUE_IN,
                            intermediateQueueFlowLifeCycle.getRunningTask().getTaskID(),
                            System.currentTimeMillis(),
                            intermediateQueueFlowLifeCycle.getStainTraceMaxEntriesPerTrace(),
                            intermediateQueueFlowLifeCycle.getStainTraceEntriesTruncatedTotal());
                }
            }
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
