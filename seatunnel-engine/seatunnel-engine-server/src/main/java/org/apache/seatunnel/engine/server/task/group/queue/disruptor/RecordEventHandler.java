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
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.lmax.disruptor.EventHandler;

public class RecordEventHandler implements EventHandler<RecordEvent> {

    private final SeaTunnelTask runningTask;

    private final Collector<Record<?>> collector;

    private final IntermediateQueueFlowLifeCycle intermediateQueueFlowLifeCycle;

    public RecordEventHandler(
            SeaTunnelTask runningTask,
            Collector<Record<?>> collector,
            IntermediateQueueFlowLifeCycle intermediateQueueFlowLifeCycle) {
        this.runningTask = runningTask;
        this.collector = collector;
        this.intermediateQueueFlowLifeCycle = intermediateQueueFlowLifeCycle;
    }

    @Override
    public void onEvent(RecordEvent recordEvent, long sequence, boolean endOfBatch)
            throws Exception {
        handleRecord(recordEvent.getRecord(), collector);
    }

    private void handleRecord(Record<?> record, Collector<Record<?>> collector) throws Exception {
        if (record != null) {
            if (record.getData() instanceof Barrier) {
                CheckpointBarrier barrier = (CheckpointBarrier) record.getData();
                runningTask.ack(barrier);
                if (barrier.prepareClose(this.runningTask.getTaskLocation())) {
                    this.intermediateQueueFlowLifeCycle.setPrepareClose(true);
                }
            } else {
                if (this.intermediateQueueFlowLifeCycle.getPrepareClose()) {
                    return;
                }
            }
            collector.collect(record);
        }
    }
}
