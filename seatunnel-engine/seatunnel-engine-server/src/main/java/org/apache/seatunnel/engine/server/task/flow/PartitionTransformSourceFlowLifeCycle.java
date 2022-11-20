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

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PartitionTransformSourceFlowLifeCycle<T> extends AbstractFlowLifeCycle implements OneOutputFlowLifeCycle<T> {

    // TODO: init ring buffer
    private Ringbuffer<T>[] ringbuffers;

    private final Map<Integer, Barrier> alignedBarriers = new HashMap<>();

    private long currentCheckpointId = Long.MAX_VALUE;

    private int alignedBarriersCounter = 0;
    public PartitionTransformSourceFlowLifeCycle(SeaTunnelTask runningTask, CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
    }

    @Override
    public void collect(Collector<T> collector) throws Exception {
        for (int i = 0; i < ringbuffers.length; i++) {
            Ringbuffer<T> ringbuffer = ringbuffers[i];
            if (ringbuffer.size() <= 0) {
                continue;
            }
            // aligned barrier
            if (alignedBarriers.get(i) != null && alignedBarriers.get(i).getId() == currentCheckpointId) {
                continue;
            }
            // Batch reads are not used because of aligned barriers.
            // get the oldest item
            T item = ringbuffer.readOne(ringbuffer.headSequence());
            Record<?> record = (Record<?>) item;
            if (record.getData() instanceof Barrier) {
                Barrier barrier = (Barrier) record.getData();
                alignedBarriers.put(i, barrier);
                alignedBarriersCounter++;
                currentCheckpointId = barrier.getId();
                if (alignedBarriersCounter == ringbuffers.length) {
                    runningTask.ack(barrier);
                    if (barrier.prepareClose()) {
                        prepareClose = true;
                    }
                    collector.collect(item);
                    alignedBarriersCounter = 0;
                }
            } else {
                if (prepareClose) {
                    return;
                }
                collector.collect(item);
            }
        }
    }
}
