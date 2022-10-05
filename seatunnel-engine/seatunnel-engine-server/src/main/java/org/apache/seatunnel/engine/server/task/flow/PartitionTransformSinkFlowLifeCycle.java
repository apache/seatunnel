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
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.ringbuffer.Ringbuffer;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class PartitionTransformSinkFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {

    // TODO: init ring buffer
    private Ringbuffer<Record<?>>[] ringbuffers;
    private final Random random = new Random();

    public PartitionTransformSinkFlowLifeCycle(SeaTunnelTask runningTask, CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
    }

    @Override
    public void received(Record<?> row) throws IOException {
        // TODO: No space in the buffer
        if (row.getData() instanceof Barrier) {
            Barrier barrier = (Barrier) row.getData();
            runningTask.ack(barrier);
            if (barrier.prepareClose()) {
                prepareClose = true;
            }
            // The barrier needs to be replicated to all channels
            for (Ringbuffer<Record<?>> ringBuffer : ringbuffers) {
                ringBuffer.add(new Record<>(barrier));
            }
        } else {
            if (prepareClose) {
                return;
            }
            getRingBuffer(row).add(row);
        }
    }

    private Ringbuffer<Record<?>> getRingBuffer(Record<?> row) {
        // TODO: choose partition
        return ringbuffers[random.nextInt(ringbuffers.length)];
    }
}
