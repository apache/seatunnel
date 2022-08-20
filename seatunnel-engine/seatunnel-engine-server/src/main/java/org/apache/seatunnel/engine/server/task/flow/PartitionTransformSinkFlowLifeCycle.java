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
import org.apache.seatunnel.engine.server.task.record.ClosedSign;

import com.hazelcast.ringbuffer.Ringbuffer;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionTransformSinkFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {

    private Ringbuffer<Record<?>>[] ringbuffers;
    private AtomicInteger closeSigns;
    private final Random random = new Random();

    public PartitionTransformSinkFlowLifeCycle(CompletableFuture<Void> completableFuture) {
        super(completableFuture);
        closeSigns = new AtomicInteger();
    }

    @Override
    public void received(Record<?> row) throws IOException {
        getRingBuffer(row).add(row);
        if (row.getData() instanceof ClosedSign) {
            if (closeSigns.incrementAndGet() == ringbuffers.length) {
                this.close();
            }
        }
    }

    private Ringbuffer<Record<?>> getRingBuffer(Record<?> row) {
        return ringbuffers[random.nextInt(ringbuffers.length)];
    }
}
