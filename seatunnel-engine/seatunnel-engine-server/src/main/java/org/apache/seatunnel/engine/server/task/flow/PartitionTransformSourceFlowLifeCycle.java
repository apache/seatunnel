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

import org.apache.seatunnel.api.transform.Collector;

import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.concurrent.CompletableFuture;

public class PartitionTransformSourceFlowLifeCycle<T> extends AbstractFlowLifeCycle implements OneOutputFlowLifeCycle<T> {

    private Ringbuffer<T>[] ringbuffers;
    // TODO checkpoint offset
    private long[] offset;

    public PartitionTransformSourceFlowLifeCycle(CompletableFuture<Void> completableFuture) {
        super(completableFuture);
    }

    @Override
    public void collect(Collector<T> collector) throws Exception {
        for (int i = 0; i < ringbuffers.length; i++) {
            Ringbuffer<T> ringbuffer = ringbuffers[i];
            long tail = ringbuffer.tailSequence();
            if (tail < 0) {
                continue;
            }
            // TODO Optimize the size of batch reads
            int size = ringbuffer.readManyAsync(offset[i], 0, (int) (tail - offset[i]), null)
                    .thenApply(rs -> {
                        for (T record : rs) {
                            // TODO checkpoint check
                            collector.collect(record);
                        }
                        return rs.size();
                    }).toCompletableFuture().join();
            offset[i] += size;
        }
        // TODO received ClosedSign to close this FlowLifeCycle.
    }
}
