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

import com.hazelcast.collection.IQueue;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("MagicNumber")
public class ShuffleSourceFlowLifeCycle<T> extends AbstractFlowLifeCycle implements OneOutputFlowLifeCycle<Record<?>> {
    private int shuffleBatchSize = 1024;
    private IQueue<Record<?>>[] shuffles;
    private List<Record<?>> unsentBuffer;
    private final Map<Integer, Barrier> alignedBarriers = new HashMap<>();
    private long currentCheckpointId = Long.MAX_VALUE;
    private int alignedBarriersCounter = 0;

    public ShuffleSourceFlowLifeCycle(SeaTunnelTask runningTask,
                                      CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        // todo initialize shuffles
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        for (int i = 0; i < shuffles.length; i++) {
            IQueue<Record<?>> shuffleQueue = shuffles[i];
            if (shuffleQueue.size() == 0) {
                continue;
            }
            // aligned barrier
            if (alignedBarriers.get(i) != null && alignedBarriers.get(i).getId() == currentCheckpointId) {
                continue;
            }

            List<Record<?>> shuffleBatch = new LinkedList<>();
            if (alignedBarriersCounter > 0) {
                shuffleBatch.add(shuffleQueue.take());
            } else if (unsentBuffer != null && !unsentBuffer.isEmpty()) {
                shuffleBatch = unsentBuffer;
                unsentBuffer = null;
            } else if (shuffleQueue.drainTo(shuffleBatch, shuffleBatchSize) == 0) {
                shuffleBatch.add(shuffleQueue.take());
            }

            for (int recordIndex = 0; recordIndex < shuffleBatch.size(); recordIndex++) {
                Record<?> record = shuffleBatch.get(recordIndex);
                if (record.getData() instanceof Barrier) {
                    Barrier barrier = (Barrier) record.getData();

                    // mark queue barrier
                    alignedBarriers.put(i, barrier);
                    alignedBarriersCounter++;
                    currentCheckpointId = barrier.getId();

                    // publish barrier
                    if (alignedBarriersCounter == shuffles.length) {
                        runningTask.ack(barrier);
                        if (barrier.prepareClose()) {
                            prepareClose = true;
                        }
                        collector.collect(record);
                        alignedBarriersCounter = 0;
                        alignedBarriers.clear();
                    }

                    if (recordIndex + 1 < shuffleBatch.size()) {
                        unsentBuffer = new LinkedList<>(shuffleBatch.subList(recordIndex + 1, shuffleBatch.size()));
                    }
                    break;
                } else {
                    if (prepareClose) {
                        return;
                    }
                    collector.collect(record);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (IQueue<Record<?>> shuffleQueue : shuffles) {
            shuffleQueue.destroy();
        }
    }
}
