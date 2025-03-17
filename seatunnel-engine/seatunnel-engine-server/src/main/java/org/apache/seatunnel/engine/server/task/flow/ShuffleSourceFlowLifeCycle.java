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
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
@SuppressWarnings("MagicNumber")
public class ShuffleSourceFlowLifeCycle<T> extends AbstractFlowLifeCycle
        implements OneOutputFlowLifeCycle<Record<?>> {
    private final ShuffleAction shuffleAction;
    private final int shuffleBatchSize;
    private final IQueue<Record<?>>[] shuffles;
    private Map<Integer, List<Record<?>>> unsentBufferMap = new HashMap<>();
    private final Map<Integer, Barrier> alignedBarriers = new HashMap<>();
    private long currentCheckpointId = Long.MAX_VALUE;
    private int alignedBarriersCounter = 0;

    public ShuffleSourceFlowLifeCycle(
            SeaTunnelTask runningTask,
            int taskIndex,
            ShuffleAction shuffleAction,
            HazelcastInstance hazelcastInstance,
            CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        int pipelineId = runningTask.getTaskLocation().getPipelineId();
        this.shuffleAction = shuffleAction;
        this.shuffles =
                shuffleAction
                        .getConfig()
                        .getShuffleStrategy()
                        .getShuffles(hazelcastInstance, pipelineId, taskIndex);
        this.shuffleBatchSize = shuffleAction.getConfig().getBatchSize();
    }

    @Override
    public void collect(Collector<Record<?>> collector) throws Exception {
        int emptyShuffleQueueCount = 0;

        for (int i = 0; i < shuffles.length; i++) {
            IQueue<Record<?>> shuffleQueue = shuffles[i];
            List<Record<?>> unsentBuffer =
                    unsentBufferMap.computeIfAbsent(i, k -> new LinkedList<>());
            if (shuffleQueue.size() == 0) {
                emptyShuffleQueueCount++;
                continue;
            }
            // aligned barrier
            if (alignedBarriers.get(i) != null
                    && alignedBarriers.get(i).getId() == currentCheckpointId) {
                continue;
            }

            List<Record<?>> shuffleBatch = new LinkedList<>();
            if (alignedBarriersCounter > 0) {
                shuffleBatch.add(shuffleQueue.take());
            } else if (!unsentBuffer.isEmpty()) {
                shuffleBatch.addAll(unsentBuffer);
                unsentBuffer.clear();
            }

            shuffleQueue.drainTo(shuffleBatch, shuffleBatchSize);

            for (int recordIndex = 0; recordIndex < shuffleBatch.size(); recordIndex++) {
                Record<?> record = shuffleBatch.get(recordIndex);
                if (record.getData() instanceof Barrier) {
                    long startTime = System.currentTimeMillis();

                    Barrier barrier = (Barrier) record.getData();

                    // mark queue barrier
                    alignedBarriers.put(i, barrier);
                    alignedBarriersCounter++;
                    currentCheckpointId = barrier.getId();

                    // publish barrier
                    if (alignedBarriersCounter == shuffles.length) {
                        if (barrier.prepareClose(runningTask.getTaskLocation())) {
                            prepareClose = true;
                        }
                        if (barrier.snapshot()) {
                            runningTask.addState(
                                    barrier,
                                    ActionStateKey.of(shuffleAction),
                                    Collections.emptyList());
                        }
                        runningTask.ack(barrier);

                        collector.collect(record);
                        log.debug(
                                "trigger barrier [{}] finished, cost: {}ms. taskLocation: [{}]",
                                barrier.getId(),
                                System.currentTimeMillis() - startTime,
                                runningTask.getTaskLocation());

                        alignedBarriersCounter = 0;
                        alignedBarriers.clear();
                    }

                    if (recordIndex + 1 < shuffleBatch.size()) {
                        unsentBuffer.addAll(
                                shuffleBatch.subList(recordIndex + 1, shuffleBatch.size()));
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

        if (emptyShuffleQueueCount == shuffles.length) {
            Thread.sleep(100);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (IQueue<Record<?>> shuffleQueue : shuffles) {
            log.info("destroy shuffle queue: {}", shuffleQueue.getName());
            shuffleQueue.destroy();
        }
    }
}
