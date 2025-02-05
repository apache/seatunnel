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

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleStrategy;
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
import java.util.Map;
import java.util.Queue;

@SuppressWarnings("MagicNumber")
@Slf4j
public class ShuffleSinkFlowLifeCycle extends AbstractFlowLifeCycle
        implements OneInputFlowLifeCycle<Record<?>> {
    private final int pipelineId;
    private final int taskIndex;
    private final ShuffleAction shuffleAction;
    private final Map<String, IQueue<Record<?>>> shuffles;
    private final int shuffleBatchSize;
    private final long shuffleBatchFlushInterval;
    private final Map<String, Queue<Record<?>>> shuffleBuffer;
    private final ShuffleStrategy shuffleStrategy;
    private int shuffleBufferSize;
    private long lastModify;

    public ShuffleSinkFlowLifeCycle(
            SeaTunnelTask runningTask,
            int taskIndex,
            ShuffleAction shuffleAction,
            HazelcastInstance hazelcastInstance,
            CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        this.pipelineId = runningTask.getTaskLocation().getTaskGroupLocation().getPipelineId();
        this.taskIndex = taskIndex;
        this.shuffleAction = shuffleAction;
        this.shuffleStrategy = shuffleAction.getConfig().getShuffleStrategy();
        this.shuffles = shuffleStrategy.createShuffles(hazelcastInstance, pipelineId, taskIndex);
        this.shuffleBatchSize = shuffleAction.getConfig().getBatchSize();
        this.shuffleBatchFlushInterval = shuffleAction.getConfig().getBatchFlushInterval();
        this.shuffleBuffer = new HashMap<>();
    }

    @Override
    public void received(Record<?> record) throws IOException {
        if (record.getData() instanceof Barrier) {
            long startTime = System.currentTimeMillis();

            // flush shuffle buffer
            shuffleFlush();

            Barrier barrier = (Barrier) record.getData();
            if (barrier.prepareClose(runningTask.getTaskLocation())) {
                prepareClose = true;
            }
            if (barrier.snapshot()) {
                runningTask.addState(
                        barrier, ActionStateKey.of(shuffleAction), Collections.emptyList());
            }
            runningTask.ack(barrier);

            // The barrier needs to be replicated to all channels
            for (Map.Entry<String, IQueue<Record<?>>> shuffle : shuffles.entrySet()) {
                IQueue<Record<?>> shuffleQueue = shuffle.getValue();
                try {
                    shuffleQueue.put(record);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            log.debug(
                    "trigger barrier [{}] finished, cost: {}ms. taskLocation: [{}]",
                    barrier.getId(),
                    System.currentTimeMillis() - startTime,
                    runningTask.getTaskLocation());
        } else if (record.getData() instanceof SchemaChangeEvent) {
            if (prepareClose) {
                return;
            }

            shuffleItem(record);
        } else {
            if (prepareClose) {
                return;
            }

            shuffleItem(record);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (Map.Entry<String, IQueue<Record<?>>> shuffleItem : shuffles.entrySet()) {
            log.info("destroy shuffle queue: {}", shuffleItem.getKey());
            shuffleItem.getValue().destroy();
        }
    }

    private synchronized void shuffleItem(Record<?> record) {
        String shuffleKey = shuffleStrategy.createShuffleKey(record, pipelineId, taskIndex);
        shuffleBuffer.computeIfAbsent(shuffleKey, key -> new LinkedList<>()).add(record);
        shuffleBufferSize++;

        if (shuffleBufferSize >= shuffleBatchSize
                || (shuffleBufferSize > 1
                        && System.currentTimeMillis() - lastModify > shuffleBatchFlushInterval)) {
            shuffleFlush();
        }
    }

    private synchronized void shuffleFlush() {
        for (Map.Entry<String, Queue<Record<?>>> shuffleBatch : shuffleBuffer.entrySet()) {
            IQueue<Record<?>> shuffleQueue = shuffles.get(shuffleBatch.getKey());
            Queue<Record<?>> shuffleQueueBatch = shuffleBatch.getValue();
            if (shuffleQueue.remainingCapacity() <= 0
                    || !shuffleQueue.addAll(shuffleBatch.getValue())) {
                for (; ; ) {
                    Record<?> shuffleItem = shuffleQueueBatch.poll();
                    if (shuffleItem == null) {
                        break;
                    }
                    try {
                        shuffleQueue.put(shuffleItem);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            shuffleQueueBatch.clear();
        }
        shuffleBufferSize = 0;
        lastModify = System.currentTimeMillis();
    }
}
