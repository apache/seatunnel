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

import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MagicNumber")
@Slf4j
public class ShuffleSinkFlowLifeCycle extends AbstractFlowLifeCycle implements OneInputFlowLifeCycle<Record<?>> {
    private Map<String, IQueue<Record<?>>> shuffles;
    private long shuffleBufferTimesMillis = 1000;
    private int shuffleBufferSize = 1024;
    private int shuffleBatchSize = 1024;
    private Map<String, Queue<Record<?>>> shuffleBuffer;
    private ShuffleStrategy shuffleStrategy;
    private long lastModify;

    public ShuffleSinkFlowLifeCycle(SeaTunnelTask runningTask,
                                    CompletableFuture<Void> completableFuture) {
        super(runningTask, completableFuture);
        // todo initialize shuffleStrategy
        this.shuffleStrategy = null;
        this.shuffles = shuffleStrategy.createShuffles();
    }

    @Override
    public void received(Record<?> record) throws IOException {
        if (record.getData() instanceof Barrier) {
            // flush shuffle buffer
            shuffleFlush();

            Barrier barrier = (Barrier) record.getData();
            runningTask.ack(barrier);
            if (barrier.prepareClose()) {
                prepareClose = true;
            }
            // The barrier needs to be replicated to all channels
            for (Map.Entry<String, IQueue<Record<?>>> shuffle : shuffles.entrySet()) {
                IQueue<Record<?>> shuffleQueue = shuffle.getValue();
                try {
                    shuffleQueue.put(record);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
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
        for (Map.Entry<String, IQueue<Record<?>>> shuffleBatch : shuffles.entrySet()) {
            shuffleBatch.getValue().destroy();
        }
    }

    public CompletableFuture<Boolean> registryScheduleFlushTask(ScheduledExecutorService scheduledExecutorService) {
        // todo Register when the job started, Unload at the end(pause/cancel/crash) of the job
        CompletableFuture<Boolean> completedFuture = new CompletableFuture();
        Runnable scheduleFlushTask = new Runnable() {
            @Override
            public void run() {
                if (!prepareClose
                    && shuffleBufferSize > 0
                    && System.currentTimeMillis() - lastModify > shuffleBufferTimesMillis) {

                    try {
                        shuffleFlush();
                    } catch (Exception e) {
                        log.error("Execute schedule task error.", e);
                    }
                }

                // submit next task
                if (!prepareClose) {
                    Runnable nextScheduleFlushTask = this;
                    scheduledExecutorService.schedule(nextScheduleFlushTask, shuffleBufferTimesMillis, TimeUnit.MILLISECONDS);
                } else {
                    completedFuture.complete(true);
                }
            }
        };
        scheduledExecutorService.schedule(scheduleFlushTask, shuffleBufferTimesMillis, TimeUnit.MILLISECONDS);
        return completedFuture;
    }

    private synchronized void shuffleItem(Record<?> record) {
        String shuffleKey = shuffleStrategy.extractShuffleKey(record);
        shuffleBuffer.compute(shuffleKey, (key, records) -> new LinkedList<>())
            .add(record);
        shuffleBufferSize++;

        if (shuffleBufferSize >= shuffleBatchSize
            || (shuffleBufferSize > 1 && System.currentTimeMillis() - lastModify > shuffleBufferTimesMillis)) {
            shuffleFlush();
        }

        lastModify = System.currentTimeMillis();
    }

    private synchronized void shuffleFlush() {
        for (Map.Entry<String, Queue<Record<?>>> shuffleBatch : shuffleBuffer.entrySet()) {
            IQueue<Record<?>> shuffleQueue = shuffleQueue(shuffleBatch.getKey());
            Queue<Record<?>> shuffleQueueBatch = shuffleBatch.getValue();
            if (!shuffleQueue.addAll(shuffleBatch.getValue())) {
                for (; ;) {
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
    }

    private IQueue<Record<?>> shuffleQueue(String shuffleKey) {
        return shuffles.get(shuffleKey);
    }

    interface ShuffleStrategy {
        Map<String, IQueue<Record<?>>> createShuffles();

        String extractShuffleKey(Record<?> record);
    }

    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class PartitionShuffle implements ShuffleStrategy {
        private final HazelcastInstance hazelcast;
        private final String jobId;
        private final int partitionNumber;
        private QueueConfig queueConfig;

        @Override
        public Map<String, IQueue<Record<?>>> createShuffles() {
            Map<String, IQueue<Record<?>>> shuffleMap = new HashMap<>();
            for (int i = 0; i < partitionNumber; i++) {
                String queueName = String.format("PartitionShuffle[%s-%s]", jobId, i);
                QueueConfig queueConfig = hazelcast.getConfig().getQueueConfig(queueName);
                queueConfig.setMaxSize(queueConfig.getMaxSize())
                    .setBackupCount(queueConfig.getBackupCount())
                    .setAsyncBackupCount(queueConfig.getAsyncBackupCount())
                    .setEmptyQueueTtl(queueConfig.getEmptyQueueTtl());
                shuffleMap.put(String.valueOf(i), hazelcast.getQueue(queueName));
            }
            return shuffleMap;
        }

        @Override
        public String extractShuffleKey(Record<?> record) {
            return RandomStringUtils.random(partitionNumber);
        }
    }

    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class MultipleRowShuffle implements ShuffleStrategy {
        private final HazelcastInstance hazelcast;
        private final String jobId;
        private final int parallelismIndex;
        private final MultipleRowType multipleRowType;
        private QueueConfig queueConfig;

        @Override
        public Map<String, IQueue<Record<?>>> createShuffles() {
            Map<String, IQueue<Record<?>>> shuffleMap = new HashMap<>();
            for (Map.Entry<String, SeaTunnelRowType> entry : multipleRowType) {
                String queueName = String.format("MultipleRowShuffle[%s-%s-%s]", jobId, parallelismIndex, entry.getKey());
                QueueConfig queueConfig = hazelcast.getConfig().getQueueConfig(queueName);
                queueConfig.setMaxSize(queueConfig.getMaxSize())
                    .setBackupCount(queueConfig.getBackupCount())
                    .setAsyncBackupCount(queueConfig.getAsyncBackupCount())
                    .setEmptyQueueTtl(queueConfig.getEmptyQueueTtl());
                shuffleMap.put(entry.getKey(), hazelcast.getQueue(queueName));
            }
            return shuffleMap;
        }

        @Override
        public String extractShuffleKey(Record<?> record) {
            return ((SeaTunnelRow) record.getData()).getTableId();
        }
    }
}
