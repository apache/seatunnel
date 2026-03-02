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

package org.apache.seatunnel.engine.server.task.group;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateDisruptor;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEvent;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEventFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_CAPACITY;
import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.INTERMEDIATE_QUEUE_SIZE;

public class TaskGroupWithIntermediateDisruptor extends AbstractTaskGroupWithIntermediateQueue {

    public static final int RING_BUFFER_SIZE = 1024;

    public TaskGroupWithIntermediateDisruptor(
            TaskGroupLocation taskGroupLocation, String taskGroupName, Collection<Task> tasks) {
        super(taskGroupLocation, taskGroupName, tasks);
    }

    private Map<Long, QueueWithMetrics> disruptor = null;

    private static final class QueueWithMetrics {
        private final Disruptor<RecordEvent> disruptor;
        private final Counter totalQueueSize;
        private final Counter queueSize;
        private final Counter putBlockedNs;

        private QueueWithMetrics(
                Disruptor<RecordEvent> disruptor,
                Counter totalQueueSize,
                Counter queueSize,
                Counter putBlockedNs) {
            this.disruptor = disruptor;
            this.totalQueueSize = totalQueueSize;
            this.queueSize = queueSize;
            this.putBlockedNs = putBlockedNs;
        }
    }

    @Override
    public void init() {
        disruptor = new ConcurrentHashMap<>();
        getTasks().stream()
                .filter(SeaTunnelTask.class::isInstance)
                .map(s -> (SeaTunnelTask) s)
                .forEach(s -> s.setTaskGroup(this));
    }

    @Override
    public AbstractIntermediateQueue<?> getQueueCache(
            long id, int capacity, MetricsContext metricsContext) {
        int effectiveCapacity = capacity > 0 ? roundUpToPowerOfTwo(capacity) : RING_BUFFER_SIZE;

        disruptor.computeIfAbsent(
                id,
                i -> {
                    Counter totalQueueSize = metricsContext.counter(INTERMEDIATE_QUEUE_SIZE);
                    Counter queueSize = metricsContext.counter(INTERMEDIATE_QUEUE_SIZE + "#" + i);
                    Counter putBlockedNs =
                            metricsContext.counter(INTERMEDIATE_QUEUE_PUT_BLOCKED_NANOS + "#" + i);
                    Counter capacityCounter =
                            metricsContext.counter(INTERMEDIATE_QUEUE_CAPACITY + "#" + i);
                    capacityCounter.set(effectiveCapacity);

                    EventFactory<RecordEvent> eventFactory = new RecordEventFactory();
                    Disruptor<RecordEvent> disruptor =
                            new Disruptor<>(
                                    eventFactory,
                                    effectiveCapacity,
                                    DaemonThreadFactory.INSTANCE,
                                    ProducerType.SINGLE,
                                    new YieldingWaitStrategy());
                    return new QueueWithMetrics(disruptor, totalQueueSize, queueSize, putBlockedNs);
                });

        QueueWithMetrics cache = disruptor.get(id);
        return new IntermediateDisruptor(
                cache.disruptor, cache.totalQueueSize, cache.queueSize, cache.putBlockedNs);
    }

    private static int roundUpToPowerOfTwo(int value) {
        int v = Math.max(2, value);
        int highestOneBit = Integer.highestOneBit(v);
        return v == highestOneBit ? v : highestOneBit << 1;
    }

    @Override
    public TaskGroupType getTaskGroupType() {
        return TaskGroupType.INTERMEDIATE_DISRUPTOR_QUEUE;
    }
}
