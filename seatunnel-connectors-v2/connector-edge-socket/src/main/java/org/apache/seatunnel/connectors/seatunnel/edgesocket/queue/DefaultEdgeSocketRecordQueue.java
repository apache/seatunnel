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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class DefaultEdgeSocketRecordQueue implements EdgeSocketRecordQueue {

    private final int capacity;
    private final int backpressureHighWaterMark;
    private final BlockingQueue<EdgeSocketQueuedRecord> queue;

    /**
     * @param capacity maximum records buffered between ingress and source emission
     * @param backpressureWatermarkRatio high-water mark ratio in (0, 1]
     */
    public DefaultEdgeSocketRecordQueue(int capacity, double backpressureWatermarkRatio) {
        this.capacity = capacity;
        this.backpressureHighWaterMark = (int) Math.ceil(capacity * backpressureWatermarkRatio);
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public QueueOfferResult offer(EdgeSocketQueuedRecord record) {
        if (!queue.offer(record)) {
            return QueueOfferResult.RETRY_FULL;
        }
        return QueueOfferResult.ACCEPTED;
    }

    @Override
    public EdgeSocketQueuedRecord poll() {
        return queue.poll();
    }

    /**
     * {@link ArrayBlockingQueue#toArray()} is thread-safe and returns a consistent point-in-time
     * view without blocking or draining the queue.
     */
    @Override
    public EdgeSocketQueuedRecord[] snapshot() {
        return queue.toArray(new EdgeSocketQueuedRecord[0]);
    }

    @Override
    public boolean isBackpressure() {
        if (capacity <= 0) {
            return false;
        }
        return capacity - queue.remainingCapacity() >= backpressureHighWaterMark;
    }
}
