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

public class LocalEdgeSocketRecordQueue implements EdgeSocketRecordQueue {
    private final BlockingQueue<EdgeSocketQueuedRecord> queue;

    /**
     * Create local bounded queue for ingress buffering.
     *
     * @param capacity queue capacity configured by source option
     */
    public LocalEdgeSocketRecordQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Try to enqueue one record without blocking.
     *
     * @param record decoded ingress record
     * @return {@link QueueOfferResult#ACCEPTED} when enqueued, otherwise {@link
     *     QueueOfferResult#RETRY_FULL}
     */
    @Override
    public QueueOfferResult offer(EdgeSocketQueuedRecord record) {
        if (!queue.offer(record)) {
            return QueueOfferResult.RETRY_FULL;
        }
        return QueueOfferResult.ACCEPTED;
    }

    /**
     * Poll one record from queue.
     *
     * @return next queued record, or null if empty
     */
    @Override
    public EdgeSocketQueuedRecord poll() {
        return queue.poll();
    }
}
