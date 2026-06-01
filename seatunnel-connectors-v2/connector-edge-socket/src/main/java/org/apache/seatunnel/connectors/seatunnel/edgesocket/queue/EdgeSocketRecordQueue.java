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

public interface EdgeSocketRecordQueue {

    /**
     * Offer one decoded record to local queue.
     *
     * @param record decoded ingress record
     * @return queue offer status used to build ACK/RETRY response
     */
    QueueOfferResult offer(EdgeSocketQueuedRecord record);

    /**
     * Poll one record for source emission.
     *
     * @return next queued record, or null when queue is empty
     */
    EdgeSocketQueuedRecord poll();

    /**
     * Take a point-in-time snapshot of all records currently in the queue.
     *
     * <p>The returned array is independent of the queue; subsequent offer/poll operations do not
     * affect it. This is called during checkpoint state serialization to persist
     * queued-but-not-yet-emitted records so they can be replayed after restore, enabling
     * At-Least-Once delivery within the Zeta pipeline.
     *
     * @return snapshot of current queue contents in queue order, never null
     */
    EdgeSocketQueuedRecord[] snapshot();

    /** Returns true when the queue has reached the configured ingress backpressure watermark. */
    boolean isBackpressure();
}
