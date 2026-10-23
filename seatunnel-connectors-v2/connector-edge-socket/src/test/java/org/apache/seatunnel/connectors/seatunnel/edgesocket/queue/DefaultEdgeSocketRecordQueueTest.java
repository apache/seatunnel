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

import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultEdgeSocketRecordQueueTest {

    @Test
    void isBackpressureAtNinetyPercentWatermark() {
        int capacity = 10;
        int highWaterMark = (int) Math.ceil(capacity * 0.9);
        DefaultEdgeSocketRecordQueue queue = new DefaultEdgeSocketRecordQueue(capacity, 0.9);
        fillQueue(queue, highWaterMark - 1);
        Assertions.assertFalse(queue.isBackpressure());

        queue.offer(new EdgeSocketQueuedRecord(1L, new byte[0], EdgeSocketCompressionType.NONE));
        Assertions.assertTrue(queue.isBackpressure());
    }

    @Test
    void isBackpressureWhenCapacityIsOne() {
        DefaultEdgeSocketRecordQueue empty = new DefaultEdgeSocketRecordQueue(1, 0.9);
        Assertions.assertFalse(empty.isBackpressure());

        DefaultEdgeSocketRecordQueue full = new DefaultEdgeSocketRecordQueue(1, 0.9);
        full.offer(new EdgeSocketQueuedRecord(1L, new byte[0], EdgeSocketCompressionType.NONE));
        Assertions.assertTrue(full.isBackpressure());
    }

    private static void fillQueue(DefaultEdgeSocketRecordQueue queue, int count) {
        for (int i = 0; i < count; i++) {
            Assertions.assertEquals(
                    QueueOfferResult.ACCEPTED,
                    queue.offer(
                            new EdgeSocketQueuedRecord(
                                    i, new byte[0], EdgeSocketCompressionType.NONE)));
        }
    }
}
