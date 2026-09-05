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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.engine.common.config.server.QueueType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntermediateQueueBenchmarkTest {

    private static final int RECORD_COUNT = 10_000;

    @Test
    void shouldTransferAllRecordsThroughEachQueueImplementation() throws Exception {
        for (QueueType queueType : QueueType.values()) {
            IntermediateQueueBenchmarkState state =
                    new IntermediateQueueBenchmarkState(queueType, 1024, 4096);
            state.setUp();
            try {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    state.publish();
                }
            } finally {
                state.tearDown();
            }

            assertEquals(RECORD_COUNT, state.getPublishedRecords());
            assertEquals(RECORD_COUNT, state.getConsumedRecords());
            assertEquals(21_783_822L, state.getConsumedChecksum());
        }
    }

    @Test
    void shouldRejectNonPowerOfTwoCapacityForEachQueueType() {
        for (QueueType queueType : QueueType.values()) {
            IntermediateQueueBenchmarkState state =
                    new IntermediateQueueBenchmarkState(queueType, 1000, 4096);

            assertThrows(IllegalArgumentException.class, state::setUp);
        }
    }
}
