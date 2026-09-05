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

package org.apache.seatunnel.e2e.connector.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies Kafka E2E helper behavior that is independent from the Testcontainers runtime. */
public class KafkaITTest {

    /**
     * READ_COMMITTED scans must continue across aborted or control records that advance position.
     */
    @Test
    public void shouldContinueEmptyReadCommittedPollWhenPositionAdvances() {
        // Aborted transactions and control records can move the position without returning visible
        // READ_COMMITTED records, so the helper must continue scanning that offset range.
        Assertions.assertFalse(KafkaIT.shouldStopAfterEmptyReadCommittedPoll(11L, 10L, 20));
        Assertions.assertFalse(KafkaIT.shouldStopAfterEmptyReadCommittedPoll(15L, 10L, 30));
        Assertions.assertFalse(KafkaIT.shouldStopAfterEmptyReadCommittedPoll(10L, 10L, 19));
        Assertions.assertTrue(KafkaIT.shouldStopAfterEmptyReadCommittedPoll(10L, 10L, 20));
    }
}
