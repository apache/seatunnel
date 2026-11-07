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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RestoredCommittableOperatorTest {
    @Test
    void testUpstreamReplayAndFreshCheckpoint() {
        CommitWrapper<String> replay = new CommitWrapper<>("old");
        CommitWrapper<String> live = new CommitWrapper<>("new");
        RestoredCommittableOperator.markRecovered(new CommittableWithLineage<>(replay, 7L, 0), 7);
        RestoredCommittableOperator.markRecovered(new CommittableWithLineage<>(live, 8L, 0), 7);
        Assertions.assertTrue(replay.isRecovered());
        Assertions.assertFalse(live.isRecovered());
    }

    @Test
    void testFreshJobAndEndOfInputRemainStrict() {
        CommitWrapper<String> fresh = new CommitWrapper<>("fresh");
        CommitWrapper<String> end = new CommitWrapper<>("end");
        CommitWrapper<String> unset = new CommitWrapper<>("unset");
        RestoredCommittableOperator.markRecovered(new CommittableWithLineage<>(fresh, 1L, 0), -1);
        RestoredCommittableOperator.markRecovered(new CommittableWithLineage<>(end, null, 0), 7);
        RestoredCommittableOperator.markRecovered(new CommittableWithLineage<>(unset, -1L, 0), -1);
        Assertions.assertFalse(unset.isRecovered());
        Assertions.assertFalse(fresh.isRecovered());
        Assertions.assertFalse(end.isRecovered());
    }
}
