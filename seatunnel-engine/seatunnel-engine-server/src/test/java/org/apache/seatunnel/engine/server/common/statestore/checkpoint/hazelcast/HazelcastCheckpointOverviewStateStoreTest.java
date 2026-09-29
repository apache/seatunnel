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

package org.apache.seatunnel.engine.server.common.statestore.checkpoint.hazelcast;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HazelcastCheckpointOverviewStateStoreTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeAll
    static void beforeAll() {
        Config config = new Config();
        config.setClusterName("HazelcastCheckpointOverviewStateStoreTest-" + System.nanoTime());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void afterAll() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void countAccessorsShouldUseLogicalOverviewCounts() {
        IMap<Long, CheckpointOverview> iMap =
                hazelcastInstance.getMap("checkpoint-overview-counts");
        iMap.clear();
        HazelcastCheckpointOverviewStateStore store =
                new HazelcastCheckpointOverviewStateStore(iMap);

        store.updateOverview(
                1L,
                1,
                pipeline -> {
                    pipeline.getInProgress()
                            .add(
                                    new InProgressCheckpoint(
                                            101L, CheckpointType.CHECKPOINT_TYPE, 10L, 1, 2));
                    pipeline.getInProgress()
                            .add(
                                    new InProgressCheckpoint(
                                            102L, CheckpointType.CHECKPOINT_TYPE, 20L, 1, 2));
                    pipeline.addHistory(historyEntry(1L, 1, 201L), 8);
                });
        store.updateOverview(1L, 2, pipeline -> pipeline.addHistory(historyEntry(1L, 2, 202L), 8));
        store.updateOverview(
                2L,
                1,
                pipeline -> {
                    pipeline.getInProgress()
                            .add(
                                    new InProgressCheckpoint(
                                            103L, CheckpointType.CHECKPOINT_TYPE, 30L, 1, 2));
                    pipeline.addHistory(historyEntry(2L, 1, 203L), 8);
                });

        awaitOverviewStats(store, 2L, 3L, 3L, 2);

        store.remove(1L);

        awaitOverviewStats(store, 1L, 1L, 1L, 1);
    }

    private static void awaitOverviewStats(
            HazelcastCheckpointOverviewStateStore store,
            long expectedOverviewJobCount,
            long expectedInProgressCheckpointCount,
            long expectedRetainedHistoryCount,
            int expectedSize) {
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(expectedOverviewJobCount, store.getOverviewJobCount());
                            assertEquals(
                                    expectedInProgressCheckpointCount,
                                    store.getInProgressCheckpointCount());
                            assertEquals(
                                    expectedRetainedHistoryCount, store.getRetainedHistoryCount());
                            assertEquals(expectedSize, store.size());
                        });
    }

    private static CheckpointHistoryEntry historyEntry(
            long jobId, int pipelineId, long checkpointId) {
        return CheckpointHistoryEntry.builder()
                .jobId(jobId)
                .pipelineId(pipelineId)
                .checkpointInfo(
                        CheckpointInfo.builder()
                                .checkpointId(checkpointId)
                                .checkpointType(CheckpointType.CHECKPOINT_TYPE)
                                .status(CheckpointStatus.COMPLETED)
                                .triggerTimestamp(checkpointId)
                                .build())
                .build();
    }
}
