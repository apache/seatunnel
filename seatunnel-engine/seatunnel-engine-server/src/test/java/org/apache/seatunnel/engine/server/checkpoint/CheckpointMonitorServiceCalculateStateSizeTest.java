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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class CheckpointMonitorServiceCalculateStateSizeTest {

    @Test
    void calculateStateSizeShouldSumNonNullSubtaskSizes() {
        TaskStatistics sourceStats = new TaskStatistics(1L, 3);
        Assertions.assertTrue(
                sourceStats.reportSubtaskStatistics(new SubtaskStatistics(0, 1L, 10L, null)));
        Assertions.assertTrue(
                sourceStats.reportSubtaskStatistics(new SubtaskStatistics(1, 2L, 20L, null)));

        Map<Long, TaskStatistics> taskStatistics = new HashMap<>();
        taskStatistics.put(1L, sourceStats);
        taskStatistics.put(2L, new TaskStatistics(2L, 1));
        taskStatistics.put(3L, null);

        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        1L,
                        1,
                        100L,
                        1L,
                        CheckpointType.CHECKPOINT_TYPE,
                        2L,
                        Collections.emptyMap(),
                        taskStatistics);

        Assertions.assertEquals(30L, CheckpointMonitorService.calculateStateSize(checkpoint));
    }

    @Test
    void calculateStateSizeShouldReturnZeroForEmptyStatistics() {
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        1L,
                        1,
                        100L,
                        1L,
                        CheckpointType.CHECKPOINT_TYPE,
                        2L,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        Assertions.assertEquals(0L, CheckpointMonitorService.calculateStateSize(checkpoint));
    }
}
