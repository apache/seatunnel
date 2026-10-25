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

package org.apache.seatunnel.engine.core.checkpoint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the retained checkpoint history window, which is what {@code
 * seatunnel.engine.checkpoint.monitor-history-size} configures.
 */
public class PipelineCheckpointOverviewTest {

    private static CheckpointHistoryEntry entry(long checkpointId) {
        CheckpointInfo info = new CheckpointInfo();
        info.setCheckpointId(checkpointId);
        CheckpointHistoryEntry historyEntry = new CheckpointHistoryEntry();
        historyEntry.setCheckpointInfo(info);
        return historyEntry;
    }

    @Test
    public void testHistoryIsTrimmedToTheGivenMaximum() {
        PipelineCheckpointOverview overview = new PipelineCheckpointOverview();

        for (long id = 1; id <= 10; id++) {
            overview.addHistory(entry(id), 3);
        }

        List<CheckpointHistoryEntry> history = new ArrayList<>(overview.getHistory());
        Assertions.assertEquals(3, history.size());

        // Newest first: addHistory prepends, so the retained window is the most
        // recent entries, not the oldest. Anything consuming this as a trend
        // depends on that order.
        Assertions.assertEquals(10L, history.get(0).getCheckpointInfo().getCheckpointId());
        Assertions.assertEquals(9L, history.get(1).getCheckpointInfo().getCheckpointId());
        Assertions.assertEquals(8L, history.get(2).getCheckpointInfo().getCheckpointId());
    }

    @Test
    public void testHistoryHonoursAWindowLargerThanTheEntryCount() {
        PipelineCheckpointOverview overview = new PipelineCheckpointOverview();

        for (long id = 1; id <= 4; id++) {
            overview.addHistory(entry(id), 64);
        }

        Assertions.assertEquals(4, overview.getHistory().size());
    }

    @Test
    public void testAWindowOfOneKeepsOnlyTheNewestEntry() {
        PipelineCheckpointOverview overview = new PipelineCheckpointOverview();

        overview.addHistory(entry(1), 1);
        overview.addHistory(entry(2), 1);

        Assertions.assertEquals(1, overview.getHistory().size());
        Assertions.assertEquals(
                2L, overview.getHistory().peekFirst().getCheckpointInfo().getCheckpointId());
    }
}
