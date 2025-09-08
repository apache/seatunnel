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

package org.apache.seatunnel.connectors.cdc.base.source.split.state;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;
import lombok.AllArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IncrementalSplitStateTest {

    @Test
    public void testMarkEnterPureIncrementPhaseIfNeed() {
        Offset startupOffset = new TestOffset(100);
        List<CompletedSnapshotSplitInfo> snapshotSplits = Collections.emptyList();
        IncrementalSplit split = createIncrementalSplit(startupOffset, snapshotSplits);
        IncrementalSplitState splitState = new IncrementalSplitState(split);
        Assertions.assertNull(splitState.getMaxSnapshotSplitsHighWatermark());
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(null));

        startupOffset = new TestOffset(100);
        snapshotSplits =
                Stream.of(
                                createCompletedSnapshotSplitInfo(
                                        "test1", new TestOffset(100), new TestOffset(100)),
                                createCompletedSnapshotSplitInfo(
                                        "test2", new TestOffset(100), new TestOffset(100)))
                        .collect(Collectors.toList());
        split = createIncrementalSplit(startupOffset, snapshotSplits);
        splitState = new IncrementalSplitState(split);
        Assertions.assertEquals(startupOffset, splitState.getMaxSnapshotSplitsHighWatermark());
        Assertions.assertFalse(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(99)));
        Assertions.assertFalse(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(snapshotSplits.isEmpty());
        Assertions.assertTrue(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(100)));
        Assertions.assertTrue(snapshotSplits.isEmpty());
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(100)));
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(101)));

        startupOffset = new TestOffset(100);
        snapshotSplits =
                Stream.of(
                                createCompletedSnapshotSplitInfo(
                                        "test1", new TestOffset(1), new TestOffset(50)),
                                createCompletedSnapshotSplitInfo(
                                        "test2", new TestOffset(50), new TestOffset(200)))
                        .collect(Collectors.toList());
        split = createIncrementalSplit(startupOffset, snapshotSplits);
        splitState = new IncrementalSplitState(split);
        Assertions.assertEquals(
                new TestOffset(200), splitState.getMaxSnapshotSplitsHighWatermark());
        Assertions.assertFalse(splitState.isEnterPureIncrementPhase());
        Assertions.assertTrue(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(201)));
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());
        Assertions.assertTrue(snapshotSplits.isEmpty());
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(200)));
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(201)));
        Assertions.assertFalse(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(202)));
    }

    @Test
    public void testAutoEnterPureIncrementPhaseIfAllowed() {
        Offset startupOffset = new TestOffset(100);
        List<CompletedSnapshotSplitInfo> snapshotSplits = Collections.emptyList();
        IncrementalSplit split = createIncrementalSplit(startupOffset, snapshotSplits);
        IncrementalSplitState splitState = new IncrementalSplitState(split);
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.autoEnterPureIncrementPhaseIfAllowed());

        startupOffset = new TestOffset(100);
        snapshotSplits =
                Stream.of(
                                createCompletedSnapshotSplitInfo(
                                        "test1", new TestOffset(100), new TestOffset(100)),
                                createCompletedSnapshotSplitInfo(
                                        "test2", new TestOffset(100), new TestOffset(100)))
                        .collect(Collectors.toList());
        split = createIncrementalSplit(startupOffset, snapshotSplits);
        splitState = new IncrementalSplitState(split);

        Assertions.assertFalse(splitState.isEnterPureIncrementPhase());
        Assertions.assertTrue(splitState.autoEnterPureIncrementPhaseIfAllowed());
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.autoEnterPureIncrementPhaseIfAllowed());
        Assertions.assertTrue(splitState.isEnterPureIncrementPhase());

        startupOffset = new TestOffset(100);
        snapshotSplits =
                Stream.of(
                                createCompletedSnapshotSplitInfo(
                                        "test1", new TestOffset(100), new TestOffset(100)),
                                createCompletedSnapshotSplitInfo(
                                        "test2", new TestOffset(100), new TestOffset(101)))
                        .collect(Collectors.toList());
        split = createIncrementalSplit(startupOffset, snapshotSplits);
        splitState = new IncrementalSplitState(split);
        Assertions.assertFalse(splitState.isEnterPureIncrementPhase());
        Assertions.assertFalse(splitState.autoEnterPureIncrementPhaseIfAllowed());
    }

    private static IncrementalSplit createIncrementalSplit(
            Offset startupOffset, List<CompletedSnapshotSplitInfo> snapshotSplits) {
        return new IncrementalSplit(
                "test",
                Arrays.asList(new TableId("db", "schema", "table")),
                startupOffset,
                null,
                snapshotSplits,
                (List<CatalogTable>) null,
                Collections.emptyMap());
    }

    private static CompletedSnapshotSplitInfo createCompletedSnapshotSplitInfo(
            String splitId, Offset lowWatermark, Offset highWatermark) {
        return new CompletedSnapshotSplitInfo(
                splitId,
                new TableId("db", "schema", "table"),
                null,
                null,
                null,
                new SnapshotSplitWatermark(null, lowWatermark, highWatermark));
    }

    @ToString
    @AllArgsConstructor
    static class TestOffset extends Offset {
        private int offset;

        @Override
        public int compareTo(Offset o) {
            return Integer.compare(offset, ((TestOffset) o).offset);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestOffset && offset == ((TestOffset) o).offset;
        }
    }
}
