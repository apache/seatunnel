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

package org.apache.seatunnel.connectors.cdc.base.source.progress;

import org.apache.seatunnel.api.cdc.CdcProgressAccuracy;
import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class CdcReaderProgressTrackerTest {

    @Test
    void publicationBudgetDoesNotReadCoordinatesBetweenSuccessfulSamples() {
        AtomicLong nanos = new AtomicLong();
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("Test-CDC", "TEST", nanos::get);
        Offset offset = Mockito.mock(Offset.class);
        Map<String, String> values = new HashMap<>(Collections.singletonMap("pos", "10"));
        Mockito.when(offset.getOffset()).thenReturn(values);
        IncrementalSplitState state = createIncrementalSplitState(offset);
        tracker.recordSplitState(state);
        Assertions.assertTrue(tracker.shouldRecordEmission(state));
        tracker.recordEmission(state, 90L, 100L);
        int initializedCopies = 2;
        values.put("pos", "11");
        for (int i = 0; i < 1000; i++) {
            Assertions.assertFalse(tracker.shouldRecordEmission(state));
        }
        Mockito.verify(offset, Mockito.times(initializedCopies)).getOffset();
        Assertions.assertEquals(
                "10",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
        nanos.set(TimeUnit.SECONDS.toNanos(1) - 1);
        Assertions.assertFalse(tracker.shouldRecordEmission(state));
        nanos.incrementAndGet();
        Assertions.assertTrue(tracker.shouldRecordEmission(state));
        tracker.recordEmission(state, 80L, 50L); // Wall-clock rollback does not defer a due sample.
        Mockito.verify(offset, Mockito.times(initializedCopies + 1)).getOffset();
        Assertions.assertEquals(
                "11",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(50L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(80L, tracker.current().getLastSourceEventAt());
        IncrementalSplitState other =
                new IncrementalSplitState(
                        new IncrementalSplit(
                                "other",
                                Collections.emptyList(),
                                new TestOffset(20),
                                null,
                                Collections.emptyList()));
        Assertions.assertTrue(tracker.shouldRecordEmission(other));
    }

    @Test
    void nullCoordinatesStayUnavailable() {
        Offset offset = Mockito.mock(Offset.class);
        Mockito.when(offset.getOffset()).thenReturn(Collections.singletonMap("optional", null));
        Assertions.assertNull(CdcProgressPositions.fromOffset("TEST", offset));
        Mockito.when(offset.getOffset()).thenReturn(Collections.emptyMap());
        Assertions.assertNull(CdcProgressPositions.fromOffset("TEST", offset));
    }

    @Test
    void mutableOffsetDoesNotChangePublishedPositionUntilSuccessfulEmission() {
        CdcReaderProgressTracker tracker = new CdcReaderProgressTracker("Test-CDC", "TEST");
        TestOffset offset = new TestOffset(10L);
        IncrementalSplitState state = createIncrementalSplitState(offset);
        tracker.recordEmission(state, 90L, 100L);
        CdcReaderProgressReport first = tracker.current();

        offset.getOffset().put("pos", "11");
        CdcReaderProgressReport pending = tracker.current();
        Assertions.assertEquals(
                "10", pending.getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(100L, pending.getLastPositionChangeAt());
        Assertions.assertEquals(90L, pending.getLastSourceEventAt());

        tracker.recordEmission(state, 190L, 200L);
        CdcReaderProgressReport second = tracker.current();
        Assertions.assertEquals(
                "11", second.getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(200L, second.getLastPositionChangeAt());
        Assertions.assertEquals(190L, second.getLastSourceEventAt());
        Assertions.assertEquals(
                "10", first.getCurrentConsumedPosition().getValue().getValues().get("pos"));

        offset.getOffset().clear();
        Assertions.assertEquals(
                second.getCurrentConsumedPosition().getValue().getValues(),
                tracker.current().getCurrentConsumedPosition().getValue().getValues());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> second.getCurrentConsumedPosition().getValue().getValues().put("pos", "99"));
    }

    @Test
    void configuredMutableOffsetIsDetachedWithoutClaimingEmission() {
        CdcReaderProgressTracker tracker = new CdcReaderProgressTracker("Test-CDC", "TEST");
        TestOffset offset = new TestOffset(10L);
        IncrementalSplitState state = createIncrementalSplitState(offset);
        tracker.recordSplitState(state);
        offset.getOffset().put("pos", "11");

        CdcReaderProgressReport report = tracker.current();
        Assertions.assertEquals(
                CdcProgressAccuracy.BEST_EFFORT, report.getCurrentConsumedPosition().getAccuracy());
        Assertions.assertEquals(
                "10", report.getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(0L, report.getLastPositionChangeAt());
        tracker.recordEmission(state, null, 100L);
        Assertions.assertEquals(
                "11",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(100L, tracker.current().getLastPositionChangeAt());
    }

    @Test
    void equalCoordinatesDoNotAdvancePositionChangeTime() {
        CdcReaderProgressTracker tracker = new CdcReaderProgressTracker("Test-CDC", "TEST");
        IncrementalSplitState state = createIncrementalSplitState(new TestOffset(10L));
        tracker.recordEmission(state, 90L, 100L);
        state.setStartupOffset(new TestOffset(10L));
        tracker.recordEmission(state, 190L, 200L);
        Assertions.assertEquals(100L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(190L, tracker.current().getLastSourceEventAt());
    }

    @Test
    void sameMutableOffsetAdvancesPositionChangeTimeAfterEmission() {
        CdcReaderProgressTracker tracker = new CdcReaderProgressTracker("Test-CDC", "TEST");
        TestOffset offset = new TestOffset(10L);
        IncrementalSplitState state = createIncrementalSplitState(offset);
        tracker.recordEmission(state, 90L, 100L);
        offset.getOffset().put("pos", "11");
        tracker.recordEmission(state, 190L, 200L);
        Assertions.assertEquals(200L, tracker.current().getLastPositionChangeAt());
    }

    @Test
    @EnabledIfSystemProperty(named = "cdc.progress.benchmark", matches = "true")
    void measureMutableCoordinatePublicationCost() {
        for (int fields : new int[] {1, 8, 64}) {
            for (boolean changing : new boolean[] {false, true}) {
                measureMutableCoordinates(fields, changing);
            }
        }
    }

    private void measureMutableCoordinates(int fields, boolean changing) {
        TestOffset offset = new TestOffset(10L);
        for (int field = 1; field < fields; field++) {
            offset.getOffset().put("coordinate-" + field, "fixture-coordinate-value");
        }
        IncrementalSplitState state = createIncrementalSplitState(offset);
        CdcReaderProgressTracker tracker = new CdcReaderProgressTracker("Test-CDC", "TEST");
        Long sourceTime = Long.valueOf(1000L);
        for (int i = 0; i < 200_000; i++) {
            offset.getOffset().put("pos", changing && (i & 1) == 0 ? "11" : "10");
            tracker.recordEmission(state, sourceTime, System.currentTimeMillis());
        }
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        bean.setThreadAllocatedMemoryEnabled(true);
        long thread = Thread.currentThread().getId();
        long bytes = bean.getThreadAllocatedBytes(thread);
        long start = System.nanoTime();
        int iterations = 1_000_000;
        for (int i = 0; i < iterations; i++) {
            offset.getOffset().put("pos", changing && (i & 1) == 0 ? "11" : "10");
            tracker.recordEmission(state, sourceTime, System.currentTimeMillis());
        }
        long nanos = System.nanoTime() - start;
        long allocated = bean.getThreadAllocatedBytes(thread) - bytes;
        System.out.printf(
                "CDC_TRACKER mutable-coordinates fields=%d changing=%s iterations=%d ns/op=%.2f bytes/op=%.2f java=%s%n",
                fields,
                changing,
                iterations,
                (double) nanos / iterations,
                (double) allocated / iterations,
                System.getProperty("java.version"));
        Assertions.assertEquals(
                fields,
                tracker.current().getCurrentConsumedPosition().getValue().getValues().size());
        Assertions.assertEquals(
                "10",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
    }

    @Test
    @EnabledIfSystemProperty(named = "cdc.progress.benchmark", matches = "true")
    void measureRepeatedPositionEmissionCost() {
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MySQL-CDC", "MYSQL_BINLOG");
        IncrementalSplitState state = createIncrementalSplitState(new TestOffset(10));
        Long sourceTime = Long.valueOf(1000);
        for (int i = 0; i < 200_000; i++) {
            tracker.recordEmission(state, sourceTime, System.currentTimeMillis());
        }
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported());
        bean.setThreadAllocatedMemoryEnabled(true);
        long thread = Thread.currentThread().getId();
        long bytes = bean.getThreadAllocatedBytes(thread);
        long start = System.nanoTime();
        int iterations = 1_000_000;
        for (int i = 0; i < iterations; i++) {
            tracker.recordEmission(state, sourceTime, System.currentTimeMillis());
        }
        long nanos = System.nanoTime() - start;
        long allocated = bean.getThreadAllocatedBytes(thread) - bytes;
        System.out.printf(
                "CDC_TRACKER repeated-position iterations=%d ns/op=%.2f bytes/op=%.2f java=%s%n",
                iterations,
                (double) nanos / iterations,
                (double) allocated / iterations,
                System.getProperty("java.version"));
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT,
                tracker.current().getCurrentConsumedPosition().getAccuracy());
    }

    @Test
    void testTracksCurrentOffsetWithoutClaimingCheckpointOrRestoreProgress() {
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MySQL-CDC", "MYSQL_BINLOG");
        IncrementalSplitState splitState = createIncrementalSplitState(new TestOffset(10L));

        tracker.recordSplitState(splitState);
        Assertions.assertEquals(0L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(
                CdcProgressAccuracy.BEST_EFFORT,
                tracker.current().getCurrentConsumedPosition().getAccuracy());

        tracker.recordEmission(splitState, 90L, 100L);
        CdcReaderProgressReport first = tracker.current();
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT, first.getCurrentConsumedPosition().getAccuracy());

        Assertions.assertEquals(CdcProgressLifecycle.INCREMENTAL, first.getLifecycle());
        Assertions.assertEquals("incremental-split", first.getActiveSplitId());
        Assertions.assertEquals(
                "10", first.getCurrentConsumedPosition().getValue().getValues().get("pos"));
        Assertions.assertEquals(100L, first.getLastPositionChangeAt());
        Assertions.assertEquals(90L, first.getLastSourceEventAt());
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED,
                first.getLastCompletedCheckpointPosition().getAccuracy());
        Assertions.assertEquals(
                CdcProgressAccuracy.UNSUPPORTED, first.getRestoredPosition().getAccuracy());

        tracker.recordEmission(splitState, null, 200L);
        Assertions.assertEquals(100L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(90L, tracker.current().getLastSourceEventAt());

        splitState.setStartupOffset(new TestOffset(11L));
        tracker.recordEmission(splitState, 190L, 300L);
        Assertions.assertEquals(300L, tracker.current().getLastPositionChangeAt());
        Assertions.assertEquals(
                "11",
                tracker.current().getCurrentConsumedPosition().getValue().getValues().get("pos"));
    }

    @Test
    void testTracksCatchUpToIncrementalTransition() {
        CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MySQL-CDC", "MYSQL_BINLOG", () -> 0L);
        TableId tableId = TableId.parse("inventory.orders");
        CompletedSnapshotSplitInfo completedSplit =
                new CompletedSnapshotSplitInfo(
                        "snapshot-split",
                        tableId,
                        null,
                        null,
                        null,
                        new SnapshotSplitWatermark(
                                "snapshot-split", new TestOffset(5L), new TestOffset(15L)));
        IncrementalSplitState splitState =
                new IncrementalSplitState(
                        new IncrementalSplit(
                                "incremental-split",
                                Collections.singletonList(tableId),
                                new TestOffset(10L),
                                null,
                                new ArrayList<>(Collections.singletonList(completedSplit))));

        tracker.recordSplitState(splitState);
        Assertions.assertEquals(CdcProgressLifecycle.CATCH_UP, tracker.current().getLifecycle());
        tracker.recordEmission(splitState, null, 90L);
        Assertions.assertFalse(tracker.shouldRecordEmission(splitState));

        Assertions.assertTrue(splitState.markEnterPureIncrementPhaseIfNeed(new TestOffset(15L)));
        splitState.setStartupOffset(new TestOffset(15L));
        Assertions.assertTrue(tracker.shouldRecordEmission(splitState));
        tracker.recordEmission(splitState, null, 100L);

        Assertions.assertEquals(CdcProgressLifecycle.INCREMENTAL, tracker.current().getLifecycle());
    }

    private IncrementalSplitState createIncrementalSplitState(Offset offset) {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("inventory.orders")),
                        offset,
                        null,
                        Collections.emptyList());
        return new IncrementalSplitState(split);
    }

    private static final class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = new HashMap<>(Collections.singletonMap("pos", String.valueOf(value)));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
