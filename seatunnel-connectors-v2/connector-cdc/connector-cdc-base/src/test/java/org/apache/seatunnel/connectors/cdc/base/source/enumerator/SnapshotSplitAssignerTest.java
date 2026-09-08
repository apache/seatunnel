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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SnapshotSplitAssignerTest {

    @Test
    public void testAddSplitsShouldKeepCompletedFinishedSplitOutOfRemainingQueue() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(
                finishedSplit.splitId(),
                new SnapshotSplitWatermark(
                        finishedSplit.splitId(),
                        finishedSplit.getLowWatermark(),
                        finishedSplit.getHighWatermark()));

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, completedOffsets);

        splitAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState state = splitAssigner.snapshotState(11L);
        Assertions.assertTrue(state.getRemainingSplits().isEmpty());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()), state.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                state.getSplitCompletedOffsets().keySet());
        Assertions.assertFalse(splitAssigner.waitingForCompletedSplits());

        splitAssigner.notifyCheckpointComplete(11L);
        Assertions.assertTrue(splitAssigner.isCompleted());
    }

    @Test
    public void testAddSplitsShouldReplayFinishedSplitWithoutCompletedWatermark() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.1");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);

        SnapshotSplitAssigner<?> splitAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        splitAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState state = splitAssigner.snapshotState(12L);
        Assertions.assertEquals(1, state.getRemainingSplits().size());
        Assertions.assertEquals(
                finishedSplit.splitId(), state.getRemainingSplits().get(0).splitId());
        Assertions.assertTrue(state.getAssignedSplits().isEmpty());
        Assertions.assertTrue(state.getSplitCompletedOffsets().isEmpty());
        Assertions.assertTrue(splitAssigner.waitingForCompletedSplits());
    }

    @Test
    public void testRestoreAfterCheckpointedCompletionShouldKeepFinishedSplitOutOfReplayQueue() {
        SnapshotSplit finishedSplit = createFinishedSnapshotSplit("db1.table1.2");
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);
        SnapshotSplitAssigner<?> runningAssigner =
                createRestoredSnapshotSplitAssigner(assignedSplits, new HashMap<>());

        runningAssigner.onCompletedSplits(
                Collections.singletonList(createWatermark(finishedSplit)));
        SnapshotPhaseState checkpointState = runningAssigner.snapshotState(13L);

        SnapshotSplitAssigner<?> restoredAssigner =
                createRestoredSnapshotSplitAssigner(
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());

        restoredAssigner.addSplits(Collections.singletonList(finishedSplit));

        SnapshotPhaseState restoredState = restoredAssigner.snapshotState(14L);
        Assertions.assertTrue(restoredState.getRemainingSplits().isEmpty());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                restoredState.getAssignedSplits().keySet());
        Assertions.assertEquals(
                Collections.singleton(finishedSplit.splitId()),
                restoredState.getSplitCompletedOffsets().keySet());
        Assertions.assertFalse(restoredAssigner.waitingForCompletedSplits());
    }

    /**
     * A standalone assigner (mirrors {@link SnapshotOnlySplitAssigner}'s construction) may release
     * dialect-owned enumerator resources once its snapshot phase has durably, checkpoint-confirmed
     * completed - there is no incremental phase left that still needs them.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void closeReleasesEnumeratorResourcesWhenStandaloneAndCompleted() {
        DataSourceDialect dialect = mock(DataSourceDialect.class);
        SnapshotSplitAssigner splitAssigner =
                createSnapshotSplitAssignerForClose(
                        dialect,
                        /* releasesEnumeratorResourcesOnCompletion= */ true,
                        /* assignerCompleted= */ true);

        // close() only releases resources open() actually acquired; exercise the real lifecycle
        // rather than calling close() on an assigner that was never opened.
        splitAssigner.open();
        splitAssigner.close();

        verify(dialect, times(1)).openEnumerator(any());
        verify(dialect, times(1)).closeEnumerator(any());
    }

    /**
     * An assigner constructed the way {@link HybridSplitAssigner} constructs its snapshot phase
     * must never release dialect-owned enumerator resources itself, even once its own snapshot work
     * is durably complete: the incremental phase it hands off to keeps depending on the same
     * resources (e.g. a PostgreSQL persistent replication slot) for the rest of the job's lifetime,
     * and close() cannot tell a genuine final stop apart from a Zeta failover/restart.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void closeSkipsEnumeratorResourcesWhenWrappedByHybridAssigner() {
        DataSourceDialect dialect = mock(DataSourceDialect.class);
        SnapshotSplitAssigner splitAssigner =
                createSnapshotSplitAssignerForClose(
                        dialect,
                        /* releasesEnumeratorResourcesOnCompletion= */ false,
                        /* assignerCompleted= */ true);

        splitAssigner.open();
        splitAssigner.close();

        verify(dialect, times(1)).openEnumerator(any());
        verify(dialect, never()).closeEnumerator(any());
    }

    /**
     * Even a standalone assigner must not release dialect-owned enumerator resources on a close()
     * reached before its snapshot phase is checkpoint-confirmed complete: that close() could be a
     * Zeta failover/restart mid-snapshot rather than the job's genuine final stop, and the
     * restarted job still needs those resources (e.g. a persistent replication slot) to resume.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void closeSkipsEnumeratorResourcesWhenSnapshotNotYetDurablyCompleted() {
        DataSourceDialect dialect = mock(DataSourceDialect.class);
        SnapshotSplitAssigner splitAssigner =
                createSnapshotSplitAssignerForClose(
                        dialect,
                        /* releasesEnumeratorResourcesOnCompletion= */ true,
                        /* assignerCompleted= */ false);

        splitAssigner.open();
        splitAssigner.close();

        verify(dialect, times(1)).openEnumerator(any());
        verify(dialect, never()).closeEnumerator(any());
    }

    /**
     * Regression for the enumerator-resource double-close: when {@code open()} fails after {@code
     * dialect.openEnumerator(...)} has run, its catch block cleans up via {@code
     * closeEnumerator(...)} exactly once. The enumerator framework still calls {@code close()}
     * afterward (SeaTunnel's own contract, not simulated here directly) - that must not invoke
     * {@code closeEnumerator(...)} a second time, which a slot-dropping dialect implementation
     * would otherwise fail on with "slot does not exist".
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void closeAfterFailedOpenDoesNotDoubleInvokeCloseEnumerator() {
        DataSourceDialect dialect = mock(DataSourceDialect.class);
        Mockito.when(dialect.createChunkSplitter(any()))
                .thenThrow(new RuntimeException("simulated chunk splitter failure"));
        SnapshotSplitAssigner splitAssigner =
                createSnapshotSplitAssignerForClose(
                        dialect,
                        /* releasesEnumeratorResourcesOnCompletion= */ true,
                        /* assignerCompleted= */ true);

        Assertions.assertThrows(RuntimeException.class, splitAssigner::open);
        verify(dialect, times(1)).openEnumerator(any());
        verify(dialect, times(1)).closeEnumerator(any());

        // The framework still calls close() after a failed open(); it must not re-invoke
        // closeEnumerator() for a resource open() already gave up on.
        splitAssigner.close();
        verify(dialect, times(1)).closeEnumerator(any());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SnapshotSplitAssigner createSnapshotSplitAssignerForClose(
            DataSourceDialect dialect,
            boolean releasesEnumeratorResourcesOnCompletion,
            boolean assignerCompleted) {
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        assignerCompleted,
                        Collections.emptyList(),
                        false,
                        true);
        SplitAssigner.Context context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.emptySet(),
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());
        return new SnapshotSplitAssigner<>(
                context, 1, checkpointState, dialect, releasesEnumeratorResourcesOnCompletion);
    }

    private SnapshotSplitAssigner<?> createRestoredSnapshotSplitAssigner(
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> completedOffsets) {
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        assignedSplits,
                        completedOffsets,
                        false,
                        Collections.emptyList(),
                        false,
                        true);
        SplitAssigner.Context<?> context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.singleton(TableId.parse("db1.table1")),
                        checkpointState.getAssignedSplits(),
                        checkpointState.getSplitCompletedOffsets());
        return new SnapshotSplitAssigner<>(context, 10, checkpointState, null, true);
    }

    private SnapshotSplit createFinishedSnapshotSplit(String splitId) {
        return new SnapshotSplit(
                splitId,
                TableId.parse("db1.table1"),
                null,
                null,
                null,
                new TestOffset(1L),
                new TestOffset(2L));
    }

    private SnapshotSplitWatermark createWatermark(SnapshotSplit finishedSplit) {
        return new SnapshotSplitWatermark(
                finishedSplit.splitId(),
                finishedSplit.getLowWatermark(),
                finishedSplit.getHighWatermark());
    }

    private static final class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private TestOffset(long value) {
            this.offset = Collections.singletonMap("pos", String.valueOf(value));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(this.offset.get("pos")),
                    Long.parseLong(other.getOffset().get("pos")));
        }
    }
}
