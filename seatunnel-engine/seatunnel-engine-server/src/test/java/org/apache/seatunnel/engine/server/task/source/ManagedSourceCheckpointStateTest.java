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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.common.runtime.source.ManagedSourceRuntimeMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

class ManagedSourceCheckpointStateTest {

    @Test
    void shouldLatchGracefulCloseUntilSchemaCheckpointEnds() {
        ManagedSourceLifecycle lifecycle = runningLifecycle();
        long requestEpoch = lifecycle.beginSchemaChange("SCHEMA", 1L);
        lifecycle.schemaTriggerRequested(requestEpoch);

        lifecycle.gracefulClose();
        Assertions.assertEquals(ManagedSourceLifecycleState.RUNNING, lifecycle.getMainState());
        Assertions.assertTrue(lifecycle.bindSchemaCheckpoint("SCHEMA", 7L, requestEpoch));
        Assertions.assertTrue(lifecycle.checkpointEnded(7L));

        Assertions.assertEquals(ManagedSourceLifecycleState.DRAINING, lifecycle.getMainState());
        Assertions.assertEquals(SchemaChangeSubState.IDLE, lifecycle.getSchemaState());
    }

    @Test
    void shouldFailSchemaChangeOnAbortAndFenceItOnRestore() {
        ManagedSourceLifecycle lifecycle = runningLifecycle();
        long requestEpoch = lifecycle.beginSchemaChange("SCHEMA", 1L);
        lifecycle.schemaTriggerRequested(requestEpoch);
        Assertions.assertTrue(lifecycle.bindSchemaCheckpoint("SCHEMA", 8L, requestEpoch));
        ManagedSourceLifecycle.Snapshot snapshot = lifecycle.snapshot();

        Assertions.assertTrue(lifecycle.checkpointAborted(8L));
        Assertions.assertTrue(lifecycle.isFailed());

        ManagedSourceLifecycle restored = new ManagedSourceLifecycle();
        restored.restoreSnapshot(snapshot);
        Assertions.assertEquals(ManagedSourceLifecycleState.RESTORING, restored.getMainState());
        Assertions.assertEquals(SchemaChangeSubState.IDLE, restored.getSchemaState());
    }

    @Test
    void shouldBlockPollingUntilCheckpointBarrierFinishes() {
        ManagedSourceLifecycle lifecycle = runningLifecycle();

        Assertions.assertTrue(lifecycle.canPoll());
        lifecycle.beginCheckpointBarrier(9L);
        Assertions.assertTrue(lifecycle.isCheckpointBarrierPending());
        Assertions.assertFalse(lifecycle.canPoll());

        ManagedSourceLifecycle.Snapshot snapshot = lifecycle.snapshot();
        lifecycle.finishCheckpointBarrier();
        Assertions.assertFalse(lifecycle.isCheckpointBarrierPending());
        Assertions.assertTrue(lifecycle.canPoll());

        ManagedSourceLifecycle restored = new ManagedSourceLifecycle();
        restored.restoreSnapshot(snapshot);
        restored.finishRestore();
        Assertions.assertFalse(restored.isCheckpointBarrierPending());
        Assertions.assertTrue(restored.canPoll());
    }

    @Test
    void shouldRoundTripAndChecksumReaderCheckpointState() throws Exception {
        ManagedReaderCheckpointState state =
                new ManagedReaderCheckpointState(
                        ManagedSourceRuntimeMode.MANAGED_READER_AND_COORDINATOR,
                        1,
                        2,
                        "digest",
                        "reader-attempt",
                        "coordinator-epoch",
                        5L,
                        new TreeSet<>(),
                        3L,
                        runningLifecycle().snapshot(),
                        Collections.singletonList("split-1"),
                        Collections.singletonList(new byte[] {1, 2, 3}));

        byte[] serialized = ManagedReaderCheckpointStateSerializer.serialize(state);
        ManagedReaderCheckpointState restored =
                ManagedReaderCheckpointStateSerializer.deserialize(serialized);

        Assertions.assertEquals(state.getRuntimeMode(), restored.getRuntimeMode());
        Assertions.assertEquals(5L, restored.getAppliedCommandWatermark());
        Assertions.assertEquals(
                Collections.singletonList("split-1"), restored.getCheckpointOwnedSplitIds());
        Assertions.assertArrayEquals(
                new byte[] {1, 2, 3}, restored.getConnectorSplitStates().get(0));

        serialized[serialized.length - 1] ^= 1;
        Assertions.assertThrows(
                IOException.class,
                () -> ManagedReaderCheckpointStateSerializer.deserialize(serialized));
    }

    @Test
    void shouldRoundTripAndChecksumCoordinatorCheckpointState() throws Exception {
        HashMap<Integer, Long> sequences = new HashMap<>();
        sequences.put(0, 2L);
        ManagedCoordinatorCheckpointState state = coordinatorState(sequences, true);

        byte[] serialized = ManagedCoordinatorCheckpointStateSerializer.serialize(state);
        ManagedCoordinatorCheckpointState restored =
                ManagedCoordinatorCheckpointStateSerializer.deserialize(serialized);

        Assertions.assertEquals(2L, restored.getNextReaderCommandSequences().get(0));
        Assertions.assertEquals(2, restored.getSourceParallelism());
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(0, 1)), restored.getNoMoreSplitsSubtasks());
        Assertions.assertTrue(restored.isAllReadersNoMoreSplits());

        serialized[serialized.length - 1] ^= 1;
        Assertions.assertThrows(
                IOException.class,
                () -> ManagedCoordinatorCheckpointStateSerializer.deserialize(serialized));
    }

    @Test
    void shouldDiscardPartialNoMoreSplitsStateAcrossRescale() {
        HashMap<Integer, Long> sequences = new HashMap<>();
        sequences.put(0, 2L);
        ManagedCoordinatorCheckpointState partial = coordinatorState(sequences, false);
        ManagedCoordinatorCheckpointState complete = coordinatorState(sequences, true);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(0, 1)),
                ManagedSourceCoordinatorRuntime.reconcileNoMoreSplitsSubtasks(partial, 2));
        Assertions.assertTrue(
                ManagedSourceCoordinatorRuntime.reconcileNoMoreSplitsSubtasks(partial, 3)
                        .isEmpty());
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(0, 1, 2)),
                ManagedSourceCoordinatorRuntime.reconcileNoMoreSplitsSubtasks(complete, 3));
    }

    @Test
    void shouldRejectInconsistentLifecycleSnapshot() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ManagedSourceLifecycle.Snapshot(
                                ManagedSourceLifecycleState.RUNNING,
                                SchemaChangeSubState.WAITING_END,
                                "",
                                -1L,
                                1L,
                                false));
    }

    @Test
    void shouldAggregateChunkedRestoreOwnershipProof() {
        SplitIdChunkAccumulator accumulator =
                new SplitIdChunkAccumulator(
                        "restore-group", 3, 1024L, new ManagedSourceMemoryBudget(1024L));

        accumulator.add("restore-group", 3, 0, Arrays.asList("split-1", "split-2"));
        accumulator.add("restore-group", 3, 2, Collections.singletonList("split-4"));
        Assertions.assertFalse(accumulator.complete());
        accumulator.add("restore-group", 3, 1, Collections.singletonList("split-3"));

        Assertions.assertTrue(accumulator.complete());
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("split-1", "split-2", "split-3", "split-4")),
                accumulator.splitIds());
    }

    @Test
    void shouldRejectConflictingOrOversizedRestoreProof() {
        SplitIdChunkAccumulator accumulator =
                new SplitIdChunkAccumulator(
                        "restore-group", 1, 128L, new ManagedSourceMemoryBudget(128L));
        accumulator.add("restore-group", 1, 0, Collections.singletonList("split-1"));

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> accumulator.add("restore-group", 1, 0, Collections.singletonList("split-2")));
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        new SplitIdChunkAccumulator(
                                        "restore-group", 1, 4L, new ManagedSourceMemoryBudget(32L))
                                .add("restore-group", 1, 0, Collections.singletonList("split-1")));
    }

    @Test
    void shouldReleaseRestoreProofMemory() {
        ManagedSourceMemoryBudget budget = new ManagedSourceMemoryBudget(1024L);
        SplitIdChunkAccumulator accumulator =
                new SplitIdChunkAccumulator("restore-group", 1, 1024L, budget);
        accumulator.add("restore-group", 1, 0, Collections.singletonList("split-1"));
        Assertions.assertTrue(budget.getUsedBytes() > 0);

        accumulator.close();

        Assertions.assertEquals(0L, budget.getUsedBytes());
    }

    private static ManagedSourceLifecycle runningLifecycle() {
        ManagedSourceLifecycle lifecycle = new ManagedSourceLifecycle();
        lifecycle.startRestore();
        lifecycle.finishRestore();
        return lifecycle;
    }

    private static ManagedCoordinatorCheckpointState coordinatorState(
            HashMap<Integer, Long> sequences, boolean allReadersNoMoreSplits) {
        return new ManagedCoordinatorCheckpointState(
                ManagedSourceRuntimeMode.MANAGED_READER_AND_COORDINATOR,
                1,
                2,
                "digest",
                2,
                new byte[] {1},
                new byte[] {2},
                sequences,
                new HashSet<>(Arrays.asList(0, 1)),
                allReadersNoMoreSplits,
                4L);
    }
}
