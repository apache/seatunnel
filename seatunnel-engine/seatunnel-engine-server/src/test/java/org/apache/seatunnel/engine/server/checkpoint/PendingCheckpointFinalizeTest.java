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
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/** Tests checkpoint finalization CAS between the complete and abort paths. */
public class PendingCheckpointFinalizeTest {

    @Test
    void testCompletedCheckpointCannotBeAbortedAgain() throws Exception {
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 0);
        PendingCheckpoint pendingCheckpoint = checkpoint(taskLocation);

        pendingCheckpoint.acknowledgeTask(
                taskLocation, Collections.emptyList(), SubtaskStatus.RUNNING);

        CompletedCheckpoint completedCheckpoint =
                pendingCheckpoint.getCompletableFuture().get(1, TimeUnit.SECONDS);
        Assertions.assertNotNull(completedCheckpoint);
        pendingCheckpoint.abortCheckpoint(CheckpointCloseReason.CHECKPOINT_EXPIRED, null);
        Assertions.assertFalse(pendingCheckpoint.getCompletableFuture().isCompletedExceptionally());
    }

    @Test
    void testAbortedCheckpointCannotCompleteAgain() {
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 0);
        PendingCheckpoint pendingCheckpoint = checkpoint(taskLocation);

        pendingCheckpoint.abortCheckpoint(CheckpointCloseReason.CHECKPOINT_EXPIRED, null);
        pendingCheckpoint.acknowledgeTask(
                taskLocation, Collections.emptyList(), SubtaskStatus.RUNNING);

        Assertions.assertTrue(pendingCheckpoint.getCompletableFuture().isCompletedExceptionally());
    }

    @Test
    void testDynamicLookupStateCreatesDurableFactPositionIntent() throws Exception {
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 0);
        ActionStateKey stateKey = new ActionStateKey("ActionStateKey - lookup");
        HashMap<ActionStateKey, ActionState> actionStates = new HashMap<>();
        actionStates.put(stateKey, new ActionState(stateKey, 1));
        PendingCheckpoint pendingCheckpoint = checkpoint(taskLocation, actionStates);
        byte[] dynamicLookupState = ByteBuffer.allocate(Integer.BYTES).putInt(0x44594C4B).array();

        pendingCheckpoint.acknowledgeTask(
                taskLocation,
                Collections.singletonList(
                        new ActionSubtaskState(
                                stateKey, 0, Collections.singletonList(dynamicLookupState))),
                SubtaskStatus.RUNNING);

        CompletedCheckpoint completedCheckpoint =
                pendingCheckpoint.getCompletableFuture().get(1, TimeUnit.SECONDS);
        Assertions.assertEquals(
                CheckpointIntent.PURPOSE_DYNAMIC_LOOKUP_FACT_POSITION_ANCHOR,
                completedCheckpoint.getCheckpointIntent().getCheckpointPurpose());
        Assertions.assertEquals(
                CheckpointIntent.PHASE_FACT_POSITIONS_DURABLE,
                completedCheckpoint.getCheckpointIntent().getTargetDurablePhase());
        Assertions.assertFalse(
                Arrays.equals(
                        new byte[32],
                        completedCheckpoint.getCheckpointIntent().getAnchoredPositionDigest()));
    }

    @Test
    void testSubtaskStateSizeCountsSerializedBytes() {
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 0);
        ActionStateKey stateKey = new ActionStateKey("ActionStateKey - byte-size");
        HashMap<ActionStateKey, ActionState> actionStates = new HashMap<>();
        actionStates.put(stateKey, new ActionState(stateKey, 1));
        PendingCheckpoint pendingCheckpoint = checkpoint(taskLocation, actionStates);

        pendingCheckpoint.acknowledgeTask(
                taskLocation,
                Collections.singletonList(
                        new ActionSubtaskState(
                                stateKey,
                                0,
                                Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5}))),
                SubtaskStatus.RUNNING);

        TaskStatistics statistics =
                pendingCheckpoint.getTaskStatistics().get(taskLocation.getTaskVertexId());
        Assertions.assertEquals(
                5L, statistics.getLatestAcknowledgedSubtaskStatistics().getStateSize());
    }

    private static PendingCheckpoint checkpoint(TaskLocation taskLocation) {
        return checkpoint(taskLocation, new HashMap<>());
    }

    private static PendingCheckpoint checkpoint(
            TaskLocation taskLocation, HashMap<ActionStateKey, ActionState> actionStates) {
        HashSet<Long> notYetAcknowledgedTasks = new HashSet<>();
        notYetAcknowledgedTasks.add(taskLocation.getTaskID());
        HashMap<Long, TaskStatistics> taskStatistics = new HashMap<>();
        taskStatistics.put(
                taskLocation.getTaskVertexId(),
                new TaskStatistics(taskLocation.getTaskVertexId(), 1));
        return new PendingCheckpoint(
                1L,
                1,
                1L,
                System.currentTimeMillis(),
                CheckpointType.CHECKPOINT_TYPE,
                notYetAcknowledgedTasks,
                taskStatistics,
                actionStates);
    }
}
