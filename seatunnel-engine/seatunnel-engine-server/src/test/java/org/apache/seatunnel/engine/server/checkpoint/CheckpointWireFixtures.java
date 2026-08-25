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

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared sample model used by {@link CheckpointWireCompatibilityTest}.
 *
 * <p>The sample data is the stable anchor for the committed {@code legacy-v0} wire fixtures. The
 * samples intentionally cover the full persisted object graph: {@link CompletedCheckpoint}, {@link
 * ActionState}, {@link ActionSubtaskState}, {@link ActionStateKey}, {@link TaskStatistics}, {@link
 * SubtaskStatistics} ({@link SubtaskStatus}), plus null slots (unreported subtasks) and empty
 * collections.
 */
public final class CheckpointWireFixtures {

    public static final long JOB_ID = 733584788375093248L;
    public static final int PIPELINE_ID = 0;
    public static final long CHECKPOINT_ID = 7L;
    public static final long TRIGGER_TIMESTAMP = 1748595600000L;
    public static final long COMPLETED_TIMESTAMP = 1748595600123L;

    private CheckpointWireFixtures() {}

    public static ActionStateKey sampleActionStateKey() {
        return new ActionStateKey("ActionStateKey - fake-source");
    }

    /**
     * Full-graph savepoint sample: one action with parallelism 2 (subtask 0 reported, subtask 1
     * left null, coordinator state reported) and task statistics with one acknowledged subtask.
     */
    public static CompletedCheckpoint sampleCompletedCheckpoint() {
        ActionStateKey key = sampleActionStateKey();
        ActionState actionState = new ActionState(key, 2);
        actionState.reportState(0, new ActionSubtaskState(key, 0, List.of(new byte[] {1, 2, 3})));
        // subtask 1 intentionally not reported -> null slot in subtaskStates
        actionState.reportState(-1, new ActionSubtaskState(key, -1, List.of(new byte[] {9, 8, 7})));

        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(key, actionState);

        TaskStatistics statistics = new TaskStatistics(0L, 2);
        statistics.reportSubtaskStatistics(
                new SubtaskStatistics(0, 1000L, 42L, SubtaskStatus.RUNNING));
        statistics.completed(0);
        Map<Long, TaskStatistics> taskStatistics = new HashMap<>();
        taskStatistics.put(0L, statistics);

        return new CompletedCheckpoint(
                JOB_ID,
                PIPELINE_ID,
                CHECKPOINT_ID,
                TRIGGER_TIMESTAMP,
                CheckpointType.SAVEPOINT_TYPE,
                COMPLETED_TIMESTAMP,
                taskStates,
                taskStatistics);
    }

    /**
     * Edge-case sample: empty state collections, {@link CheckpointType#COMPLETED_POINT_TYPE}, and
     * the runtime-only {@code isRestored} flag set to {@code true}.
     */
    public static CompletedCheckpoint sampleEmptyCompletedCheckpoint() {
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        JOB_ID,
                        PIPELINE_ID,
                        8L,
                        TRIGGER_TIMESTAMP,
                        CheckpointType.COMPLETED_POINT_TYPE,
                        COMPLETED_TIMESTAMP,
                        new HashMap<>(),
                        new HashMap<>());
        checkpoint.setRestored(true);
        return checkpoint;
    }

    /**
     * The storage envelope written to disk: {@link PipelineState} wrapping the checkpoint payload.
     */
    public static PipelineState samplePipelineState() {
        return PipelineState.builder()
                .jobId(String.valueOf(JOB_ID))
                .pipelineId(PIPELINE_ID)
                .checkpointId(CHECKPOINT_ID)
                .states(new ProtoStuffSerializer().serialize(sampleCompletedCheckpoint()))
                .build();
    }
}
