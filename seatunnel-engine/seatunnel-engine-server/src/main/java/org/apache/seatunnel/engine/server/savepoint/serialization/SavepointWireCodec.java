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

package org.apache.seatunnel.engine.server.savepoint.serialization;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.ActionState;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.SubtaskStatistics;
import org.apache.seatunnel.engine.server.checkpoint.SubtaskStatus;
import org.apache.seatunnel.engine.server.checkpoint.TaskStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Codec between the {@code engine-wire-v1} DTOs and the runtime checkpoint model.
 *
 * <p>Only this class (plus the frozen DTOs) is allowed to touch the savepoint payload format. The
 * runtime {@link CompletedCheckpoint} is explicitly NOT a storage contract: conversion always goes
 * through {@link WireSavepoint}. The version of this payload family is anchored in the bundle
 * {@link SavepointMeta#getFormatVersion()} ({@link SavepointStorageConstants#FORMAT_VERSION}) - its
 * manifest entries carry {@link SavepointStorageConstants#PAYLOAD_FORMAT_V1}.
 */
public final class SavepointWireCodec {

    private static final ProtoStuffSerializer SERIALIZER = new ProtoStuffSerializer();

    private SavepointWireCodec() {}

    public static byte[] encode(WireSavepoint checkpoint) {
        return SERIALIZER.serialize(checkpoint);
    }

    public static WireSavepoint decode(byte[] data) {
        return SERIALIZER.deserialize(data, WireSavepoint.class);
    }

    /** Converts runtime checkpoint state into the wire DTO (drops runtime-only fields). */
    public static WireSavepoint fromCompletedCheckpoint(CompletedCheckpoint checkpoint) {
        Map<String, WireActionState> taskStates = new HashMap<>();
        checkpoint
                .getTaskStates()
                .forEach((key, state) -> taskStates.put(key.getName(), fromActionState(state)));

        Map<Long, WireTaskStatistics> taskStatistics = new HashMap<>();
        checkpoint
                .getTaskStatistics()
                .forEach((key, stats) -> taskStatistics.put(key, fromTaskStatistics(stats)));

        return new WireSavepoint(
                checkpoint.getCheckpointId(),
                checkpoint.getPipelineId(),
                checkpoint.getJobId(),
                checkpoint.getCheckpointTimestamp(),
                checkpoint.getCheckpointType().getName(),
                checkpoint.getCompletedTimestamp(),
                taskStates,
                taskStatistics);
    }

    /** Converts a wire DTO back into the runtime checkpoint model. */
    public static CompletedCheckpoint toCompletedCheckpoint(WireSavepoint wire) {
        CheckpointType checkpointType;
        try {
            checkpointType = CheckpointType.fromName(wire.getCheckpointTypeName());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown checkpoint type in savepoint payload: "
                            + wire.getCheckpointTypeName()
                            + ". Supported: "
                            + supportedCheckpointTypeNames(),
                    e);
        }

        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        wire.getTaskStates()
                .forEach(
                        (name, state) ->
                                taskStates.put(new ActionStateKey(name), toActionState(state)));

        Map<Long, TaskStatistics> taskStatistics = new HashMap<>();
        wire.getTaskStatistics()
                .forEach((key, stats) -> taskStatistics.put(key, toTaskStatistics(stats)));

        return new CompletedCheckpoint(
                wire.getJobId(),
                wire.getPipelineId(),
                wire.getCheckpointId(),
                wire.getTriggerTimestamp(),
                checkpointType,
                wire.getCompletedTimestamp(),
                taskStates,
                taskStatistics);
    }

    private static WireActionState fromActionState(ActionState state) {
        List<WireSubtaskState> subtaskStates = new ArrayList<>();
        state.getSubtaskStates()
                .forEach(sub -> subtaskStates.add(sub == null ? null : fromSubtaskState(sub)));
        return new WireActionState(
                state.getStateKey().getName(),
                subtaskStates,
                state.getCoordinatorState() == null
                        ? null
                        : fromSubtaskState(state.getCoordinatorState()),
                state.getParallelism());
    }

    private static WireSubtaskState fromSubtaskState(ActionSubtaskState state) {
        return new WireSubtaskState(state.getIndex(), state.getState());
    }

    private static WireTaskStatistics fromTaskStatistics(TaskStatistics stats) {
        List<WireSubtaskStatistics> subtaskStats = new ArrayList<>();
        stats.getSubtaskStats()
                .forEach(sub -> subtaskStats.add(sub == null ? null : fromSubtaskStatistics(sub)));
        return new WireTaskStatistics(
                stats.getJobVertexId(),
                subtaskStats,
                stats.getSubtaskCompleted(),
                stats.getNumAcknowledgedSubtasks(),
                stats.getLatestAcknowledgedSubtaskStatistics() == null
                        ? null
                        : fromSubtaskStatistics(stats.getLatestAcknowledgedSubtaskStatistics()));
    }

    private static WireSubtaskStatistics fromSubtaskStatistics(SubtaskStatistics stats) {
        return new WireSubtaskStatistics(
                stats.getSubtaskIndex(),
                stats.getAckTimestamp(),
                stats.getStateSize(),
                stats.getSubtaskStatus().name());
    }

    private static ActionState toActionState(WireActionState wire) {
        ActionState state =
                new ActionState(new ActionStateKey(wire.getStateKeyName()), wire.getParallelism());
        if (wire.getSubtaskStates() != null) {
            for (int i = 0; i < wire.getSubtaskStates().size(); i++) {
                WireSubtaskState sub = wire.getSubtaskStates().get(i);
                if (sub != null) {
                    // The list position is authoritative for the subtask slot.
                    state.reportState(
                            i, new ActionSubtaskState(state.getStateKey(), i, sub.getState()));
                }
            }
        }
        if (wire.getCoordinatorState() != null) {
            WireSubtaskState coordinator = wire.getCoordinatorState();
            state.reportState(
                    -1, new ActionSubtaskState(state.getStateKey(), -1, coordinator.getState()));
        }
        return state;
    }

    private static TaskStatistics toTaskStatistics(WireTaskStatistics wire) {
        List<WireSubtaskStatistics> subs =
                wire.getSubtaskStats() == null
                        ? java.util.Collections.emptyList()
                        : wire.getSubtaskStats();
        TaskStatistics stats = new TaskStatistics(wire.getJobVertexId(), subs.size());
        for (int i = 0; i < subs.size(); i++) {
            WireSubtaskStatistics sub = subs.get(i);
            if (sub != null) {
                stats.reportSubtaskStatistics(toSubtaskStatistics(sub));
            }
        }
        if (wire.getSubtaskCompleted() != null) {
            for (int i = 0; i < wire.getSubtaskCompleted().length; i++) {
                if (wire.getSubtaskCompleted()[i]) {
                    stats.completed(i);
                }
            }
        }
        return stats;
    }

    private static SubtaskStatistics toSubtaskStatistics(WireSubtaskStatistics wire) {
        SubtaskStatus status;
        try {
            status = SubtaskStatus.valueOf(wire.getSubtaskStatusName());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown subtask status in savepoint payload: " + wire.getSubtaskStatusName(),
                    e);
        }
        return new SubtaskStatistics(
                wire.getSubtaskIndex(), wire.getAckTimestamp(), wire.getStateSize(), status);
    }

    private static String supportedCheckpointTypeNames() {
        StringBuilder names = new StringBuilder();
        for (CheckpointType type : CheckpointType.values()) {
            if (names.length() > 0) {
                names.append(", ");
            }
            names.append(type.getName());
        }
        return names.toString();
    }
}
