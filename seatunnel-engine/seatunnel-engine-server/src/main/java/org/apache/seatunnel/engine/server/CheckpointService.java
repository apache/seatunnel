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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointEntryConsumer;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.ActionState;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.utils.CheckpointRestoreUtils;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The service to manage the checkpoint data.
 *
 * <p>The service provides the APIs to get the latest checkpoint data of a job.
 */
@Slf4j
public class CheckpointService {
    @Getter private CheckpointStorage checkpointStorage;
    private Serializer serializer = new ProtoStuffSerializer();

    /** Dedicated serializer for the type-aware cleanup, which has no service instance at hand. */
    private static final Serializer CLEANUP_SERIALIZER = new ProtoStuffSerializer();

    @SneakyThrows
    public CheckpointService(CheckpointConfig config) {
        this.checkpointStorage =
                FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                CheckpointStorageFactory.class,
                                config.getStorage().getStorage())
                        .create(config.getStorage().getStoragePluginConfig());
    }

    @SneakyThrows
    public List<CompletedCheckpoint> getLatestCheckpoint(String jobId) {
        List<PipelineState> pipelineStates = checkpointStorage.getLatestCheckpoint(jobId);
        return pipelineStates.stream()
                .map(this::deserializeCheckpoint)
                .sorted(Comparator.comparingInt(CompletedCheckpoint::getPipelineId))
                .collect(Collectors.toList());
    }

    /**
     * Get the latest checkpoint data of a job.
     *
     * <p>The checkpoint data contains the state of the job pipeline, including the state of each
     * action and subtask.
     *
     * @param jobId
     * @return
     */
    public List<JobPipelineCheckpointData> getLatestCheckpointData(String jobId) {
        return toJobPipelineCheckpointData(getLatestCheckpoint(jobId));
    }

    /**
     * Get the latest checkpoint data of a job after filtering by restore mode.
     *
     * @param jobId job id
     * @param restoreMode restore mode used to filter eligible checkpoint types
     * @return latest restore-eligible checkpoint data for each pipeline, ordered by pipeline id
     */
    public List<JobPipelineCheckpointData> getLatestCheckpointData(
            String jobId, RestoreMode restoreMode) {
        return toJobPipelineCheckpointData(getLatestCheckpointsForRestore(jobId, restoreMode));
    }

    /**
     * Returns the latest restore-eligible checkpoint for each pipeline of a job.
     *
     * <p>Selection rules:
     *
     * <ul>
     *   <li>filter checkpoints by {@link CheckpointRestoreUtils#matchesRestoreCheckpointType}
     *   <li>group by pipeline id
     *   <li>select the latest checkpoint within each pipeline by checkpoint id, then completed
     *       timestamp as tie-breaker
     *   <li>sort the final result by pipeline id
     * </ul>
     */
    @SneakyThrows
    public List<CompletedCheckpoint> getLatestCheckpointsForRestore(
            String jobId, RestoreMode restoreMode) {
        return checkpointStorage.getAllCheckpoints(jobId).stream()
                .map(this::deserializeCheckpoint)
                .filter(
                        checkpoint ->
                                CheckpointRestoreUtils.matchesRestoreCheckpointType(
                                        checkpoint.getCheckpointType(), restoreMode))
                .collect(Collectors.groupingBy(CompletedCheckpoint::getPipelineId))
                .values()
                .stream()
                .map(
                        checkpoints ->
                                checkpoints.stream()
                                        .max(
                                                Comparator.comparingLong(
                                                                CompletedCheckpoint
                                                                        ::getCheckpointId)
                                                        .thenComparingLong(
                                                                CompletedCheckpoint
                                                                        ::getCompletedTimestamp)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted(Comparator.comparingInt(CompletedCheckpoint::getPipelineId))
                .collect(Collectors.toList());
    }

    /**
     * Deletes the automatic (periodic) checkpoint data of a job while keeping the savepoints that
     * are stored under the same job id.
     *
     * <p>Savepoints share the per-job checkpoint directory with periodic checkpoints, and a
     * savepoint restore keeps the source job id. A job that was restored from a savepoint therefore
     * writes its periodic checkpoints next to the savepoint it was started from, so the
     * terminal-state cleanup must not delete the job directory blindly: a savepoint is
     * operator-managed recovery state that stays restorable until it is deleted explicitly (see
     * {@code docs/en/engines/zeta/state-storage-and-recovery.md}).
     *
     * <p>The cleanup only ever deletes the entries it positively classified as automatic
     * checkpoints, one pipeline at a time. It never deletes the job directory as a whole, because a
     * storage listing is not an inventory of that directory: an entry that cannot be read or
     * decoded is skipped by {@link CheckpointStorage#forEachCheckpoint(String,
     * CheckpointEntryConsumer)} while it stays on disk, so a "missing" entry may still be an
     * operator-managed savepoint and has to survive. Unreadable or otherwise unknown entries are
     * therefore kept, which can leave a job directory behind that still holds them.
     *
     * <p>The checkpoint type is not part of {@link PipelineState}; it is only inside the serialized
     * {@link CompletedCheckpoint}, so every entry has to be read to classify it. The entries are
     * visited one at a time and only the ids of the classified entries are retained, so a job that
     * accumulated a long savepoint history across repeated restores is cleaned without loading its
     * payloads at once.
     *
     * @param checkpointStorage storage that holds the job's checkpoint data
     * @param jobId job id whose automatic checkpoint data should be deleted
     */
    public static void deleteCheckpointDataKeepingSavepoints(
            CheckpointStorage checkpointStorage, String jobId) {
        Map<Integer, List<String>> automaticCheckpointIds = new HashMap<>();
        List<Long> keptSavepointIds = new ArrayList<>();
        try {
            checkpointStorage.forEachCheckpoint(
                    jobId,
                    pipelineState -> {
                        CheckpointType checkpointType = readCheckpointType(pipelineState);
                        if (!checkpointType.isAuto()) {
                            keptSavepointIds.add(pipelineState.getCheckpointId());
                            return;
                        }
                        automaticCheckpointIds
                                .computeIfAbsent(
                                        pipelineState.getPipelineId(), id -> new ArrayList<>())
                                .add(String.valueOf(pipelineState.getCheckpointId()));
                    });
        } catch (Exception e) {
            log.warn(
                    "Failed to classify the stored checkpoints of job {}, keeping all checkpoint data: {}",
                    jobId,
                    e.getMessage());
            return;
        }
        if (automaticCheckpointIds.isEmpty()) {
            return;
        }

        if (!keptSavepointIds.isEmpty()) {
            log.info(
                    "Job {} still has savepoints {} in checkpoint storage, deleting only its periodic checkpoints",
                    jobId,
                    keptSavepointIds);
        }
        for (Map.Entry<Integer, List<String>> entry : automaticCheckpointIds.entrySet()) {
            try {
                checkpointStorage.deleteCheckpoint(
                        jobId, String.valueOf(entry.getKey()), entry.getValue());
            } catch (Exception e) {
                // The cleanup is best effort: a periodic checkpoint left behind is harmless, while
                // aborting the whole cleanup would keep the job's data around indefinitely.
                log.warn(
                        "Failed to delete the periodic checkpoints {} of job {} pipeline {}: {}",
                        entry.getValue(),
                        jobId,
                        entry.getKey(),
                        e.getMessage());
            }
        }
    }

    /**
     * Reads the persisted type of one stored checkpoint.
     *
     * @param pipelineState stored checkpoint entry
     * @return the stored checkpoint type, never null
     * @throws CheckpointStorageException if the entry cannot be decoded or carries no type, which
     *     aborts the cleanup without deleting anything
     */
    private static CheckpointType readCheckpointType(PipelineState pipelineState)
            throws CheckpointStorageException {
        CompletedCheckpoint checkpoint;
        try {
            checkpoint =
                    CLEANUP_SERIALIZER.deserialize(
                            pipelineState.getStates(), CompletedCheckpoint.class);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Failed to deserialize checkpoint %d of pipeline %d",
                            pipelineState.getCheckpointId(), pipelineState.getPipelineId()),
                    e);
        }
        CheckpointType checkpointType = checkpoint == null ? null : checkpoint.getCheckpointType();
        if (checkpointType == null) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint %d of pipeline %d carries no checkpoint type",
                            pipelineState.getCheckpointId(), pipelineState.getPipelineId()));
        }
        return checkpointType;
    }

    private CompletedCheckpoint deserializeCheckpoint(PipelineState pipelineState) {
        try {
            return serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<JobPipelineCheckpointData> toJobPipelineCheckpointData(
            List<CompletedCheckpoint> checkpoints) {
        return checkpoints.stream()
                .map(
                        checkpoint -> {
                            Map<String, JobPipelineCheckpointData.ActionState> taskStates =
                                    new HashMap<>();
                            for (ActionStateKey stateKey : checkpoint.getTaskStates().keySet()) {
                                ActionState taskState = checkpoint.getTaskStates().get(stateKey);
                                List<JobPipelineCheckpointData.ActionSubtaskState> subtaskStates =
                                        taskState.getSubtaskStates().stream()
                                                .map(
                                                        state -> {
                                                            if (state == null) {
                                                                return null;
                                                            }
                                                            return new JobPipelineCheckpointData
                                                                    .ActionSubtaskState(
                                                                    state.getIndex(),
                                                                    state.getState());
                                                        })
                                                .collect(Collectors.toList());
                                ActionSubtaskState coordinatorState =
                                        taskState.getCoordinatorState();
                                JobPipelineCheckpointData.ActionState actionState =
                                        new JobPipelineCheckpointData.ActionState(
                                                coordinatorState == null
                                                        ? null
                                                        : coordinatorState.getState(),
                                                subtaskStates);
                                taskStates.put(stateKey.getName(), actionState);
                            }
                            return JobPipelineCheckpointData.builder()
                                    .jobId(checkpoint.getJobId())
                                    .pipelineId(checkpoint.getPipelineId())
                                    .checkpointId(checkpoint.getCheckpointId())
                                    .checkpointType(checkpoint.getCheckpointType())
                                    .triggerTimestamp(checkpoint.getCheckpointTimestamp())
                                    .completedTimestamp(checkpoint.getCompletedTimestamp())
                                    .taskStates(taskStates)
                                    .build();
                        })
                .collect(Collectors.toList());
    }
}
