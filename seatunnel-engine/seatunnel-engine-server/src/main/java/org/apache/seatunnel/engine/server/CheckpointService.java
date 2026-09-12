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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
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

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
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
public class CheckpointService {
    @Getter private CheckpointStorage checkpointStorage;
    private Serializer serializer = new ProtoStuffSerializer();

    @SneakyThrows
    public CheckpointService(CheckpointConfig config) {
        this(config, Common.appStarterDir().resolve("zeta"));
    }

    /**
     * Creates the checkpoint service with an explicit Zeta starter directory for isolated
     * classloader verification.
     */
    @SneakyThrows
    @VisibleForTesting
    CheckpointService(CheckpointConfig config, Path zetaDirectory) {
        ClassLoader storageClassLoader = Thread.currentThread().getContextClassLoader();
        List<URL> storageJars =
                FileUtils.searchJarFilesForStorage(
                        zetaDirectory,
                        config.getStorage().getStoragePluginConfig().get("storage.type"));
        if (!storageJars.isEmpty()) {
            storageClassLoader =
                    new URLClassLoader(storageJars.toArray(new URL[0]), storageClassLoader);
        }

        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(storageClassLoader)) {
            this.checkpointStorage =
                    FactoryUtil.discoverFactory(
                                    Thread.currentThread().getContextClassLoader(),
                                    CheckpointStorageFactory.class,
                                    config.getStorage().getStorage())
                            .create(config.getStorage().getStoragePluginConfig());
        }
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
