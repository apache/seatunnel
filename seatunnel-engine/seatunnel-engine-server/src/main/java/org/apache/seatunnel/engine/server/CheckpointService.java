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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.ActionState;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CheckpointService {
    private CheckpointStorage checkpointStorage;
    private Serializer serializer = new ProtoStuffSerializer();

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
                .map(
                        pipelineState -> {
                            try {
                                return serializer.deserialize(
                                        pipelineState.getStates(), CompletedCheckpoint.class);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .sorted(Comparator.comparingInt(CompletedCheckpoint::getPipelineId))
                .collect(Collectors.toList());
    }

    public List<JobPipelineCheckpointData> getLatestCheckpointData(String jobId) {
        return getLatestCheckpoint(jobId).stream()
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
