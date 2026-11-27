/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests that a completed checkpoint round-trips through the serialization layers used by checkpoint
 * storage - ProtoStuff for the {@link PipelineState} envelope and the {@link CompletedCheckpoint}
 * nested in it, and the JDK serialization wrapped by {@link DefaultSerializer} for the connector
 * state bytes nested in {@link ActionSubtaskState} - without losing information.
 */
public class CheckpointSerializeTest {

    @Test
    public void testCheckpointStateRoundTrip(@TempDir Path snapshotDir) throws IOException {
        DefaultSerializer<FakeSourceState> sourceStateSerializer = new DefaultSerializer<>();
        DefaultSerializer<FakeSourceSplit> sourceSplitSerializer = new DefaultSerializer<>();
        DefaultSerializer<FileSinkState> sinkStateSerializer = new DefaultSerializer<>();
        DefaultSerializer<FileAggregatedCommitInfo> aggregatedCommitInfoSerializer =
                new DefaultSerializer<>();

        // A source with a split enumerator in the coordinator state and one split per subtask,
        // built exactly like PendingCheckpoint#acknowledgeTask records it.
        ActionStateKey sourceKey = new ActionStateKey("ActionStateKey - fake-source");
        ActionState sourceState = new ActionState(sourceKey, 2);
        Set<FakeSourceSplit> assignedSplits =
                new HashSet<>(
                        Arrays.asList(
                                new FakeSourceSplit("table-1", 0, 10),
                                new FakeSourceSplit("table-2", 1, 20)));
        sourceState.reportState(
                CheckpointPlan.COORDINATOR_INDEX,
                new ActionSubtaskState(
                        sourceKey,
                        CheckpointPlan.COORDINATOR_INDEX,
                        Collections.singletonList(
                                sourceStateSerializer.serialize(
                                        new FakeSourceState(assignedSplits)))));
        for (int index = 0; index < 2; index++) {
            sourceState.reportState(
                    index,
                    new ActionSubtaskState(
                            sourceKey,
                            index,
                            Collections.singletonList(
                                    sourceSplitSerializer.serialize(
                                            new FakeSourceSplit(
                                                    "table-" + (index + 1), index, 10)))));
        }

        // A sink with the aggregated commit info in the coordinator state and the transaction
        // state of one subtask.
        ActionStateKey sinkKey = new ActionStateKey("ActionStateKey - file-sink");
        ActionState sinkState = new ActionState(sinkKey, 2);
        LinkedHashMap<String, String> needMoveFiles = new LinkedHashMap<>();
        needMoveFiles.put("/tmp/seatunnel/uuid-1/final", "/tmp/hive/warehouse/dt=ok");
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
        partitionDirAndValuesMap.put("dt", Collections.singletonList("ok"));
        FileSinkState fileSinkState =
                new FileSinkState(
                        "t-1",
                        "uuid-1",
                        42L,
                        needMoveFiles,
                        partitionDirAndValuesMap,
                        "/tmp/seatunnel/uuid-1");
        LinkedHashMap<String, LinkedHashMap<String, String>> transactionMap = new LinkedHashMap<>();
        LinkedHashMap<String, String> moves = new LinkedHashMap<>();
        moves.put("/tmp/seatunnel/uuid-1/final", "/tmp/hive/warehouse/dt=ok");
        transactionMap.put("/tmp/seatunnel/uuid-1", moves);
        FileAggregatedCommitInfo aggregatedCommitInfo =
                new FileAggregatedCommitInfo(transactionMap, partitionDirAndValuesMap);
        sinkState.reportState(
                CheckpointPlan.COORDINATOR_INDEX,
                new ActionSubtaskState(
                        sinkKey,
                        CheckpointPlan.COORDINATOR_INDEX,
                        Collections.singletonList(
                                aggregatedCommitInfoSerializer.serialize(aggregatedCommitInfo))));
        for (int index = 0; index < 2; index++) {
            sinkState.reportState(
                    index,
                    new ActionSubtaskState(
                            sinkKey,
                            index,
                            Collections.singletonList(
                                    sinkStateSerializer.serialize(fileSinkState))));
        }

        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(sourceKey, sourceState);
        taskStates.put(sinkKey, sinkState);
        Map<Long, TaskStatistics> taskStatistics = new HashMap<>();
        taskStatistics.put(1L, new TaskStatistics(1L, 32));

        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        679984510862884865L,
                        1,
                        1676885754364L,
                        Instant.now().toEpochMilli(),
                        CheckpointType.COMPLETED_POINT_TYPE,
                        Instant.now().toEpochMilli(),
                        taskStates,
                        taskStatistics);

        // Write the snapshot through the same layers checkpoint storage uses, then read it back.
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        PipelineState pipelineState =
                PipelineState.builder()
                        .jobId(String.valueOf(679984510862884865L))
                        .pipelineId(1)
                        .checkpointId(1676885754364L)
                        .states(serializer.serialize(completedCheckpoint))
                        .build();
        Path snapshot = Files.createFile(snapshotDir.resolve("1676885754364-316-1-2.ser"));
        Files.write(snapshot, serializer.serialize(pipelineState));

        PipelineState restoredPipeline =
                serializer.deserialize(Files.readAllBytes(snapshot), PipelineState.class);
        CompletedCheckpoint restoredCheckpoint =
                serializer.deserialize(restoredPipeline.getStates(), CompletedCheckpoint.class);

        Assertions.assertEquals(String.valueOf(679984510862884865L), restoredPipeline.getJobId());
        Assertions.assertEquals(1, restoredPipeline.getPipelineId());
        Assertions.assertEquals(1676885754364L, restoredPipeline.getCheckpointId());
        Assertions.assertEquals(679984510862884865L, restoredCheckpoint.getJobId());
        Assertions.assertEquals(1, restoredCheckpoint.getPipelineId());
        Assertions.assertEquals(1676885754364L, restoredCheckpoint.getCheckpointId());
        Assertions.assertEquals(
                CheckpointType.COMPLETED_POINT_TYPE, restoredCheckpoint.getCheckpointType());

        ActionState restoredSourceState = restoredCheckpoint.getTaskStates().get(sourceKey);
        Assertions.assertNotNull(restoredSourceState);
        Assertions.assertEquals(2, restoredSourceState.getParallelism());
        FakeSourceState restoredSourceCoordinatorState =
                sourceStateSerializer.deserialize(
                        restoredSourceState.getCoordinatorState().getState().get(0));
        Assertions.assertEquals(assignedSplits, restoredSourceCoordinatorState.getAssignedSplits());
        for (int index = 0; index < 2; index++) {
            FakeSourceSplit restoredSplit =
                    sourceSplitSerializer.deserialize(
                            restoredSourceState.getSubtaskStates().get(index).getState().get(0));
            Assertions.assertEquals("table-" + (index + 1) + "_" + index, restoredSplit.splitId());
        }

        ActionState restoredSinkState = restoredCheckpoint.getTaskStates().get(sinkKey);
        Assertions.assertNotNull(restoredSinkState);
        FileAggregatedCommitInfo restoredAggregatedCommitInfo =
                aggregatedCommitInfoSerializer.deserialize(
                        restoredSinkState.getCoordinatorState().getState().get(0));
        Assertions.assertEquals(transactionMap, restoredAggregatedCommitInfo.getTransactionMap());
        for (int index = 0; index < 2; index++) {
            FileSinkState restoredSinkSubtaskState =
                    sinkStateSerializer.deserialize(
                            restoredSinkState.getSubtaskStates().get(index).getState().get(0));
            Assertions.assertEquals("t-1", restoredSinkSubtaskState.getTransactionId());
            Assertions.assertEquals(
                    "/tmp/seatunnel/uuid-1", restoredSinkSubtaskState.getTransactionDir());
            Assertions.assertEquals(needMoveFiles, restoredSinkSubtaskState.getNeedMoveFiles());
        }
    }
}
