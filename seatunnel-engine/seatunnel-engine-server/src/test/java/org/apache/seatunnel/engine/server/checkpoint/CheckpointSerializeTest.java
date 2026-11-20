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

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CheckpointSerializeTest {

    private static final String V1_FIXTURE = "/checkpoint/v1-completed-checkpoint.ser";

    /**
     * The fixture is a v1 checkpoint file written by the code base before STIP-45. It must stay
     * restorable, and serializing the same (inline) checkpoint today must yield the very same
     * bytes, i.e. the v1 on-disk format is unchanged.
     */
    @Test
    public void testV1CheckpointIsRestorableAndByteIdentical() throws IOException {
        byte[] fixture = readResource(V1_FIXTURE);
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();

        PipelineState pipelineState = serializer.deserialize(fixture, PipelineState.class);
        CompletedCheckpoint restored =
                serializer.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);

        Assertions.assertEquals(7L, restored.getCheckpointId());
        ActionState actionState =
                restored.getTaskStates().get(new ActionStateKey("ActionStateKey - fixture-source"));
        Assertions.assertTrue(actionState.getCoordinatorState().isInline());
        Assertions.assertArrayEquals(
                bytes("enumerator-state"), actionState.getCoordinatorState().getState().get(0));
        ActionSubtaskState subtask0 = actionState.getSubtaskStates().get(0);
        Assertions.assertTrue(subtask0.isInline());
        Assertions.assertNull(subtask0.getExternalStateHandle());
        Assertions.assertArrayEquals(bytes("split-0b"), subtask0.getState().get(1));
        Assertions.assertEquals(16, subtask0.getStateSize());
        Assertions.assertNull(actionState.getSubtaskStates().get(1).getState().get(1));
        Assertions.assertTrue(actionState.getSubtaskStates().get(2).getState().isEmpty());

        PipelineState reserialized =
                PipelineState.builder()
                        .jobId("100")
                        .pipelineId(1)
                        .checkpointId(7L)
                        .states(serializer.serialize(buildV1FixtureCheckpoint()))
                        .build();
        Assertions.assertArrayEquals(fixture, serializer.serialize(reserialized));
    }

    @Test
    public void testExternalStateHandleSurvivesCheckpointAndAckSerialization() throws Exception {
        ActionStateKey key = new ActionStateKey("ActionStateKey - external");
        FileChunkStateHandle handle =
                new FileChunkStateHandle("100/1/chk-8/subtask-0011223344556677-0.chunk", 4096);
        ActionSubtaskState external = ActionSubtaskState.ofExternal(key, 0, handle);
        ActionState actionState = new ActionState(key, 1);
        actionState.reportState(0, external);
        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(key, actionState);
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        100L,
                        1,
                        8L,
                        1L,
                        CheckpointType.CHECKPOINT_TYPE,
                        2L,
                        taskStates,
                        new HashMap<>());

        // _metadata payload path: ProtoStuff
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        ActionSubtaskState fromMetadata =
                serializer
                        .deserialize(serializer.serialize(checkpoint), CompletedCheckpoint.class)
                        .getTaskStates()
                        .get(key)
                        .getSubtaskStates()
                        .get(0);
        Assertions.assertFalse(fromMetadata.isInline());
        Assertions.assertNull(fromMetadata.getState());
        Assertions.assertEquals(handle, fromMetadata.getExternalStateHandle());
        Assertions.assertEquals(4096, fromMetadata.getStateSize());

        // acknowledgement path: Java serialization of the state list
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new ArrayList<>(Arrays.asList(external)));
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            @SuppressWarnings("unchecked")
            List<ActionSubtaskState> acked = (List<ActionSubtaskState>) in.readObject();
            Assertions.assertEquals(external, acked.get(0));
        }
    }

    @Test
    public void testToInlineReadsExternalStateFromChunk(@TempDir Path namespace) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("namespace", namespace.toString());
        LocalFileStorage storage = new LocalFileStorage(config);
        ActionStateKey key = new ActionStateKey("ActionStateKey - enumerator");
        FileChunkStateHandle handle =
                storage.writeChunk(
                        new ChunkDescriptor("100", "1", 9, key.getName(), -1),
                        Arrays.asList(bytes("assigned-splits"), null));

        ActionSubtaskState resolved =
                ActionSubtaskState.ofExternal(key, -1, handle).toInline(() -> storage);

        Assertions.assertTrue(resolved.isInline());
        Assertions.assertEquals(-1, resolved.getIndex());
        Assertions.assertArrayEquals(bytes("assigned-splits"), resolved.getState().get(0));
        Assertions.assertNull(resolved.getState().get(1));

        ActionSubtaskState inline =
                new ActionSubtaskState(key, 0, Collections.singletonList(bytes("x")));
        Assertions.assertSame(
                inline,
                inline.toInline(
                        () -> {
                            throw new AssertionError("inline state must not touch the storage");
                        }));
    }

    private static byte[] readResource(String name) throws IOException {
        try (InputStream in = CheckpointSerializeTest.class.getResourceAsStream(name)) {
            Assertions.assertNotNull(in, "missing test resource " + name);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    /**
     * Builds the deterministic checkpoint that the {@code v1-completed-checkpoint.ser} fixture was
     * generated from (with the pre-STIP-45 code base, where subtask state was only {@code
     * List<byte[]>}).
     */
    static CompletedCheckpoint buildV1FixtureCheckpoint() {
        ActionStateKey key = new ActionStateKey("ActionStateKey - fixture-source");
        ActionState actionState = new ActionState(key, 3);
        actionState.reportState(
                CheckpointPlan.COORDINATOR_INDEX,
                new ActionSubtaskState(
                        key,
                        CheckpointPlan.COORDINATOR_INDEX,
                        Collections.singletonList(bytes("enumerator-state"))));
        actionState.reportState(
                0,
                new ActionSubtaskState(
                        key, 0, Arrays.asList(bytes("split-0a"), bytes("split-0b"))));
        actionState.reportState(
                1, new ActionSubtaskState(key, 1, Arrays.asList(bytes("split-1"), null)));
        actionState.reportState(2, new ActionSubtaskState(key, 2, Collections.emptyList()));
        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(key, actionState);
        return new CompletedCheckpoint(
                100L,
                1,
                7L,
                1700000000000L,
                CheckpointType.CHECKPOINT_TYPE,
                1700000001000L,
                taskStates,
                new HashMap<>());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @Disabled
    public void testPipelineStateDeserialize() throws IOException {
        File file =
                new File(
                        "/private/tmp/seatunnel/checkpoint_snapshot/679984510862884865/1676885754364-316-1-2.ser");
        FileInputStream fileInputStream = null;
        byte[] bFile = new byte[(int) file.length()];
        // convert file into array of bytes
        fileInputStream = new FileInputStream(file);
        fileInputStream.read(bFile);
        fileInputStream.close();
        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        PipelineState pipelineState = protoStuffSerializer.deserialize(bFile, PipelineState.class);
        CompletedCheckpoint latestCompletedCheckpoint =
                protoStuffSerializer.deserialize(
                        pipelineState.getStates(), CompletedCheckpoint.class);
        ActionState actionState = latestCompletedCheckpoint.getTaskStates().get(1L);
        List<ActionSubtaskState> subtaskStates = actionState.getSubtaskStates();
        List<byte[]> coordinatorBytes = actionState.getCoordinatorState().getState();
        DefaultSerializer<FakeSourceState> fakeSourceSerializer =
                new DefaultSerializer<FakeSourceState>();
        FakeSourceState fakeSourceState = fakeSourceSerializer.deserialize(coordinatorBytes.get(0));

        for (ActionSubtaskState state : subtaskStates) {
            List<byte[]> bList = state.getState();
            for (int i = 0; i < bList.size(); i++) {
                byte[] bytes = bList.get(i);
                DefaultSerializer<FakeSourceSplit> defaultSerializer =
                        new DefaultSerializer<FakeSourceSplit>();
                FakeSourceSplit split = defaultSerializer.deserialize(bytes);
                log.info(String.valueOf(split.getSplitId()));
            }
        }

        actionState = latestCompletedCheckpoint.getTaskStates().get(2L);
        List<byte[]> sinkCommitStateSeri = actionState.getCoordinatorState().getState();
        DefaultSerializer<FileAggregatedCommitInfo> fileSinkStateDefaultSerializer =
                new DefaultSerializer<FileAggregatedCommitInfo>();
        FileAggregatedCommitInfo fileAggregatedCommitInfo =
                fileSinkStateDefaultSerializer.deserialize(sinkCommitStateSeri.get(0));
        subtaskStates = actionState.getSubtaskStates();
        for (ActionSubtaskState state : subtaskStates) {
            List<byte[]> bList = state.getState();
            for (int i = 0; i < bList.size(); i++) {
                byte[] bytes = bList.get(i);
                DefaultSerializer<FileSinkState> defaultSerializer =
                        new DefaultSerializer<FileSinkState>();
                FileSinkState fileSinkState = defaultSerializer.deserialize(bytes);
                log.info(fileSinkState.getTransactionDir());
            }
        }
    }
}
