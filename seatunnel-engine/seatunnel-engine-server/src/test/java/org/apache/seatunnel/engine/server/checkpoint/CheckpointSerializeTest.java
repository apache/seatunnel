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
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

@Slf4j
public class CheckpointSerializeTest {

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
