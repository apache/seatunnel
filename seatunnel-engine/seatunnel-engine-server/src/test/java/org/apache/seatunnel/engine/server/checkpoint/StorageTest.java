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
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class StorageTest {

    @Test
    public void localFileTest() throws IOException {

        Map<Long, TaskStatistics> taskStatisticsMap = new HashMap<>();
        taskStatisticsMap.put(1L, new TaskStatistics(1L, 32));
        Map<Long, ActionState> actionStateMap = new HashMap<>();
        actionStateMap.put(2L, new ActionState("test", 13));
        CompletedCheckpoint completedCheckpoint = new CompletedCheckpoint(1, 2, 4324,
            Instant.now().toEpochMilli(),
            CheckpointType.COMPLETED_POINT_TYPE,
            Instant.now().toEpochMilli(),
            actionStateMap,
            taskStatisticsMap);

        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        byte[] data = protoStuffSerializer.serialize(completedCheckpoint);
        PipelineState pipelineState = PipelineState.builder()
            .checkpointId(1)
            .jobId(String.valueOf(1))
            .pipelineId(1)
            .states(data)
            .build();

        byte[] pipeData = protoStuffSerializer.serialize(pipelineState);

        File file = new File("/tmp/seatunnel/test.data");

        FileUtils.writeByteArrayToFile(file, pipeData);

        byte[] fileData = FileUtils.readFileToByteArray(file);

        PipelineState state = protoStuffSerializer.deserialize(fileData, PipelineState.class);

        CompletedCheckpoint checkpoint = new ProtoStuffSerializer().deserialize(state.getStates(), CompletedCheckpoint.class);
        Assertions.assertNotNull(checkpoint);
    }

}
