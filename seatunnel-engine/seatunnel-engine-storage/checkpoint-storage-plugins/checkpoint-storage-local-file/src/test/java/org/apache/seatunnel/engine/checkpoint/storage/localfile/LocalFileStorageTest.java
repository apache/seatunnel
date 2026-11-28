/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class LocalFileStorageTest {

    private static LocalFileStorage STORAGE = new LocalFileStorage(null);
    private static final String JOB_ID = "chris";

    private static final String STREAMING_JOB_ID = "streaming";

    @TempDir Path tempDir;

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        PipelineState pipelineState =
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(1)
                        .checkpointId(1)
                        .states(new byte[0])
                        .build();
        STORAGE.storeCheckPoint(pipelineState);
        pipelineState.setCheckpointId(2);
        STORAGE.storeCheckPoint(pipelineState);
        pipelineState.setPipelineId(2);
        pipelineState.setCheckpointId(3);
        STORAGE.storeCheckPoint(pipelineState);
    }

    @Test
    public void testGetAllCheckpoints() throws CheckpointStorageException {

        List<PipelineState> pipelineStates = STORAGE.getAllCheckpoints(JOB_ID);
        Assertions.assertEquals(3, pipelineStates.size());
    }

    @Test
    public void testGetLatestCheckpoints() throws CheckpointStorageException {
        List<PipelineState> pipelineStates = STORAGE.getLatestCheckpoint(JOB_ID);
        Assertions.assertEquals(2, pipelineStates.size());
    }

    @Test
    public void testGetLatestCheckpointByJobIdAndPipelineId() throws CheckpointStorageException {
        PipelineState state = STORAGE.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, "1");
        Assertions.assertEquals(2, state.getCheckpointId());
    }

    @Test
    public void testGetCheckpointsByJobIdAndPipelineId() throws CheckpointStorageException {
        List<PipelineState> state = STORAGE.getCheckpointsByJobIdAndPipelineId(JOB_ID, "1");
        Assertions.assertEquals(2, state.size());
    }

    /**
     * A terminal cleanup can face a long saved history, so the visitor must decode an entry only
     * when it reaches it instead of loading and decoding the whole history up front. Decoding the
     * history in one batch is exactly the memory amplification the visitor exists to prevent.
     */
    @Test
    public void testForEachCheckpointDecodesOneEntryAtATime() throws Exception {
        AtomicInteger decodedEntries = new AtomicInteger();
        LocalFileStorage streamingStorage =
                new LocalFileStorage(
                        Collections.singletonMap(
                                StorageConstants.STORAGE_NAME_SPACE, tempDir.toString())) {
                    @Override
                    public PipelineState deserializeCheckPointData(byte[] data) throws IOException {
                        decodedEntries.incrementAndGet();
                        return super.deserializeCheckPointData(data);
                    }
                };
        for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
            streamingStorage.storeCheckPoint(
                    PipelineState.builder()
                            .jobId(STREAMING_JOB_ID)
                            .pipelineId(1)
                            .checkpointId(checkpointId)
                            .states(new byte[0])
                            .build());
        }

        List<Integer> decodedBeforeConsumption = new ArrayList<>();
        streamingStorage.forEachCheckpoint(
                STREAMING_JOB_ID,
                pipelineState -> decodedBeforeConsumption.add(decodedEntries.get()));

        Assertions.assertEquals(
                Arrays.asList(1, 2, 3),
                decodedBeforeConsumption,
                "each entry must be decoded right before the consumer sees it, not as a batch");
        Assertions.assertEquals(3, decodedEntries.get());
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }
}
