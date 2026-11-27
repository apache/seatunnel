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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class AbstractFileCheckPointTest {

    protected static HdfsStorage STORAGE;
    protected static final String JOB_ID = "chris";

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

    @Test
    public void testForEachCheckpointVisitsAllEntries() throws CheckpointStorageException {
        List<PipelineState> visited = new ArrayList<>();
        STORAGE.forEachCheckpoint(JOB_ID, visited::add);
        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("1:1", "1:2", "2:3")),
                visited.stream()
                        .map(state -> state.getPipelineId() + ":" + state.getCheckpointId())
                        .collect(Collectors.toCollection(TreeSet::new)),
                "the bounded visitor must report every stored checkpoint");
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }

    /**
     * init storage data
     *
     * @throws CheckpointStorageException exception if init failed
     */
    protected static void initStorageData() throws CheckpointStorageException {
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
}
