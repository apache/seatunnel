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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointRequest;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

/** HDFS savepoint capability test over the local Hadoop file system (storage.type=local). */
@DisabledOnOs(OS.WINDOWS)
public class HdfsSavepointStorageTest {

    @TempDir Path tempDir;

    private HdfsStorage storage;

    @BeforeEach
    public void setup() throws CheckpointStorageException {
        Map<String, String> config = new HashMap<>();
        config.put(STORAGE_NAME_SPACE, tempDir + "/");
        config.put("storage.type", "local");
        storage = new HdfsStorage(config);
    }

    @AfterEach
    public void cleanup() throws CheckpointStorageException {
        storage.deleteSavepoints("1");
    }

    @Test
    public void testBeginWriteCommitListReadDelete() throws CheckpointStorageException {
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1000", "attempt-1", new HashSet<>(Arrays.asList(0, 1))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1, 2, 3}));
        writer.writePipeline(pipelineState(1, 20, new byte[] {4, 5, 6}));

        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());

        SavepointMeta meta = new SavepointMeta(1, "1000", "1", "test", 2000L, null, null);
        writer.commitSavepoint(meta);

        List<SavepointHandle> handles = storage.listCompletedSavepoints("1");
        Assertions.assertEquals(1, handles.size());
        Assertions.assertEquals("1000", handles.get(0).getSavepointId());
        Assertions.assertEquals(2, handles.get(0).getPipelineCount());

        SavepointData data = storage.readSavepoint("1", "1000");
        Assertions.assertEquals(2, data.getPipelineStates().size());
        Assertions.assertEquals(10L, data.getPipelineStates().get(0).getCheckpointId());

        storage.deleteSavepoint("1", "1000");
        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());
    }

    @Test
    public void testCorruptedPayloadDetectedOnRead() throws Exception {
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1002", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1, 2, 3}));
        writer.commitSavepoint(new SavepointMeta(1, "1002", "1", "test", 3000L, null, null));

        org.apache.hadoop.fs.Path payload =
                new org.apache.hadoop.fs.Path(
                        storage.getStorageParentDirectory() + "/savepoint/1/1002/0-10.ser");
        try (org.apache.hadoop.fs.FSDataOutputStream out = storage.fs.create(payload, true)) {
            out.write(new byte[] {9, 9, 9});
        }

        CheckpointStorageException exception =
                Assertions.assertThrows(
                        CheckpointStorageException.class, () -> storage.readSavepoint("1", "1002"));
        Assertions.assertTrue(exception.getMessage().contains("mismatch"), exception.getMessage());
    }

    private PipelineState pipelineState(int pipelineId, long checkpointId, byte[] states) {
        return PipelineState.builder()
                .jobId("1")
                .pipelineId(pipelineId)
                .checkpointId(checkpointId)
                .states(states)
                .build();
    }
}
