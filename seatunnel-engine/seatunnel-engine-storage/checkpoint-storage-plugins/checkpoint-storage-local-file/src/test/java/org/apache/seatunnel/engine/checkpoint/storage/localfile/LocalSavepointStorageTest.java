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

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointRequest;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageUtils;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

@DisabledOnOs(OS.WINDOWS)
public class LocalSavepointStorageTest {

    @TempDir Path tempDir;

    private LocalFileStorage storage;

    @BeforeEach
    public void setup() {
        Map<String, String> config = new HashMap<>();
        config.put(STORAGE_NAME_SPACE, tempDir + "/");
        storage = new LocalFileStorage(config);
    }

    @AfterEach
    public void cleanup() {
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

        // staging must not be visible before commit
        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());

        SavepointMeta meta = new SavepointMeta(1, "1000", "1", "test", 2000L, null, null);
        writer.commitSavepoint(meta);

        List<SavepointHandle> handles = storage.listCompletedSavepoints("1");
        Assertions.assertEquals(1, handles.size());
        SavepointHandle handle = handles.get(0);
        Assertions.assertEquals("1000", handle.getSavepointId());
        Assertions.assertEquals(1, handle.getFormatVersion());
        Assertions.assertEquals(2000L, handle.getTriggerTimestamp());
        Assertions.assertEquals(2, handle.getPipelineCount());

        SavepointData data = storage.readSavepoint("1", "1000");
        Assertions.assertEquals("1", data.getJobId());
        Assertions.assertEquals("1000", data.getSavepointId());
        Assertions.assertEquals(2, data.getPipelineStates().size());
        Assertions.assertEquals("1", data.getPipelineStates().get(0).getJobId());
        Assertions.assertEquals(10L, data.getPipelineStates().get(0).getCheckpointId());
        Assertions.assertEquals(2, data.getMeta().getPipelines().size());
        Assertions.assertNotNull(data.getMeta().getManifestChecksum());

        storage.deleteSavepoint("1", "1000");
        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());
    }

    @Test
    public void testAbortRemovesStaging() throws CheckpointStorageException {
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1001", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1}));
        writer.abortSavepoint();
        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());
        File jobDir =
                tempDir.resolve(SavepointStorageConstants.SAVEPOINT_ROOT_DIR).resolve("1").toFile();
        File stagingAttempt =
                new File(new File(jobDir, SavepointStorageConstants.STAGING_DIR), "attempt-1");
        Assertions.assertFalse(stagingAttempt.exists());
    }

    @Test
    public void testCorruptedPayloadDetectedOnRead() throws Exception {
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1002", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1, 2, 3}));
        writer.commitSavepoint(new SavepointMeta(1, "1002", "1", "test", 3000L, null, null));

        File payload =
                tempDir.resolve(SavepointStorageConstants.SAVEPOINT_ROOT_DIR)
                        .resolve("1")
                        .resolve("1002")
                        .resolve("0-10.ser")
                        .toFile();
        Files.write(payload.toPath(), new byte[] {9, 9, 9});

        CheckpointStorageException exception =
                Assertions.assertThrows(
                        CheckpointStorageException.class, () -> storage.readSavepoint("1", "1002"));
        Assertions.assertTrue(exception.getMessage().contains("mismatch"), exception.getMessage());
    }

    @Test
    public void testDuplicateSavepointIdRejected() throws CheckpointStorageException {
        SavepointWriter first =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1003", "attempt-1", new HashSet<>(Arrays.asList(0))));
        first.writePipeline(pipelineState(0, 10, new byte[] {1}));
        first.commitSavepoint(new SavepointMeta(1, "1003", "1", "test", 4000L, null, null));

        SavepointWriter second =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1003", "attempt-2", new HashSet<>(Arrays.asList(0))));
        second.writePipeline(pipelineState(0, 11, new byte[] {2}));
        CheckpointStorageException exception =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                second.commitSavepoint(
                                        new SavepointMeta(
                                                1, "1003", "1", "test", 4001L, null, null)));
        Assertions.assertTrue(exception.getMessage().contains("already exists"));
    }

    @Test
    public void testSavepointIsolatesCheckpointDirectory() throws CheckpointStorageException {
        storage.storeCheckPoint(pipelineState(0, 10, new byte[] {1, 2, 3}));
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1004", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 20, new byte[] {4, 5}));
        writer.commitSavepoint(new SavepointMeta(1, "1004", "1", "test", 5000L, null, null));

        // deleting checkpoints must leave the savepoint bundle untouched
        storage.deleteCheckpoint("1");
        Assertions.assertEquals(0, storage.getAllCheckpoints("1").size());
        Assertions.assertEquals(1, storage.listCompletedSavepoints("1").size());
        Assertions.assertNotNull(storage.readSavepoint("1", "1004"));
    }

    @Test
    public void testIncompleteBundleRejectedOnCommit() throws CheckpointStorageException {
        // expected pipelines {0,1}, only 0 is written -> commit must fail
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1005", "attempt-1", new HashSet<>(Arrays.asList(0, 1))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1}));
        CheckpointStorageException exception =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                writer.commitSavepoint(
                                        new SavepointMeta(
                                                1, "1005", "1", "test", 6000L, null, null)));
        Assertions.assertTrue(exception.getMessage().contains("missing pipeline ids"));
        // the incomplete bundle must not be listed
        Assertions.assertTrue(storage.listCompletedSavepoints("1").isEmpty());
    }

    @Test
    public void testSavepointMetadataValidation() throws Exception {
        // helper: scaffold a committed bundle
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1006", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1}));
        writer.commitSavepoint(new SavepointMeta(1, "1006", "1", "test", 7000L, null, null));

        // 1) job id tamper: rewrite _metadata.ser with a different jobId
        File metaFile =
                tempDir.resolve(SavepointStorageConstants.SAVEPOINT_ROOT_DIR)
                        .resolve("1")
                        .resolve("1006")
                        .resolve(SavepointStorageConstants.META_FILE_NAME)
                        .toFile();
        SavepointMeta tamperedMeta =
                SavepointStorageUtils.deserializeMeta(Files.readAllBytes(metaFile.toPath()));
        tamperedMeta.setJobId("999");
        Files.write(metaFile.toPath(), SavepointStorageUtils.serializeMeta(tamperedMeta));
        CheckpointStorageException e1 =
                Assertions.assertThrows(
                        CheckpointStorageException.class, () -> storage.readSavepoint("1", "1006"));
        Assertions.assertTrue(e1.getMessage().contains("job id mismatch"));

        // restore metadata, then tamper savepoint id
        tamperedMeta.setJobId("1");
        tamperedMeta.setSavepointId("9999");
        Files.write(metaFile.toPath(), SavepointStorageUtils.serializeMeta(tamperedMeta));
        CheckpointStorageException e2 =
                Assertions.assertThrows(
                        CheckpointStorageException.class, () -> storage.readSavepoint("1", "1006"));
        Assertions.assertTrue(e2.getMessage().contains("id mismatch"));
    }

    @Test
    public void testUnsafePayloadFileNameRejected() throws Exception {
        SavepointWriter writer =
                storage.beginSavepoint(
                        new SavepointRequest(
                                "1", "1007", "attempt-1", new HashSet<>(Arrays.asList(0))));
        writer.writePipeline(pipelineState(0, 10, new byte[] {1}));
        writer.commitSavepoint(new SavepointMeta(1, "1007", "1", "test", 8000L, null, null));

        File metaFile =
                tempDir.resolve(SavepointStorageConstants.SAVEPOINT_ROOT_DIR)
                        .resolve("1")
                        .resolve("1007")
                        .resolve(SavepointStorageConstants.META_FILE_NAME)
                        .toFile();
        SavepointMeta meta =
                SavepointStorageUtils.deserializeMeta(Files.readAllBytes(metaFile.toPath()));
        meta.getPipelines().get(0).setPayloadFile("../outside.ser");
        // keep the manifest checksum consistent so the filename check is what fires
        meta.setManifestChecksum(SavepointStorageUtils.manifestChecksum(meta.getPipelines()));
        Files.write(metaFile.toPath(), SavepointStorageUtils.serializeMeta(meta));

        CheckpointStorageException e =
                Assertions.assertThrows(
                        CheckpointStorageException.class, () -> storage.readSavepoint("1", "1007"));
        Assertions.assertTrue(e.getMessage().contains("unsafe payload file name"));
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
