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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.CheckpointService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * Unit coverage for the type-aware checkpoint cleanup that protects savepoints from the automatic
 * terminal-state cleanup.
 *
 * <p>Savepoints and periodic checkpoints share one directory per job id, and the checkpoint type
 * only lives inside the serialized {@link CompletedCheckpoint}, so the cleanup has to read every
 * stored entry before it may delete anything. The end-to-end behaviour is covered by {@code
 * SavepointSurvivesRestoredJobCancelIT}; these tests pin the classification itself, including the
 * cases the E2E test cannot create on demand (no savepoint at all, and an entry whose type cannot
 * be decoded).
 */
class CheckpointStorageCleanupTest {

    private static final long JOB_ID = 424242L;

    private static final ProtoStuffSerializer SERIALIZER = new ProtoStuffSerializer();

    @TempDir Path tempDir;

    @Test
    void testKeepsSavepointsAndDeletesOnlyPeriodicCheckpoints() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.SAVEPOINT_TYPE);
        storeCheckpoint(storage, 1, 3, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 2, 1, CheckpointType.CHECKPOINT_TYPE);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertEquals(
                new TreeSet<>(Collections.singleton("1:2:SAVEPOINT_TYPE")),
                describe(storage.getAllCheckpoints(String.valueOf(JOB_ID))),
                "the savepoint must survive while every periodic checkpoint is deleted");
    }

    @Test
    void testDeletesEverythingWhenNoSavepointIsStored() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.CHECKPOINT_TYPE);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertTrue(
                storage.getAllCheckpoints(String.valueOf(JOB_ID)).isEmpty(),
                "without a savepoint the cleanup keeps its previous behaviour and removes the job"
                        + " data");
    }

    @Test
    void testKeepsAllCheckpointDataWhenAnEntryCannotBeClassified() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(JOB_ID))
                        .pipelineId(1)
                        .checkpointId(2)
                        .states("not-a-serialized-checkpoint".getBytes(StandardCharsets.UTF_8))
                        .build());

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("1:1:CHECKPOINT_TYPE", "1:2:UNKNOWN")),
                describe(storage.getAllCheckpoints(String.valueOf(JOB_ID))),
                "an entry whose type cannot be decoded must never be deleted, because it could be"
                        + " a savepoint");
    }

    private CheckpointStorage createStorage() {
        return new LocalFileStorage(
                Collections.singletonMap(StorageConstants.STORAGE_NAME_SPACE, tempDir.toString()));
    }

    private static void storeCheckpoint(
            CheckpointStorage storage, int pipelineId, long checkpointId, CheckpointType type)
            throws Exception {
        long now = System.currentTimeMillis();
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        JOB_ID,
                        pipelineId,
                        checkpointId,
                        now,
                        type,
                        now,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(JOB_ID))
                        .pipelineId(pipelineId)
                        .checkpointId(checkpointId)
                        .states(SERIALIZER.serialize(checkpoint))
                        .build());
    }

    private static TreeSet<String> describe(List<PipelineState> pipelineStates) {
        TreeSet<String> descriptions = new TreeSet<>();
        for (PipelineState pipelineState : pipelineStates) {
            String type = "UNKNOWN";
            try {
                CompletedCheckpoint checkpoint =
                        SERIALIZER.deserialize(
                                pipelineState.getStates(), CompletedCheckpoint.class);
                if (checkpoint.getCheckpointType() != null) {
                    type = checkpoint.getCheckpointType().name();
                }
            } catch (Exception ignored) {
                // Keep UNKNOWN so the assertion also covers undecodable entries.
            }
            descriptions.add(
                    pipelineState.getPipelineId()
                            + ":"
                            + pipelineState.getCheckpointId()
                            + ":"
                            + type);
        }
        return descriptions;
    }
}
