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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import java.util.List;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class LocalFileStorageTest {

    private static LocalFileStorage STORAGE = new LocalFileStorage(null);
    private static final String JOB_ID = "chris";

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

    @Test
    public void testRecoveryReturnsOnlyRequestedPipelineAndAllowsAbsentJob() throws Exception {
        Assertions.assertEquals(2, STORAGE.getCheckpointsByJobIdAndPipelineId(JOB_ID, "1").size());
        Assertions.assertTrue(
                STORAGE.getCheckpointsByJobIdAndPipelineId(
                                "absent-" + java.util.UUID.randomUUID(), "1")
                        .isEmpty());
    }

    @Test
    public void testRecoveryRelistsAfterRetentionDeletesAListedCheckpoint() throws Exception {
        assertRecoveryRetentionRace(false, false);
    }

    @Test
    public void testRecoveryRejectsEmptyRelistAfterRetentionRace() throws Exception {
        assertRecoveryRetentionRace(true, false);
    }

    @Test
    public void testRecoveryDoesNotRollBackWhenNewestListedCheckpointDisappears() throws Exception {
        assertRecoveryRetentionRace(false, true);
    }

    @Test
    public void testRecoveryRejectsDirectoryDisappearingAfterListedCheckpoint() throws Exception {
        java.nio.file.Path namespace = java.nio.file.Files.createTempDirectory("local-directory-");
        LocalFileStorage storage =
                new LocalFileStorage(
                        java.util.Collections.singletonMap(
                                "namespace", namespace.toString() + "/")) {
                    @Override
                    PipelineState readRecoveryCheckpoint(java.nio.file.Path file)
                            throws java.io.IOException {
                        java.nio.file.Files.delete(file);
                        java.nio.file.Files.delete(file.getParent());
                        return super.readRecoveryCheckpoint(file);
                    }
                };
        String job = "removed-directory";
        try {
            storage.storeCheckPoint(
                    PipelineState.builder()
                            .jobId(job)
                            .pipelineId(1)
                            .checkpointId(8)
                            .states(new byte[0])
                            .build());
            Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> storage.getCheckpointsByJobIdAndPipelineId(job, "1"));
        } finally {
            storage.deleteCheckpoint(job);
            java.nio.file.Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRereadsNewestCheckpointAndBoundsPruningRetries() throws Exception {
        for (boolean continuous : new boolean[] {false, true}) {
            java.nio.file.Path namespace = java.nio.file.Files.createTempDirectory("local-relist-");
            String job = "newest-retention";
            PipelineState state =
                    PipelineState.builder()
                            .jobId(job)
                            .pipelineId(1)
                            .checkpointId(14)
                            .states(new byte[0])
                            .build();
            java.util.concurrent.atomic.AtomicInteger opens =
                    new java.util.concurrent.atomic.AtomicInteger();
            LocalFileStorage storage =
                    new LocalFileStorage(
                            java.util.Collections.singletonMap(
                                    "namespace", namespace.toString() + "/")) {
                        @Override
                        PipelineState readRecoveryCheckpoint(java.nio.file.Path file)
                                throws java.io.IOException {
                            int call = opens.incrementAndGet();
                            if (continuous || call == 1) {
                                java.nio.file.Files.delete(file);
                                state.setCheckpointId(14 + call);
                                try {
                                    storeCheckPoint(state);
                                } catch (CheckpointStorageException failure) {
                                    throw new java.io.IOException(failure);
                                }
                            }
                            return super.readRecoveryCheckpoint(file);
                        }
                    };
            storage.storeCheckPoint(state);
            try {
                if (continuous) {
                    Assertions.assertThrows(
                            CheckpointStorageException.class,
                            () -> storage.getCheckpointsByJobIdAndPipelineId(job, "1"));
                    Assertions.assertEquals(3, opens.get());
                } else {
                    Assertions.assertEquals(
                            15,
                            storage.getCheckpointsByJobIdAndPipelineId(job, "1")
                                    .get(0)
                                    .getCheckpointId());
                }
            } finally {
                storage.deleteCheckpoint(job);
                java.nio.file.Files.delete(namespace);
            }
        }
    }

    private void assertRecoveryRetentionRace(boolean removeAll, boolean removeNewest)
            throws Exception {
        java.nio.file.Path namespace = java.nio.file.Files.createTempDirectory("local-retention-");
        java.util.concurrent.atomic.AtomicReference<java.nio.file.Path> victim =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<java.nio.file.Path> latest =
                new java.util.concurrent.atomic.AtomicReference<>();
        LocalFileStorage storage =
                new LocalFileStorage(
                        java.util.Collections.singletonMap(
                                "namespace", namespace.toString() + "/")) {
                    @Override
                    PipelineState readRecoveryCheckpoint(java.nio.file.Path file)
                            throws java.io.IOException {
                        java.nio.file.Path removed = victim.get();
                        if (file.equals(removed) && victim.compareAndSet(removed, null)) {
                            java.nio.file.Files.delete(file);
                            if (removeAll) {
                                java.nio.file.Files.delete(latest.get());
                            }
                        }
                        return super.readRecoveryCheckpoint(file);
                    }
                };
        String job = "retention-race";
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build();
        victim.set(java.nio.file.Paths.get(storage.storeCheckPoint(state)));
        state.setCheckpointId(9);
        latest.set(java.nio.file.Paths.get(storage.storeCheckPoint(state)));
        if (removeNewest) {
            victim.set(latest.get());
        }
        try {
            if (removeAll || removeNewest) {
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> storage.getCheckpointsByJobIdAndPipelineId(job, "1"));
            } else {
                List<PipelineState> restored = storage.getCheckpointsByJobIdAndPipelineId(job, "1");
                Assertions.assertEquals(1, restored.size());
                Assertions.assertEquals(9, restored.get(0).getCheckpointId());
            }
            Assertions.assertNull(victim.get());
        } finally {
            storage.deleteCheckpoint(job);
            java.nio.file.Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRejectsCorruptCheckpointInsteadOfReturningPartialState()
            throws Exception {
        String job = "corrupt-" + java.util.UUID.randomUUID();
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(7)
                        .states(new byte[0])
                        .build();
        STORAGE.storeCheckPoint(state);
        state.setCheckpointId(8);
        String latestFile = STORAGE.storeCheckPoint(state);
        try {
            java.nio.file.Files.write(java.nio.file.Paths.get(latestFile), new byte[] {1, 2, 3});
            Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> STORAGE.getCheckpointsByJobIdAndPipelineId(job, "1"));
        } finally {
            STORAGE.deleteCheckpoint(job);
        }
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }
}
