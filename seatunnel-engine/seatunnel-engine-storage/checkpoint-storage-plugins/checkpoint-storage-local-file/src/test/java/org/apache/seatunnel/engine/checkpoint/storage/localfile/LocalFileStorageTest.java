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

import org.apache.commons.io.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testRecoveryAllowsNewerFileVersionWithLowerCheckpointId() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-version-race-");
        LocalFileStorage storage =
                new LocalFileStorage(
                        new HashMap<>(
                                Collections.singletonMap("namespace", namespace.toString() + "/")));
        String job = "version-race";
        Path directory = Files.createDirectories(namespace.resolve(job));
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(12L)
                        .states(new byte[0])
                        .build();
        Files.write(directory.resolve("100-0-1-12.ser"), storage.serializeCheckPointData(state));
        AtomicInteger reads = new AtomicInteger();
        LocalFileStorage recovering = Mockito.spy(storage);
        Mockito.doAnswer(
                        call -> {
                            Path file = call.getArgument(0);
                            if (reads.incrementAndGet() == 1) {
                                Files.delete(file);
                                state.setCheckpointId(7L);
                                Files.write(
                                        directory.resolve("200-0-1-7.ser"),
                                        storage.serializeCheckPointData(state));
                            }
                            return call.callRealMethod();
                        })
                .when(recovering)
                .readRecoveryCheckpoint(Mockito.any(Path.class));
        try {
            Assertions.assertEquals(
                    7L,
                    recovering.getLatestCheckpointByJobIdAndPipelineId(job, "1").getCheckpointId());
            Assertions.assertEquals(2, reads.get());
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testLatestPreservesFileVersionOrdering() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-version-");
        LocalFileStorage storage =
                new LocalFileStorage(
                        Collections.singletonMap("namespace", namespace.toString() + "/"));
        String job = "version-order";
        Path directory = Files.createDirectories(namespace.resolve(job));
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(12L)
                        .states(new byte[0])
                        .build();
        Files.write(directory.resolve("100-0-1-12.ser"), storage.serializeCheckPointData(state));
        state.setCheckpointId(8L);
        Files.write(directory.resolve("200-0-1-8.ser"), storage.serializeCheckPointData(state));
        try {
            Assertions.assertEquals(
                    8L,
                    storage.getLatestCheckpointByJobIdAndPipelineId(job, "1").getCheckpointId());
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
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
                STORAGE.getCheckpointsByJobIdAndPipelineId("absent-" + UUID.randomUUID(), "1")
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
        Path namespace = Files.createTempDirectory("local-directory-");
        LocalFileStorage storage =
                new LocalFileStorage(
                        Collections.singletonMap("namespace", namespace.toString() + "/")) {
                    @Override
                    PipelineState readRecoveryCheckpoint(Path file) throws IOException {
                        Files.delete(file);
                        Files.delete(file.getParent());
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
                    () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRereadsNewestCheckpointAndBoundsPruningRetries() throws Exception {
        for (boolean continuous : new boolean[] {false, true}) {
            Path namespace = Files.createTempDirectory("local-relist-");
            String job = "newest-retention";
            PipelineState state =
                    PipelineState.builder()
                            .jobId(job)
                            .pipelineId(1)
                            .checkpointId(14)
                            .states(new byte[0])
                            .build();
            AtomicInteger opens = new AtomicInteger();
            LocalFileStorage storage =
                    new LocalFileStorage(
                            Collections.singletonMap("namespace", namespace.toString() + "/")) {
                        @Override
                        PipelineState readRecoveryCheckpoint(Path file) throws IOException {
                            int call = opens.incrementAndGet();
                            if (continuous || call == 1) {
                                Files.delete(file);
                                state.setCheckpointId(14 + call);
                                try {
                                    storeCheckPoint(state);
                                } catch (CheckpointStorageException failure) {
                                    throw new IOException(failure);
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
                            () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
                    Assertions.assertEquals(3, opens.get());
                } else {
                    Assertions.assertEquals(
                            15,
                            storage.getLatestCheckpointByJobIdAndPipelineId(job, "1")
                                    .getCheckpointId());
                }
            } finally {
                storage.deleteCheckpoint(job);
                Files.delete(namespace);
            }
        }
    }

    private void assertRecoveryRetentionRace(boolean removeAll, boolean removeNewest)
            throws Exception {
        Path namespace = Files.createTempDirectory("local-retention-");
        AtomicReference<Path> victim = new AtomicReference<>();
        AtomicReference<Path> latest = new AtomicReference<>();
        LocalFileStorage storage =
                new LocalFileStorage(
                        Collections.singletonMap("namespace", namespace.toString() + "/")) {
                    @Override
                    PipelineState readRecoveryCheckpoint(Path file) throws IOException {
                        Path removed = victim.get();
                        if (file.equals(removed) && victim.compareAndSet(removed, null)) {
                            Files.delete(file);
                            if (removeAll) {
                                Files.delete(latest.get());
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
        victim.set(Paths.get(storage.storeCheckPoint(state)));
        state.setCheckpointId(9);
        latest.set(Paths.get(storage.storeCheckPoint(state)));
        if (removeAll || removeNewest) {
            victim.set(latest.get());
        }
        try {
            if (removeAll || removeNewest) {
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
            } else {
                PipelineState restored = storage.getLatestCheckpointByJobIdAndPipelineId(job, "1");
                Assertions.assertEquals(9, restored.getCheckpointId());
            }
            if (removeAll || removeNewest) {
                Assertions.assertNull(victim.get());
            }
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRejectsCorruptCheckpointInsteadOfReturningPartialState()
            throws Exception {
        String job = "corrupt-" + UUID.randomUUID();
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
            Files.write(Paths.get(latestFile), new byte[] {1, 2, 3});
            Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> STORAGE.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
        } finally {
            STORAGE.deleteCheckpoint(job);
        }
    }

    @Test
    public void testLegacyBulkReadSkipsUnreadableOldCheckpoint() throws Exception {
        String job = "legacy-corrupt-" + UUID.randomUUID();
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(7)
                        .states(new byte[0])
                        .build();
        String old = STORAGE.storeCheckPoint(state);
        Thread.sleep(5);
        state.setCheckpointId(8);
        STORAGE.storeCheckPoint(state);
        try {
            try (MockedStatic<FileUtils> files =
                    Mockito.mockStatic(FileUtils.class, Mockito.CALLS_REAL_METHODS)) {
                files.when(() -> FileUtils.readFileToByteArray(new File(old)))
                        .thenThrow(new IOException("old checkpoint unreadable"));
                List<PipelineState> states = STORAGE.getCheckpointsByJobIdAndPipelineId(job, "1");
                Assertions.assertEquals(1, states.size());
                Assertions.assertEquals(8, states.get(0).getCheckpointId());
                Assertions.assertEquals(
                        8,
                        STORAGE.getLatestCheckpointByJobIdAndPipelineId(job, "1")
                                .getCheckpointId());
            }
        } finally {
            STORAGE.deleteCheckpoint(job);
        }
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }
}
