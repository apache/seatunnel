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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageUnavailableException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.security.AccessControlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@EnabledOnOs({OS.LINUX, OS.MAC})
public class LocalFileCheckPointTest extends AbstractFileCheckPointTest {

    @Test
    public void testRecoveryAllowsNewerFileVersionWithLowerCheckpointId() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-version-race-");
        HdfsStorage storage =
                new HdfsStorage(
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
        FileSystem original = storage.fs;
        storage.fs =
                new FilterFileSystem(original) {
                    @Override
                    public FSDataInputStream open(org.apache.hadoop.fs.Path file, int bufferSize)
                            throws IOException {
                        if (reads.incrementAndGet() == 1) {
                            original.delete(file, false);
                            state.setCheckpointId(7L);
                            Files.write(
                                    directory.resolve("200-0-1-7.ser"),
                                    storage.serializeCheckPointData(state));
                        }
                        return super.open(file, bufferSize);
                    }
                };
        try {
            Assertions.assertEquals(
                    7L,
                    storage.getLatestCheckpointByJobIdAndPipelineId(job, "1").getCheckpointId());
            Assertions.assertEquals(2, reads.get());
        } finally {
            storage.fs = original;
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testLatestPreservesFileVersionOrdering() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-version-");
        HdfsStorage storage =
                new HdfsStorage(
                        new HashMap<>(
                                Collections.singletonMap("namespace", namespace.toString() + "/")));
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
    public void testOnlyInitialTransportListingFailureCanRestartRecoveryScan() throws Exception {
        HdfsStorage storage = Mockito.spy(new HdfsStorage(new HashMap<>()));
        Mockito.doThrow(
                        new CheckpointStorageException(
                                "LIST timed out", new SocketTimeoutException()))
                .when(storage)
                .getFileNames(ArgumentMatchers.anyString());
        Assertions.assertThrows(
                CheckpointStorageUnavailableException.class,
                () -> storage.getLatestCheckpointByJobIdAndPipelineId("outage", "1"));
        Mockito.doThrow(
                        new CheckpointStorageException(
                                "Access denied", new AccessControlException("denied")))
                .when(storage)
                .getFileNames(ArgumentMatchers.anyString());
        Exception denied =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> storage.getLatestCheckpointByJobIdAndPipelineId("outage", "1"));
        Assertions.assertFalse(denied instanceof CheckpointStorageUnavailableException);
    }

    @Test
    public void testLegacyBulkReadSkipsCorruptOlderCheckpoint() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-legacy-bulk-");
        Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "legacy-bulk";
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8L)
                        .states(new byte[0])
                        .build();
        String older = storage.storeCheckPoint(state);
        state.setCheckpointId(9L);
        storage.storeCheckPoint(state);
        try {
            try (FSDataOutputStream out =
                    storage.fs.create(
                            new org.apache.hadoop.fs.Path(namespace + "/" + job + "/" + older),
                            true)) {
                out.write(new byte[] {1, 2, 3});
            }
            Assertions.assertEquals(1, storage.getCheckpointsByJobIdAndPipelineId(job, "1").size());
            Assertions.assertEquals(
                    9L,
                    storage.getLatestCheckpointByJobIdAndPipelineId(job, "1").getCheckpointId());
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRejectsCorruptRetainedCheckpoint() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-corrupt-");
        Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "corrupt-retained";
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build();
        storage.storeCheckPoint(state);
        state.setCheckpointId(9);
        String newest = storage.storeCheckPoint(state);
        try {
            try (FSDataOutputStream out =
                    storage.fs.create(
                            new org.apache.hadoop.fs.Path(namespace + "/" + job + "/" + newest),
                            true)) {
                out.write(new byte[] {1, 2, 3});
            }
            Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
        } finally {
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryRelistsAfterRetentionDeletesAListedCheckpoint() throws Exception {
        assertRecoveryRetentionRace(false, false);
    }

    @Test
    public void testRecoveryDoesNotReturnEmptyAfterListedCheckpointsDisappear() throws Exception {
        assertRecoveryRetentionRace(true, false);
    }

    @Test
    public void testRecoveryDoesNotRollBackWhenNewestListedCheckpointDisappears() throws Exception {
        assertRecoveryRetentionRace(false, true);
    }

    @Test
    public void testRecoveryRereadsNewestCheckpointAndBoundsPruningRetries() throws Exception {
        for (boolean continuous : new boolean[] {false, true}) {
            Path namespace = Files.createTempDirectory("checkpoint-relist-");
            Map<String, String> options = new HashMap<>();
            options.put("namespace", namespace.toString() + "/");
            HdfsStorage storage = new HdfsStorage(options);
            String job = "newest-retention";
            PipelineState state =
                    PipelineState.builder()
                            .jobId(job)
                            .pipelineId(1)
                            .checkpointId(14)
                            .states(new byte[0])
                            .build();
            storage.storeCheckPoint(state);
            FileSystem original = storage.fs;
            AtomicInteger opens = new AtomicInteger();
            try {
                storage.fs =
                        new FilterFileSystem(original) {
                            @Override
                            public FSDataInputStream open(
                                    org.apache.hadoop.fs.Path path, int bufferSize)
                                    throws IOException {
                                int call = opens.incrementAndGet();
                                if (continuous || call == 1) {
                                    original.delete(path, false);
                                    state.setCheckpointId(14 + call);
                                    try {
                                        storage.storeCheckPoint(state);
                                    } catch (CheckpointStorageException failure) {
                                        throw new IOException(failure);
                                    }
                                }
                                return super.open(path, bufferSize);
                            }
                        };
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
                storage.fs = original;
                storage.deleteCheckpoint(job);
                Files.delete(namespace);
            }
        }
    }

    private void assertRecoveryRetentionRace(boolean removeAll, boolean removeNewest)
            throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-retention-");
        Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "retention-race";
        PipelineState state =
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build();
        String oldFile = storage.storeCheckPoint(state);
        state.setCheckpointId(9);
        String latestFile = storage.storeCheckPoint(state);
        String victimFile = (removeAll || removeNewest) ? latestFile : oldFile;
        FileSystem original = storage.fs;
        AtomicBoolean removed = new AtomicBoolean();
        try {
            storage.fs =
                    new FilterFileSystem(original) {
                        @Override
                        public FSDataInputStream open(
                                org.apache.hadoop.fs.Path path, int bufferSize) throws IOException {
                            if (path.getName().equals(victimFile)
                                    && removed.compareAndSet(false, true)) {
                                original.delete(path, false);
                                if (removeAll) {
                                    original.delete(
                                            new org.apache.hadoop.fs.Path(
                                                    namespace + "/" + job + "/" + latestFile),
                                            false);
                                }
                            }
                            return super.open(path, bufferSize);
                        }
                    };
            if (removeAll || removeNewest) {
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
            } else {
                PipelineState restored = storage.getLatestCheckpointByJobIdAndPipelineId(job, "1");
                Assertions.assertEquals(9, restored.getCheckpointId());
            }
            Assertions.assertEquals(removeAll || removeNewest, removed.get());
        } finally {
            storage.fs = original;
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @Test
    public void testRecoveryDoesNotIgnoreReadFailureAfterSuccessfulListing() throws Exception {
        Path namespace = Files.createTempDirectory("checkpoint-recovery-");
        Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "recovery-read-failure";
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build());
        FileSystem original = storage.fs;
        try {
            storage.fs =
                    new FilterFileSystem(original) {
                        @Override
                        public FSDataInputStream open(
                                org.apache.hadoop.fs.Path path, int bufferSize) throws IOException {
                            throw new SocketTimeoutException(
                                    "checkpoint GET timed out after LIST succeeded");
                        }
                    };
            Assertions.assertFalse(storage.getFileNames(namespace + "/" + job).isEmpty());
            Exception failure =
                    Assertions.assertThrows(
                            CheckpointStorageException.class,
                            () -> storage.getLatestCheckpointByJobIdAndPipelineId(job, "1"));
            Assertions.assertFalse(failure instanceof CheckpointStorageUnavailableException);
        } finally {
            storage.fs = original;
            storage.deleteCheckpoint(job);
            Files.delete(namespace);
        }
    }

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        HashMap config = new HashMap();
        config.put("namespace", "/tmp/");
        config.put("disable.cache", "false");
        STORAGE = new HdfsStorage(config);
        initStorageData();
    }
}
