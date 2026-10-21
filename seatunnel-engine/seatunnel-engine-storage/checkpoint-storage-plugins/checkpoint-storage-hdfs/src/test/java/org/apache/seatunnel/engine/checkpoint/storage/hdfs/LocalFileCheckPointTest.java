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

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.HashMap;

@EnabledOnOs({OS.LINUX, OS.MAC})
public class LocalFileCheckPointTest extends AbstractFileCheckPointTest {

    @org.junit.jupiter.api.Test
    public void testRecoveryRejectsCorruptRetainedCheckpoint() throws Exception {
        java.nio.file.Path namespace =
                java.nio.file.Files.createTempDirectory("checkpoint-corrupt-");
        java.util.Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "corrupt-retained";
        org.apache.seatunnel.engine.checkpoint.storage.PipelineState state =
                org.apache.seatunnel.engine.checkpoint.storage.PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build();
        storage.storeCheckPoint(state);
        state.setCheckpointId(9);
        String newest = storage.storeCheckPoint(state);
        try {
            try (org.apache.hadoop.fs.FSDataOutputStream out =
                    storage.fs.create(
                            new org.apache.hadoop.fs.Path(namespace + "/" + job + "/" + newest),
                            true)) {
                out.write(new byte[] {1, 2, 3});
            }
            org.junit.jupiter.api.Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> storage.getCheckpointsForRecovery(job, "1"));
        } finally {
            storage.deleteCheckpoint(job);
            java.nio.file.Files.delete(namespace);
        }
    }

    @org.junit.jupiter.api.Test
    public void testRecoveryRelistsAfterRetentionDeletesAListedCheckpoint() throws Exception {
        assertRecoveryRetentionRace(false, false);
    }

    @org.junit.jupiter.api.Test
    public void testRecoveryDoesNotReturnEmptyAfterListedCheckpointsDisappear() throws Exception {
        assertRecoveryRetentionRace(true, false);
    }

    @org.junit.jupiter.api.Test
    public void testRecoveryDoesNotRollBackWhenNewestListedCheckpointDisappears() throws Exception {
        assertRecoveryRetentionRace(false, true);
    }

    @org.junit.jupiter.api.Test
    public void testRecoveryRereadsNewestCheckpointAndBoundsPruningRetries() throws Exception {
        for (boolean continuous : new boolean[] {false, true}) {
            java.nio.file.Path namespace =
                    java.nio.file.Files.createTempDirectory("checkpoint-relist-");
            java.util.Map<String, String> options = new HashMap<>();
            options.put("namespace", namespace.toString() + "/");
            HdfsStorage storage = new HdfsStorage(options);
            String job = "newest-retention";
            org.apache.seatunnel.engine.checkpoint.storage.PipelineState state =
                    org.apache.seatunnel.engine.checkpoint.storage.PipelineState.builder()
                            .jobId(job)
                            .pipelineId(1)
                            .checkpointId(14)
                            .states(new byte[0])
                            .build();
            storage.storeCheckPoint(state);
            org.apache.hadoop.fs.FileSystem original = storage.fs;
            java.util.concurrent.atomic.AtomicInteger opens =
                    new java.util.concurrent.atomic.AtomicInteger();
            try {
                storage.fs =
                        new org.apache.hadoop.fs.FilterFileSystem(original) {
                            @Override
                            public org.apache.hadoop.fs.FSDataInputStream open(
                                    org.apache.hadoop.fs.Path path, int bufferSize)
                                    throws java.io.IOException {
                                int call = opens.incrementAndGet();
                                if (continuous || call == 1) {
                                    original.delete(path, false);
                                    state.setCheckpointId(14 + call);
                                    try {
                                        storage.storeCheckPoint(state);
                                    } catch (CheckpointStorageException failure) {
                                        throw new java.io.IOException(failure);
                                    }
                                }
                                return super.open(path, bufferSize);
                            }
                        };
                if (continuous) {
                    org.junit.jupiter.api.Assertions.assertThrows(
                            CheckpointStorageException.class,
                            () -> storage.getCheckpointsForRecovery(job, "1"));
                    org.junit.jupiter.api.Assertions.assertEquals(3, opens.get());
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals(
                            15,
                            storage.getCheckpointsForRecovery(job, "1").get(0).getCheckpointId());
                }
            } finally {
                storage.fs = original;
                storage.deleteCheckpoint(job);
                java.nio.file.Files.delete(namespace);
            }
        }
    }

    private void assertRecoveryRetentionRace(boolean removeAll, boolean removeNewest)
            throws Exception {
        java.nio.file.Path namespace =
                java.nio.file.Files.createTempDirectory("checkpoint-retention-");
        java.util.Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "retention-race";
        org.apache.seatunnel.engine.checkpoint.storage.PipelineState state =
                org.apache.seatunnel.engine.checkpoint.storage.PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build();
        String oldFile = storage.storeCheckPoint(state);
        state.setCheckpointId(9);
        String latestFile = storage.storeCheckPoint(state);
        String victimFile = removeNewest ? latestFile : oldFile;
        org.apache.hadoop.fs.FileSystem original = storage.fs;
        java.util.concurrent.atomic.AtomicBoolean removed =
                new java.util.concurrent.atomic.AtomicBoolean();
        try {
            storage.fs =
                    new org.apache.hadoop.fs.FilterFileSystem(original) {
                        @Override
                        public org.apache.hadoop.fs.FSDataInputStream open(
                                org.apache.hadoop.fs.Path path, int bufferSize)
                                throws java.io.IOException {
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
                org.junit.jupiter.api.Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> storage.getCheckpointsForRecovery(job, "1"));
            } else {
                java.util.List<org.apache.seatunnel.engine.checkpoint.storage.PipelineState>
                        restored = storage.getCheckpointsForRecovery(job, "1");
                org.junit.jupiter.api.Assertions.assertEquals(1, restored.size());
                org.junit.jupiter.api.Assertions.assertEquals(9, restored.get(0).getCheckpointId());
            }
            org.junit.jupiter.api.Assertions.assertTrue(removed.get());
        } finally {
            storage.fs = original;
            storage.deleteCheckpoint(job);
            java.nio.file.Files.delete(namespace);
        }
    }

    @org.junit.jupiter.api.Test
    public void testRecoveryDoesNotIgnoreReadFailureAfterSuccessfulListing() throws Exception {
        java.nio.file.Path namespace =
                java.nio.file.Files.createTempDirectory("checkpoint-recovery-");
        java.util.Map<String, String> options = new HashMap<>();
        options.put("namespace", namespace.toString() + "/");
        HdfsStorage storage = new HdfsStorage(options);
        String job = "recovery-read-failure";
        storage.storeCheckPoint(
                org.apache.seatunnel.engine.checkpoint.storage.PipelineState.builder()
                        .jobId(job)
                        .pipelineId(1)
                        .checkpointId(8)
                        .states(new byte[0])
                        .build());
        org.apache.hadoop.fs.FileSystem original = storage.fs;
        try {
            storage.fs =
                    new org.apache.hadoop.fs.FilterFileSystem(original) {
                        @Override
                        public org.apache.hadoop.fs.FSDataInputStream open(
                                org.apache.hadoop.fs.Path path, int bufferSize)
                                throws java.io.IOException {
                            throw new java.io.IOException(
                                    "checkpoint GET denied after LIST succeeded");
                        }
                    };
            org.junit.jupiter.api.Assertions.assertFalse(
                    storage.getFileNames(namespace + "/" + job).isEmpty());
            org.junit.jupiter.api.Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> storage.getCheckpointsForRecovery(job, "1"));
        } finally {
            storage.fs = original;
            storage.deleteCheckpoint(job);
            java.nio.file.Files.delete(namespace);
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
