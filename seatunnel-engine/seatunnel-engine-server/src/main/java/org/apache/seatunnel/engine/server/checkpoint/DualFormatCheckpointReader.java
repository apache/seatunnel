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
import org.apache.seatunnel.engine.checkpoint.storage.api.ChunkSupportStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads restorable checkpoints in both on-storage formats (STIP-45):
 *
 * <ul>
 *   <li>v1: one ProtoStuff blob per pipeline checkpoint, read through the {@link CheckpointStorage}
 *       methods;
 *   <li>v2: {@code chk-<id>/_metadata} files of a {@link ChunkSupportStorage}, whose payload is the
 *       same serialized {@link CompletedCheckpoint} that v1 keeps in {@link
 *       PipelineState#getStates()}.
 * </ul>
 *
 * <p>Both are returned as {@link PipelineState}, so the restore path downstream is identical.
 *
 * <p><b>Failure contract (fail closed):</b> a candidate that cannot be read (e.g. a v2 metadata
 * file that fails its CRC after a partial write on an object store, or an unreadable v1 blob) is
 * skipped with a warning and the next older restorable checkpoint is used. If completed checkpoints
 * exist but <em>none</em> of them can be read, a {@link CheckpointStorageException} is thrown:
 * returning "no checkpoint" there would silently restart the job from its initial position.
 */
@Slf4j
final class DualFormatCheckpointReader {

    private DualFormatCheckpointReader() {}

    /**
     * Returns the restorable checkpoint with the highest checkpoint id across both formats, or
     * {@code null} if the pipeline has no completed checkpoint at all. On an id tie the v2
     * checkpoint wins.
     *
     * @throws CheckpointStorageException if completed checkpoints exist but none can be read
     */
    static PipelineState getLatestCheckpoint(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        if (!(storage instanceof ChunkSupportStorage)) {
            // unchanged v1-only behavior, including propagating read failures
            return storage.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId);
        }
        List<String> failures = new ArrayList<>();
        PipelineState v1 = null;
        try {
            v1 = storage.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId);
        } catch (CheckpointStorageException | RuntimeException e) {
            // a broken v1 blob must not hide a readable v2 checkpoint
            failures.add("latest v1 checkpoint: " + e.getMessage());
            log.warn(
                    "Failed to read the latest v1 checkpoint of job {}, pipeline {}; trying v2",
                    jobId,
                    pipelineId,
                    e);
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        List<Long> ids = chunkStorage.listCompletedCheckpointIds(jobId, pipelineId);
        for (int i = ids.size() - 1; i >= 0; i--) {
            long checkpointId = ids.get(i);
            if (v1 != null && v1.getCheckpointId() > checkpointId) {
                return v1;
            }
            PipelineState v2 = readV2(chunkStorage, jobId, pipelineId, checkpointId, failures);
            if (v2 != null) {
                return v2;
            }
        }
        if (v1 != null) {
            return v1;
        }
        failIfUnreadable(jobId, pipelineId, failures);
        return null;
    }

    /**
     * Returns every readable checkpoint of the pipeline in both formats, in no fixed order;
     * unreadable candidates are skipped.
     *
     * @throws CheckpointStorageException if completed checkpoints exist but none can be read
     */
    static List<PipelineState> getCheckpoints(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        if (!(storage instanceof ChunkSupportStorage)) {
            return storage.getCheckpointsByJobIdAndPipelineId(jobId, pipelineId);
        }
        List<String> failures = new ArrayList<>();
        List<PipelineState> states = new ArrayList<>();
        try {
            states.addAll(storage.getCheckpointsByJobIdAndPipelineId(jobId, pipelineId));
        } catch (CheckpointStorageException | RuntimeException e) {
            failures.add("v1 checkpoints: " + e.getMessage());
            log.warn(
                    "Failed to read the v1 checkpoints of job {}, pipeline {}; trying v2",
                    jobId,
                    pipelineId,
                    e);
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        for (long checkpointId : chunkStorage.listCompletedCheckpointIds(jobId, pipelineId)) {
            PipelineState v2 = readV2(chunkStorage, jobId, pipelineId, checkpointId, failures);
            if (v2 != null) {
                states.add(v2);
            }
        }
        if (states.isEmpty()) {
            failIfUnreadable(jobId, pipelineId, failures);
        }
        return states;
    }

    private static void failIfUnreadable(String jobId, String pipelineId, List<String> failures)
            throws CheckpointStorageException {
        if (!failures.isEmpty()) {
            throw new CheckpointStorageException(
                    String.format(
                            "Job %s, pipeline %s has %d completed checkpoint candidate(s) but none "
                                    + "is readable, refusing to restore from scratch: %s",
                            jobId, pipelineId, failures.size(), failures));
        }
    }

    private static PipelineState readV2(
            ChunkSupportStorage storage,
            String jobId,
            String pipelineId,
            long checkpointId,
            List<String> failures) {
        MetadataEnvelope envelope;
        try {
            envelope = storage.readMetadata(jobId, pipelineId, checkpointId);
        } catch (CheckpointStorageException e) {
            failures.add("v2 checkpoint " + checkpointId + ": " + e.getMessage());
            log.warn(
                    "Skip unreadable checkpoint metadata of job {}, pipeline {}, checkpoint {}; "
                            + "falling back to an older checkpoint",
                    jobId,
                    pipelineId,
                    checkpointId,
                    e);
            return null;
        }
        return PipelineState.builder()
                .jobId(jobId)
                .pipelineId(Integer.parseInt(pipelineId))
                .checkpointId(checkpointId)
                .states(envelope.getPayload())
                .build();
    }
}
