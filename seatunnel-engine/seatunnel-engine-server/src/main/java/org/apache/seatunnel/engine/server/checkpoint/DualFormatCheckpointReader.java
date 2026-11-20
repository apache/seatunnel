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
import org.apache.seatunnel.engine.checkpoint.storage.state.V1CheckpointFile;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads restorable checkpoints in both on-storage formats (STIP-45):
 *
 * <ul>
 *   <li>v1: one ProtoStuff blob per pipeline checkpoint ({@code <jobId>/*.ser});
 *   <li>v2: {@code chk-<id>/_metadata} files of a {@link ChunkSupportStorage}, whose payload is the
 *       same serialized {@link CompletedCheckpoint} that v1 keeps in {@link
 *       PipelineState#getStates()}.
 * </ul>
 *
 * <p>Both are returned as {@link PipelineState}, so the restore path downstream is identical.
 *
 * <p><b>Failure contract (fail closed)</b>, for storages implementing {@link ChunkSupportStorage}:
 * candidates are enumerated from file names first and then read strictly, so an unreadable
 * checkpoint is never mistaken for a missing one (the v1 {@link CheckpointStorage} getters log and
 * skip unreadable files). An unreadable candidate, e.g. a v2 metadata file that fails its CRC or a
 * corrupt v1 blob, is skipped with a warning and the next older candidate is used. If completed
 * checkpoints exist but <em>none</em> of them can be read, a {@link CheckpointStorageException} is
 * thrown: returning "no checkpoint" there would silently restart the job from its initial position.
 * Other storages keep their v1-only behavior unchanged.
 */
@Slf4j
final class DualFormatCheckpointReader {

    private DualFormatCheckpointReader() {}

    /**
     * Returns the newest readable checkpoint across both formats, or {@code null} if the pipeline
     * has no completed checkpoint at all.
     *
     * <p>v1 candidates keep their existing order (newest write timestamp first, as {@link
     * CheckpointStorage#getLatestCheckpointByJobIdAndPipelineId} uses) and v2 candidates are
     * ordered by checkpoint id; the two lists are merged by checkpoint id, v2 first on a tie.
     *
     * @throws CheckpointStorageException if completed checkpoints exist but none can be read
     */
    static PipelineState getLatestCheckpoint(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        if (!(storage instanceof ChunkSupportStorage)) {
            return storage.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId);
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        List<V1CheckpointFile> v1 = chunkStorage.listV1Checkpoints(jobId, pipelineId);
        List<Long> v2 = chunkStorage.listCompletedCheckpointIds(jobId, pipelineId);
        List<String> failures = new ArrayList<>();
        int nextV1 = 0;
        int nextV2 = v2.size() - 1;
        while (nextV1 < v1.size() || nextV2 >= 0) {
            boolean takeV2 =
                    nextV2 >= 0
                            && (nextV1 >= v1.size()
                                    || v2.get(nextV2) >= v1.get(nextV1).getCheckpointId());
            PipelineState state =
                    takeV2
                            ? readV2(chunkStorage, jobId, pipelineId, v2.get(nextV2--), failures)
                            : readV1(chunkStorage, v1.get(nextV1++), failures);
            if (state != null) {
                return state;
            }
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
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        List<String> failures = new ArrayList<>();
        List<PipelineState> states = new ArrayList<>();
        for (V1CheckpointFile file : chunkStorage.listV1Checkpoints(jobId, pipelineId)) {
            PipelineState v1 = readV1(chunkStorage, file, failures);
            if (v1 != null) {
                states.add(v1);
            }
        }
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
                            "Job %s, pipeline %s has %d completed checkpoint(s) but none is "
                                    + "readable, refusing to restore from scratch: %s",
                            jobId, pipelineId, failures.size(), failures));
        }
    }

    private static PipelineState readV1(
            ChunkSupportStorage storage, V1CheckpointFile file, List<String> failures) {
        try {
            return storage.readV1Checkpoint(file);
        } catch (CheckpointStorageException e) {
            failures.add("v1 checkpoint " + file.getCheckpointId() + ": " + e.getMessage());
            log.warn(
                    "Skip unreadable v1 checkpoint {} of job {}, pipeline {}; "
                            + "falling back to an older checkpoint",
                    file.getLocation(),
                    file.getJobId(),
                    file.getPipelineId(),
                    e);
            return null;
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
