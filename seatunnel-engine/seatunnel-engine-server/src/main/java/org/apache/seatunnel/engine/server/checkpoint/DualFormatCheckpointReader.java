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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CorruptCheckpointException;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;
import org.apache.seatunnel.engine.checkpoint.storage.state.V1CheckpointFile;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.utils.CheckpointRestoreUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
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
 * <p><b>Failure contract</b>, for storages implementing {@link ChunkSupportStorage} (other storages
 * keep their v1-only behavior unchanged). Candidates are enumerated from file names first and then
 * read strictly, so an unreadable checkpoint is never mistaken for a missing one:
 *
 * <ul>
 *   <li>A candidate whose content is invalid ({@link CorruptCheckpointException}: it fails framing
 *       or CRC validation, cannot be deserialized, or does not belong to the job, pipeline and
 *       checkpoint it is stored as) is skipped with a warning and the next older candidate is used.
 *   <li>A candidate that cannot be read at all (any other {@link CheckpointStorageException}, e.g.
 *       a transient I/O error or missing permissions) fails the restore: the checkpoint itself may
 *       be intact, and restoring an older one would replay data.
 *   <li>If completed checkpoints exist but every one of them is corrupt, a {@link
 *       CheckpointStorageException} is thrown instead of restarting the job from scratch.
 * </ul>
 */
@Slf4j
final class DualFormatCheckpointReader {

    private DualFormatCheckpointReader() {}

    /**
     * Returns the newest valid checkpoint across both formats, or {@code null} if the pipeline has
     * no completed checkpoint at all.
     *
     * <p>v1 candidates keep their existing order (newest write timestamp first, as {@link
     * CheckpointStorage#getLatestCheckpointByJobIdAndPipelineId} uses) and v2 candidates are
     * ordered by checkpoint id; the two lists are merged by checkpoint id, v2 first on a tie.
     *
     * @throws CheckpointStorageException if a candidate cannot be read, or completed checkpoints
     *     exist but all of them are corrupt
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
        Candidates candidates = new Candidates();
        int nextV1 = 0;
        int nextV2 = v2.size() - 1;
        while (nextV1 < v1.size() || nextV2 >= 0) {
            boolean takeV2 =
                    nextV2 >= 0
                            && (nextV1 >= v1.size()
                                    || v2.get(nextV2) >= v1.get(nextV1).getCheckpointId());
            PipelineState state =
                    takeV2
                            ? readV2(chunkStorage, jobId, pipelineId, v2.get(nextV2--), candidates)
                            : readV1(chunkStorage, v1.get(nextV1++), candidates);
            if (state != null) {
                return state;
            }
        }
        candidates.failIfNothingValid(jobId, pipelineId);
        return null;
    }

    /**
     * Returns every valid checkpoint of the pipeline in both formats, in no fixed order; corrupt
     * candidates are skipped.
     *
     * @throws CheckpointStorageException if a candidate cannot be read, or completed checkpoints
     *     exist but all of them are corrupt
     */
    static List<PipelineState> getCheckpoints(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        Candidates candidates = readAll(storage, jobId, pipelineId);
        if (candidates.states.isEmpty()) {
            candidates.failIfNothingValid(jobId, pipelineId);
        }
        return candidates.states;
    }

    /**
     * Returns the newest valid checkpoint whose type matches the restore mode (see {@link
     * CheckpointRestoreUtils#matchesRestoreCheckpointType}), or {@code null} if the pipeline has
     * none and no candidate was skipped.
     *
     * <p>The type of a corrupt candidate is unknown, so it may have been the requested checkpoint.
     * Therefore this fails when nothing matches but a candidate was corrupt, and, for {@link
     * RestoreMode#SAVEPOINT}, also when a corrupt candidate is newer than the matching savepoint:
     * an explicitly requested savepoint is never silently replaced by an older one.
     *
     * @throws CheckpointStorageException in the cases above, or if a candidate cannot be read
     */
    static PipelineState getLatestCheckpointOfType(
            CheckpointStorage storage, String jobId, String pipelineId, RestoreMode restoreMode)
            throws CheckpointStorageException {
        Candidates candidates = readAll(storage, jobId, pipelineId);
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        PipelineState matched = null;
        for (PipelineState state : candidates.states) {
            CompletedCheckpoint checkpoint;
            try {
                checkpoint = serializer.deserialize(state.getStates(), CompletedCheckpoint.class);
            } catch (Exception e) {
                throw new CheckpointStorageException(
                        String.format(
                                "Failed to deserialize checkpoint %d of job %s, pipeline %s",
                                state.getCheckpointId(), jobId, pipelineId),
                        e);
            }
            if (CheckpointRestoreUtils.matchesRestoreCheckpointType(
                            checkpoint.getCheckpointType(), restoreMode)
                    && (matched == null || state.getCheckpointId() > matched.getCheckpointId())) {
                matched = state;
            }
        }
        if (candidates.failures.isEmpty()) {
            return matched;
        }
        if (matched == null) {
            throw new CheckpointStorageException(
                    String.format(
                            "Job %s, pipeline %s has no valid checkpoint for restore mode %s, but "
                                    + "%d checkpoint(s) are corrupt and may include the requested "
                                    + "one, refusing to restore from scratch: %s",
                            jobId,
                            pipelineId,
                            restoreMode,
                            candidates.failures.size(),
                            candidates.failures));
        }
        if (restoreMode == RestoreMode.SAVEPOINT
                && candidates.highestCorruptCheckpointId > matched.getCheckpointId()) {
            throw new CheckpointStorageException(
                    String.format(
                            "Job %s, pipeline %s: checkpoint %d is corrupt and newer than "
                                    + "savepoint %d, it may be the requested savepoint, refusing to "
                                    + "restore an older one: %s",
                            jobId,
                            pipelineId,
                            candidates.highestCorruptCheckpointId,
                            matched.getCheckpointId(),
                            candidates.failures));
        }
        return matched;
    }

    /**
     * Returns the highest checkpoint id the pipeline has on storage in either format, whether or
     * not that checkpoint is readable, or {@code -1} if there is none or the storage does not
     * implement {@link ChunkSupportStorage}. A v2 checkpoint id is committed once its {@code
     * _metadata} exists and can never be written again, so new checkpoints must start above it,
     * also after restoring an older checkpoint past a corrupt one.
     */
    static long highestCommittedCheckpointId(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        if (!(storage instanceof ChunkSupportStorage)) {
            return -1;
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        long highest = -1;
        for (V1CheckpointFile file : chunkStorage.listV1Checkpoints(jobId, pipelineId)) {
            highest = Math.max(highest, file.getCheckpointId());
        }
        for (long checkpointId : chunkStorage.listCompletedCheckpointIds(jobId, pipelineId)) {
            highest = Math.max(highest, checkpointId);
        }
        return highest;
    }

    private static Candidates readAll(CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        Candidates candidates = new Candidates();
        if (!(storage instanceof ChunkSupportStorage)) {
            candidates.states.addAll(storage.getCheckpointsByJobIdAndPipelineId(jobId, pipelineId));
            return candidates;
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        for (V1CheckpointFile file : chunkStorage.listV1Checkpoints(jobId, pipelineId)) {
            PipelineState v1 = readV1(chunkStorage, file, candidates);
            if (v1 != null) {
                candidates.states.add(v1);
            }
        }
        for (long checkpointId : chunkStorage.listCompletedCheckpointIds(jobId, pipelineId)) {
            PipelineState v2 = readV2(chunkStorage, jobId, pipelineId, checkpointId, candidates);
            if (v2 != null) {
                candidates.states.add(v2);
            }
        }
        candidates.states.sort(Comparator.comparingLong(PipelineState::getCheckpointId));
        return candidates;
    }

    /** Reads a v1 candidate; returns {@code null} if it is corrupt, throws if it is unreadable. */
    private static PipelineState readV1(
            ChunkSupportStorage storage, V1CheckpointFile file, Candidates candidates)
            throws CheckpointStorageException {
        try {
            return storage.readV1Checkpoint(file);
        } catch (CorruptCheckpointException e) {
            candidates.corrupt(file.getCheckpointId(), "v1", e);
            log.warn(
                    "Skip corrupt v1 checkpoint {} of job {}, pipeline {}; "
                            + "falling back to an older checkpoint",
                    file.getLocation(),
                    file.getJobId(),
                    file.getPipelineId(),
                    e);
            return null;
        }
    }

    /**
     * Reads a v2 candidate and checks that its payload is the checkpoint of this job, pipeline and
     * {@code chk-<id>} directory; returns {@code null} if it is corrupt, throws if it is
     * unreadable.
     */
    private static PipelineState readV2(
            ChunkSupportStorage storage,
            String jobId,
            String pipelineId,
            long checkpointId,
            Candidates candidates)
            throws CheckpointStorageException {
        try {
            MetadataEnvelope envelope = storage.readMetadata(jobId, pipelineId, checkpointId);
            checkIdentity(envelope.getPayload(), jobId, pipelineId, checkpointId);
            return PipelineState.builder()
                    .jobId(jobId)
                    .pipelineId(Integer.parseInt(pipelineId))
                    .checkpointId(checkpointId)
                    .states(envelope.getPayload())
                    .build();
        } catch (CorruptCheckpointException e) {
            candidates.corrupt(checkpointId, "v2", e);
            log.warn(
                    "Skip corrupt checkpoint metadata of job {}, pipeline {}, checkpoint {}; "
                            + "falling back to an older checkpoint",
                    jobId,
                    pipelineId,
                    checkpointId,
                    e);
            return null;
        }
    }

    private static void checkIdentity(
            byte[] payload, String jobId, String pipelineId, long checkpointId)
            throws CorruptCheckpointException {
        CompletedCheckpoint checkpoint;
        try {
            checkpoint = new ProtoStuffSerializer().deserialize(payload, CompletedCheckpoint.class);
        } catch (Exception e) {
            throw new CorruptCheckpointException(
                    String.format(
                            "Checkpoint metadata of job %s, pipeline %s, checkpoint %d cannot be "
                                    + "deserialized",
                            jobId, pipelineId, checkpointId),
                    e);
        }
        if (checkpoint == null
                || checkpoint.getCheckpointId() != checkpointId
                || !String.valueOf(checkpoint.getPipelineId()).equals(pipelineId)
                || !String.valueOf(checkpoint.getJobId()).equals(jobId)) {
            throw new CorruptCheckpointException(
                    String.format(
                            "Checkpoint metadata in the directory of job %s, pipeline %s, "
                                    + "checkpoint %d holds checkpoint %s",
                            jobId,
                            pipelineId,
                            checkpointId,
                            checkpoint == null
                                    ? "null"
                                    : String.format(
                                            "%d of job %d, pipeline %d",
                                            checkpoint.getCheckpointId(),
                                            checkpoint.getJobId(),
                                            checkpoint.getPipelineId())));
        }
    }

    /** The valid and corrupt candidates of one read. */
    private static final class Candidates {

        private final List<PipelineState> states = new ArrayList<>();

        private final List<String> failures = new ArrayList<>();

        private long highestCorruptCheckpointId = -1;

        private void corrupt(long checkpointId, String format, CorruptCheckpointException e) {
            failures.add(format + " checkpoint " + checkpointId + ": " + e.getMessage());
            highestCorruptCheckpointId = Math.max(highestCorruptCheckpointId, checkpointId);
        }

        private void failIfNothingValid(String jobId, String pipelineId)
                throws CheckpointStorageException {
            if (!failures.isEmpty()) {
                throw new CheckpointStorageException(
                        String.format(
                                "Job %s, pipeline %s has %d completed checkpoint(s) but all are "
                                        + "corrupt, refusing to restore from scratch: %s",
                                jobId, pipelineId, failures.size(), failures));
            }
        }
    }
}
