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
 * <p>Both are returned as {@link PipelineState}, so the restore path downstream is identical. A v2
 * metadata file that fails validation (e.g. a partial write on an object store) is skipped with a
 * warning and the next older checkpoint is used instead.
 */
@Slf4j
final class DualFormatCheckpointReader {

    private DualFormatCheckpointReader() {}

    /**
     * Returns the restorable checkpoint with the highest checkpoint id across both formats, or
     * {@code null} if there is none. On an id tie the v2 checkpoint wins.
     */
    static PipelineState getLatestCheckpoint(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        PipelineState v1 = storage.getLatestCheckpointByJobIdAndPipelineId(jobId, pipelineId);
        if (!(storage instanceof ChunkSupportStorage)) {
            return v1;
        }
        ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
        List<Long> ids = chunkStorage.listCompletedCheckpointIds(jobId, pipelineId);
        for (int i = ids.size() - 1; i >= 0; i--) {
            long checkpointId = ids.get(i);
            if (v1 != null && v1.getCheckpointId() > checkpointId) {
                return v1;
            }
            PipelineState v2 = readV2(chunkStorage, jobId, pipelineId, checkpointId);
            if (v2 != null) {
                return v2;
            }
        }
        return v1;
    }

    /** Returns every restorable checkpoint of the pipeline in both formats, in no fixed order. */
    static List<PipelineState> getCheckpoints(
            CheckpointStorage storage, String jobId, String pipelineId)
            throws CheckpointStorageException {
        List<PipelineState> states =
                new ArrayList<>(storage.getCheckpointsByJobIdAndPipelineId(jobId, pipelineId));
        if (storage instanceof ChunkSupportStorage) {
            ChunkSupportStorage chunkStorage = (ChunkSupportStorage) storage;
            for (long checkpointId : chunkStorage.listCompletedCheckpointIds(jobId, pipelineId)) {
                PipelineState v2 = readV2(chunkStorage, jobId, pipelineId, checkpointId);
                if (v2 != null) {
                    states.add(v2);
                }
            }
        }
        return states;
    }

    private static PipelineState readV2(
            ChunkSupportStorage storage, String jobId, String pipelineId, long checkpointId) {
        MetadataEnvelope envelope;
        try {
            envelope = storage.readMetadata(jobId, pipelineId, checkpointId);
        } catch (CheckpointStorageException e) {
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
