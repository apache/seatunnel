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

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;
import org.apache.seatunnel.engine.checkpoint.storage.state.V1CheckpointFile;

import java.util.List;

/**
 * Optional extension of {@link CheckpointStorage} for the v2 checkpoint format (STIP-45): state is
 * written as per-subtask chunk files and a checkpoint is committed by a small {@code _metadata}
 * file. See {@link org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout} for
 * the directory layout.
 *
 * <p>Storage plugins that do not implement this interface keep working with the v1 format only.
 * Implementations must keep every method of {@link CheckpointStorage} restricted to v1 files, so
 * existing callers observe no change.
 */
public interface ChunkSupportStorage extends CheckpointStorage {

    /**
     * Writes the state segments of one subtask action into its chunk file. Readers never observe a
     * partially written chunk. A chunk of an uncommitted checkpoint (no {@code _metadata} yet) may
     * be written again, which replaces it; this happens when a checkpoint id is attempted again
     * after a restore. Chunks of a committed checkpoint are immutable.
     *
     * @param descriptor identifies the chunk, its location is derived from it
     * @param states the state segments, elements may be {@code null}
     * @return a handle referencing the written chunk
     * @throws CheckpointStorageException if the chunk could not be written, or the checkpoint is
     *     already committed
     */
    FileChunkStateHandle writeChunk(ChunkDescriptor descriptor, List<byte[]> states)
            throws CheckpointStorageException;

    /**
     * Reads the state segments referenced by a chunk handle.
     *
     * @param handle a handle returned by {@link #writeChunk}
     * @return the state segments, in the order they were written
     * @throws CheckpointStorageException if the chunk is missing or malformed
     */
    List<byte[]> readChunk(FileChunkStateHandle handle) throws CheckpointStorageException;

    /**
     * Writes the {@code _metadata} file of a checkpoint. Its existence is the single criterion that
     * the checkpoint completed, so it must be written after all chunks it references.
     *
     * <p>The metadata is written exactly once: if it already exists the call fails and the existing
     * file is left untouched, also when the write itself fails half-way. Readers never observe a
     * missing or partial metadata file of a committed checkpoint.
     *
     * @throws CheckpointStorageException if the metadata could not be written or already exists
     */
    void writeMetadata(
            String jobId, String pipelineId, long checkpointId, MetadataEnvelope envelope)
            throws CheckpointStorageException;

    /**
     * Reads and validates (magic, version, length and CRC) the {@code _metadata} file of a
     * checkpoint.
     *
     * @throws CheckpointStorageException if the file is missing or fails validation
     */
    MetadataEnvelope readMetadata(String jobId, String pipelineId, long checkpointId)
            throws CheckpointStorageException;

    /**
     * Lists the ids of the v2 checkpoints of a pipeline that have a {@code _metadata} file, in
     * ascending order. Directories without metadata (incomplete checkpoints) are not listed. The
     * metadata content is not validated here.
     *
     * @return completed checkpoint ids, ascending; empty if there are none
     * @throws CheckpointStorageException if the pipeline directory could not be listed
     */
    List<Long> listCompletedCheckpointIds(String jobId, String pipelineId)
            throws CheckpointStorageException;

    /**
     * Lists the v1 checkpoint files of a pipeline from their names only, newest first by the write
     * timestamp in the name (the order {@link #getLatestCheckpointByJobIdAndPipelineId} uses).
     * Files are not read, so an unreadable file is still listed.
     *
     * @return the v1 checkpoint files, or an empty list if there are none
     * @throws CheckpointStorageException if the job directory cannot be listed
     */
    List<V1CheckpointFile> listV1Checkpoints(String jobId, String pipelineId)
            throws CheckpointStorageException;

    /**
     * Reads one v1 checkpoint file. Unlike the {@link CheckpointStorage} getters, which log and
     * skip unreadable files, every read or deserialization failure is thrown, so restore can tell
     * an unreadable checkpoint apart from a missing one.
     *
     * @throws CheckpointStorageException if the file cannot be read or does not contain the
     *     expected checkpoint
     */
    PipelineState readV1Checkpoint(V1CheckpointFile file) throws CheckpointStorageException;
}
