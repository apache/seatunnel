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
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkCodec;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.InlineStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;
import org.apache.seatunnel.engine.checkpoint.storage.state.V1CheckpointFile;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Base class for file-system based storages that support the v2 checkpoint format. It implements
 * the format and layout logic once; subclasses only provide raw file operations on paths relative
 * to the storage namespace.
 */
public abstract class AbstractChunkSupportStorage extends AbstractCheckpointStorage
        implements ChunkSupportStorage {

    /**
     * Publishes a file so that readers never observe a partial file: the data is written to a
     * temporary file unique to this attempt (ending in {@code .tmp}) in the target directory, which
     * is then moved into place. Missing parent directories are created.
     *
     * <ul>
     *   <li>{@code replace == false} (create once): the move must fail with {@link
     *       java.nio.file.FileAlreadyExistsException} if the target exists, and an existing target
     *       must never be deleted or modified, not even when the publication fails.
     *   <li>{@code replace == true}: an existing target is replaced. Only used for files that are
     *       not part of a committed checkpoint, so a short window without the target is harmless.
     * </ul>
     *
     * <p>The temporary file is removed when the publication fails.
     *
     * @param relativePath path relative to the storage namespace
     */
    protected abstract void publishFile(String relativePath, byte[] data, boolean replace)
            throws IOException;

    /** Reads a whole file; throws if it does not exist. */
    protected abstract byte[] readFile(String relativePath) throws IOException;

    /** Returns a temporary path for one publication attempt of {@code relativePath}. */
    protected static String attemptTmpPath(String relativePath) {
        return relativePath + "." + UUID.randomUUID() + CheckpointFileLayout.TMP_SUFFIX;
    }

    /** Returns whether a regular file exists at the path. */
    protected abstract boolean fileExists(String relativePath) throws IOException;

    /** Lists the names of the regular files directly in a directory, or an empty list if absent. */
    protected abstract List<String> listFileNames(String relativePath) throws IOException;

    /** Lists the names of the direct child directories, or an empty list if the path is absent. */
    protected abstract List<String> listChildDirectories(String relativePath) throws IOException;

    @Override
    public FileChunkStateHandle writeChunk(ChunkDescriptor descriptor, List<byte[]> states)
            throws CheckpointStorageException {
        String location = CheckpointFileLayout.chunkPath(descriptor);
        String metadata =
                CheckpointFileLayout.metadataPath(
                        descriptor.getJobId(),
                        descriptor.getPipelineId(),
                        descriptor.getCheckpointId());
        try {
            // a committed checkpoint is immutable; chunks of an uncommitted one may be rewritten,
            // e.g. when chk-N is attempted again after restoring from checkpoint N-1
            if (fileExists(metadata)) {
                throw new CheckpointStorageException(
                        String.format(
                                "Refusing to write checkpoint chunk %s: checkpoint %d of job %s, "
                                        + "pipeline %s is already committed",
                                location,
                                descriptor.getCheckpointId(),
                                descriptor.getJobId(),
                                descriptor.getPipelineId()));
            }
            publishFile(location, ChunkCodec.encode(states), true);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Failed to write checkpoint chunk %s (job %s, pipeline %s, checkpoint %d)",
                            location,
                            descriptor.getJobId(),
                            descriptor.getPipelineId(),
                            descriptor.getCheckpointId()),
                    e);
        }
        return new FileChunkStateHandle(location, InlineStateHandle.sizeOf(states));
    }

    @Override
    public List<byte[]> readChunk(FileChunkStateHandle handle) throws CheckpointStorageException {
        String location = handle.getLocation();
        byte[] data;
        try {
            data = readFile(location);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to read checkpoint chunk %s", location), e);
        }
        return ChunkCodec.decode(data, location);
    }

    @Override
    public void writeMetadata(
            String jobId, String pipelineId, long checkpointId, MetadataEnvelope envelope)
            throws CheckpointStorageException {
        String location = CheckpointFileLayout.metadataPath(jobId, pipelineId, checkpointId);
        try {
            // the commit point is written exactly once and never replaced
            publishFile(location, envelope.encode(), false);
        } catch (FileAlreadyExistsException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Checkpoint %d of job %s, pipeline %s is already committed (%s exists)",
                            checkpointId, jobId, pipelineId, location),
                    e);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to write checkpoint metadata %s", location), e);
        }
    }

    @Override
    public MetadataEnvelope readMetadata(String jobId, String pipelineId, long checkpointId)
            throws CheckpointStorageException {
        String location = CheckpointFileLayout.metadataPath(jobId, pipelineId, checkpointId);
        byte[] data;
        try {
            data = readFile(location);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to read checkpoint metadata %s", location), e);
        }
        return MetadataEnvelope.decode(data, location);
    }

    @Override
    public List<Long> listCompletedCheckpointIds(String jobId, String pipelineId)
            throws CheckpointStorageException {
        String pipelineDir = CheckpointFileLayout.pipelineDir(jobId, pipelineId);
        try {
            List<Long> ids = new ArrayList<>();
            for (String dirName : listChildDirectories(pipelineDir)) {
                long checkpointId = CheckpointFileLayout.parseCheckpointId(dirName);
                if (checkpointId >= 0
                        && fileExists(
                                CheckpointFileLayout.metadataPath(
                                        jobId, pipelineId, checkpointId))) {
                    ids.add(checkpointId);
                }
            }
            Collections.sort(ids);
            return ids;
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to list v2 checkpoints under %s", pipelineDir), e);
        }
    }

    @Override
    public List<V1CheckpointFile> listV1Checkpoints(String jobId, String pipelineId)
            throws CheckpointStorageException {
        List<String> names;
        try {
            names = listFileNames(jobId);
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to list the v1 checkpoints of job %s", jobId), e);
        }
        List<V1CheckpointFile> files = new ArrayList<>();
        for (String name : names) {
            V1CheckpointFile file = parseV1FileName(jobId, name);
            if (file != null && file.getPipelineId().equals(pipelineId)) {
                files.add(file);
            }
        }
        files.sort(
                Comparator.comparingLong(V1CheckpointFile::getWriteTimestamp)
                        .thenComparingLong(V1CheckpointFile::getCheckpointId)
                        .reversed());
        return files;
    }

    @Override
    public PipelineState readV1Checkpoint(V1CheckpointFile file) throws CheckpointStorageException {
        PipelineState state;
        try {
            state = deserializeCheckPointData(readFile(file.getLocation()));
        } catch (IOException | RuntimeException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Failed to read v1 checkpoint %s (job %s, pipeline %s, checkpoint %d)",
                            file.getLocation(),
                            file.getJobId(),
                            file.getPipelineId(),
                            file.getCheckpointId()),
                    e);
        }
        // a truncated or foreign file can deserialize without an error into an incomplete state
        if (state == null
                || state.getStates() == null
                || state.getCheckpointId() != file.getCheckpointId()
                || !String.valueOf(state.getPipelineId()).equals(file.getPipelineId())) {
            throw new CheckpointStorageException(
                    String.format(
                            "v1 checkpoint %s does not contain checkpoint %d of job %s, pipeline %s",
                            file.getLocation(),
                            file.getCheckpointId(),
                            file.getJobId(),
                            file.getPipelineId()));
        }
        return state;
    }

    /**
     * Parses {@code <writeTimestamp>-<random>-<pipelineId>-<checkpointId>.ser}; returns {@code
     * null} for any other file name.
     */
    private static V1CheckpointFile parseV1FileName(String jobId, String fileName) {
        String suffix = "." + FILE_FORMAT;
        if (!fileName.endsWith(suffix)) {
            return null;
        }
        String[] segments =
                fileName.substring(0, fileName.length() - suffix.length()).split(FILE_NAME_SPLIT);
        if (segments.length != 4) {
            return null;
        }
        try {
            return new V1CheckpointFile(
                    jobId + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName,
                    jobId,
                    segments[FILE_NAME_PIPELINE_ID_INDEX],
                    Long.parseLong(segments[FILE_NAME_CHECKPOINT_ID_INDEX]),
                    Long.parseLong(segments[FILE_SORT_ID_INDEX]));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
