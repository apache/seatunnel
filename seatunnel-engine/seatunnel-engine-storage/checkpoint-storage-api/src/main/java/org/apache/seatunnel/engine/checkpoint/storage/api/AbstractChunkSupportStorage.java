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

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkCodec;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.InlineStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for file-system based storages that support the v2 checkpoint format. It implements
 * the format and layout logic once; subclasses only provide raw file operations on paths relative
 * to the storage namespace.
 */
public abstract class AbstractChunkSupportStorage extends AbstractCheckpointStorage
        implements ChunkSupportStorage {

    /**
     * Writes a file so that readers never observe a partial file: write {@code path + ".tmp"}, then
     * rename it to {@code path}, replacing an existing file. Missing parent directories are
     * created.
     *
     * @param relativePath path relative to the storage namespace
     */
    protected abstract void writeFileAtomically(String relativePath, byte[] data)
            throws IOException;

    /** Reads a whole file; throws if it does not exist. */
    protected abstract byte[] readFile(String relativePath) throws IOException;

    /** Returns whether a regular file exists at the path. */
    protected abstract boolean fileExists(String relativePath) throws IOException;

    /** Lists the names of the direct child directories, or an empty list if the path is absent. */
    protected abstract List<String> listChildDirectories(String relativePath) throws IOException;

    @Override
    public FileChunkStateHandle writeChunk(ChunkDescriptor descriptor, List<byte[]> states)
            throws CheckpointStorageException {
        String location = CheckpointFileLayout.chunkPath(descriptor);
        try {
            writeFileAtomically(location, ChunkCodec.encode(states));
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
            writeFileAtomically(location, envelope.encode());
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
}
