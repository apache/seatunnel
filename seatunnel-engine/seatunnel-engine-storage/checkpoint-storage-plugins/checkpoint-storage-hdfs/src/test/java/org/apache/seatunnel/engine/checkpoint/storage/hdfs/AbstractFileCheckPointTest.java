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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CorruptCheckpointException;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkCodec;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractFileCheckPointTest {

    protected static HdfsStorage STORAGE;
    protected static final String JOB_ID = "chris";

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
    public void testListCompletedV2CheckpointIds() throws CheckpointStorageException {
        Assertions.assertEquals(
                Arrays.asList(5L, 9L), STORAGE.listCompletedCheckpointIds(JOB_ID, "1"));
        Assertions.assertTrue(STORAGE.listCompletedCheckpointIds(JOB_ID, "404").isEmpty());
    }

    @Test
    public void testV2ChunkRoundTrip() throws Exception {
        List<byte[]> states = Arrays.asList(bytes("split-a"), null, bytes("split-bb"));
        ChunkDescriptor descriptor = new ChunkDescriptor(JOB_ID, "3", 1, "action", 0);

        FileChunkStateHandle first = STORAGE.writeChunk(descriptor, states);
        // a retried write never replaces a chunk: every attempt gets its own file
        FileChunkStateHandle handle = STORAGE.writeChunk(descriptor, states);
        Assertions.assertNotEquals(first.getLocation(), handle.getLocation());

        Assertions.assertEquals(15, handle.getStateSize());
        Assertions.assertEquals(
                Collections.emptyList(),
                tmpFilesIn(CheckpointFileLayout.checkpointDir(JOB_ID, "3", 1)));
        List<byte[]> restored = STORAGE.readChunk(handle);
        Assertions.assertArrayEquals(bytes("split-a"), restored.get(0));
        Assertions.assertNull(restored.get(1));
        Assertions.assertArrayEquals(bytes("split-bb"), restored.get(2));
    }

    @Test
    public void testChunkContentAndLocationAreValidated() throws Exception {
        FileChunkStateHandle handle =
                STORAGE.writeChunk(
                        new ChunkDescriptor(JOB_ID, "6", 4, "action", 0),
                        Arrays.asList(bytes("state-a")));

        // a replaced chunk with valid framing is rejected by the CRC bound in the handle
        writeRaw(handle.getLocation(), ChunkCodec.encode(Arrays.asList(bytes("state-b"))));
        Assertions.assertThrows(CorruptCheckpointException.class, () -> STORAGE.readChunk(handle));

        // a location outside the layout is never read
        Assertions.assertThrows(
                CorruptCheckpointException.class,
                () ->
                        STORAGE.readChunk(
                                new FileChunkStateHandle(
                                        "../../outside.chunk",
                                        handle.getStateSize(),
                                        handle.getEncodedLength(),
                                        handle.getChecksum())));
    }

    @Test
    public void testV2MetadataCorruptionDetection() throws Exception {
        STORAGE.writeMetadata(JOB_ID, "4", 2, MetadataEnvelope.of(bytes("payload")));
        Assertions.assertArrayEquals(
                bytes("payload"), STORAGE.readMetadata(JOB_ID, "4", 2).getPayload());

        byte[] encoded = MetadataEnvelope.of(bytes("payload")).encode();
        writeRaw(
                CheckpointFileLayout.metadataPath(JOB_ID, "4", 3),
                Arrays.copyOf(encoded, encoded.length - 2));
        Assertions.assertThrows(
                CorruptCheckpointException.class, () -> STORAGE.readMetadata(JOB_ID, "4", 3));
    }

    @Test
    public void testCommittedMetadataIsNeverReplaced() throws Exception {
        FileChunkStateHandle chunk =
                STORAGE.writeChunk(
                        new ChunkDescriptor(JOB_ID, "5", 6, "action", 0),
                        Arrays.asList(bytes("committed-state")));
        STORAGE.writeMetadata(JOB_ID, "5", 6, MetadataEnvelope.of(bytes("committed")));

        // a second commit of the same checkpoint fails and leaves the committed file untouched
        CheckpointStorageException failure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                STORAGE.writeMetadata(
                                        JOB_ID, "5", 6, MetadataEnvelope.of(bytes("other"))));
        Assertions.assertTrue(failure.getMessage().contains("already committed"));
        Assertions.assertArrayEquals(
                bytes("committed"), STORAGE.readMetadata(JOB_ID, "5", 6).getPayload());

        // a late chunk write lands in its own file; the committed chunk is untouched
        FileChunkStateHandle late =
                STORAGE.writeChunk(
                        new ChunkDescriptor(JOB_ID, "5", 6, "action", 0),
                        Arrays.asList(bytes("late")));
        Assertions.assertNotEquals(chunk.getLocation(), late.getLocation());
        Assertions.assertArrayEquals(bytes("committed-state"), STORAGE.readChunk(chunk).get(0));
        Assertions.assertEquals(
                Collections.emptyList(),
                tmpFilesIn(CheckpointFileLayout.checkpointDir(JOB_ID, "5", 6)));
    }

    /** Overwrites a file through the Hadoop file system, keeping its checksum file consistent. */
    private static void writeRaw(String relativePath, byte[] data) throws IOException {
        try (FSDataOutputStream out =
                STORAGE.fs.create(
                        new Path(STORAGE.getStorageParentDirectory() + relativePath), true)) {
            out.write(data);
        }
    }

    private static List<String> tmpFilesIn(String relativeDir) throws IOException {
        List<String> names = new ArrayList<>();
        for (FileStatus status :
                STORAGE.fs.listStatus(
                        new Path(STORAGE.getStorageParentDirectory() + relativeDir))) {
            String name = status.getPath().getName();
            if (name.endsWith(CheckpointFileLayout.TMP_SUFFIX)) {
                names.add(name);
            }
        }
        return names;
    }

    protected static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }

    /**
     * init storage data
     *
     * @throws CheckpointStorageException exception if init failed
     */
    protected static void initStorageData() throws CheckpointStorageException {
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

        // v2 (STIP-45) checkpoints of pipeline 1 next to the v1 files: the v1 assertions must not
        // see them. chk-11 has a chunk but no _metadata, i.e. it never completed.
        STORAGE.writeMetadata(JOB_ID, "1", 5, MetadataEnvelope.of(bytes("chk-5")));
        STORAGE.writeMetadata(JOB_ID, "1", 9, MetadataEnvelope.of(bytes("chk-9")));
        STORAGE.writeChunk(
                new ChunkDescriptor(JOB_ID, "1", 11, "action", 0), Arrays.asList(bytes("orphan")));
    }
}
