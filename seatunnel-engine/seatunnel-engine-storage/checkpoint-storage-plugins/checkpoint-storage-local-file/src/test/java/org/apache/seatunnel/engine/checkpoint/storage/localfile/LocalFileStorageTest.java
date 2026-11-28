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

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CorruptCheckpointException;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkCodec;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.FileChunkStateHandle;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
public class LocalFileStorageTest {

    private static LocalFileStorage STORAGE = new LocalFileStorage(null);
    private static final String JOB_ID = "chris";

    @BeforeAll
    public static void setup() throws CheckpointStorageException {
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

        // v2 (STIP-45) checkpoints of pipeline 1 next to the v1 files: the v1 assertions below
        // must not see them. chk-11 has a chunk but no _metadata, i.e. it never completed.
        STORAGE.writeMetadata(JOB_ID, "1", 5, MetadataEnvelope.of(bytes("chk-5")));
        STORAGE.writeMetadata(JOB_ID, "1", 9, MetadataEnvelope.of(bytes("chk-9")));
        STORAGE.writeChunk(
                new ChunkDescriptor(JOB_ID, "1", 11, "action", 0), Arrays.asList(bytes("orphan")));
    }

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
        Assertions.assertTrue(STORAGE.listCompletedCheckpointIds("no-such-job", "1").isEmpty());
    }

    @Test
    public void testV2ChunkRoundTrip() throws Exception {
        List<byte[]> states = Arrays.asList(bytes("split-a"), null, bytes("split-bb"));
        ChunkDescriptor descriptor = new ChunkDescriptor(JOB_ID, "3", 1, "action", -1);

        FileChunkStateHandle first = STORAGE.writeChunk(descriptor, Arrays.asList(bytes("old")));
        // a retried write never replaces a chunk: every attempt gets its own file
        FileChunkStateHandle handle = STORAGE.writeChunk(descriptor, states);
        Assertions.assertNotEquals(first.getLocation(), handle.getLocation());
        Assertions.assertArrayEquals(bytes("old"), STORAGE.readChunk(first).get(0));

        Assertions.assertTrue(handle.getLocation().startsWith(JOB_ID + "/3/chk-1/subtask-"));
        Assertions.assertEquals(15, handle.getStateSize());
        Assertions.assertEquals(ChunkCodec.encode(states).length, handle.getEncodedLength());
        Assertions.assertEquals(
                Collections.emptyList(),
                tmpFilesIn(CheckpointFileLayout.checkpointDir(JOB_ID, "3", 1)));
        List<byte[]> restored = STORAGE.readChunk(handle);
        Assertions.assertArrayEquals(bytes("split-a"), restored.get(0));
        Assertions.assertNull(restored.get(1));
        Assertions.assertArrayEquals(bytes("split-bb"), restored.get(2));

        // a chunk that cannot be read is a read failure, not corrupt content
        String missing =
                CheckpointFileLayout.chunkPath(descriptor, CheckpointFileLayout.newAttemptId());
        CheckpointStorageException readFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> STORAGE.readChunk(new FileChunkStateHandle(missing, 1, 8, 0)));
        Assertions.assertFalse(readFailure instanceof CorruptCheckpointException);
    }

    @Test
    public void testChunkContentIsBoundToItsHandle() throws Exception {
        ChunkDescriptor descriptor = new ChunkDescriptor(JOB_ID, "6", 4, "action", 0);
        FileChunkStateHandle handle =
                STORAGE.writeChunk(descriptor, Arrays.asList(bytes("state-a")));

        // the decoded size must match the handle
        Assertions.assertThrows(
                CorruptCheckpointException.class,
                () ->
                        STORAGE.readChunk(
                                new FileChunkStateHandle(
                                        handle.getLocation(),
                                        999,
                                        handle.getEncodedLength(),
                                        handle.getChecksum())));

        // a replaced chunk with valid framing and the same length is rejected by the CRC
        writeRaw(handle.getLocation(), ChunkCodec.encode(Arrays.asList(bytes("state-b"))));
        CorruptCheckpointException replaced =
                Assertions.assertThrows(
                        CorruptCheckpointException.class, () -> STORAGE.readChunk(handle));
        Assertions.assertTrue(replaced.getMessage().contains("CRC"), replaced.getMessage());

        // a truncated chunk is rejected by its length
        writeRaw(handle.getLocation(), new byte[] {0, 0, 0, 1});
        Assertions.assertThrows(CorruptCheckpointException.class, () -> STORAGE.readChunk(handle));
    }

    @Test
    public void testChunkLocationOutsideTheLayoutIsNeverRead() throws Exception {
        // a file outside the namespace and a non-chunk file inside it
        FileChunkStateHandle valid =
                STORAGE.writeChunk(
                        new ChunkDescriptor(JOB_ID, "7", 1, "action", 0),
                        Arrays.asList(bytes("x")));
        for (String location :
                Arrays.asList(
                        "../../outside.chunk",
                        JOB_ID + "/7/chk-1/../../../../outside.chunk",
                        CheckpointFileLayout.metadataPath(JOB_ID, "7", 1))) {
            CorruptCheckpointException failure =
                    Assertions.assertThrows(
                            CorruptCheckpointException.class,
                            () ->
                                    STORAGE.readChunk(
                                            new FileChunkStateHandle(
                                                    location,
                                                    valid.getStateSize(),
                                                    valid.getEncodedLength(),
                                                    valid.getChecksum())),
                            location);
            Assertions.assertTrue(failure.getMessage().contains(location), failure.getMessage());
        }
    }

    @Test
    public void testV2MetadataRoundTripAndCorruptionDetection() throws Exception {
        STORAGE.writeMetadata(JOB_ID, "4", 2, MetadataEnvelope.of(bytes("payload")));
        Assertions.assertArrayEquals(
                bytes("payload"), STORAGE.readMetadata(JOB_ID, "4", 2).getPayload());

        // simulate a partially written _metadata on a store without atomic rename
        byte[] encoded = MetadataEnvelope.of(bytes("payload")).encode();
        writeRaw(
                CheckpointFileLayout.metadataPath(JOB_ID, "4", 3),
                Arrays.copyOf(encoded, encoded.length - 2));
        Assertions.assertEquals(
                Arrays.asList(2L, 3L), STORAGE.listCompletedCheckpointIds(JOB_ID, "4"));
        Assertions.assertThrows(
                CorruptCheckpointException.class, () -> STORAGE.readMetadata(JOB_ID, "4", 3));
        CheckpointStorageException missing =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () -> STORAGE.readMetadata(JOB_ID, "4", 99));
        Assertions.assertFalse(missing instanceof CorruptCheckpointException);
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

        // a late chunk write of an earlier attempt lands in its own file and cannot touch the
        // chunk the committed metadata references
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

    /** A failing temporary-file cleanup must never replace the outcome of a publication. */
    @Test
    public void testTemporaryFileCleanupFailureDoesNotHideThePublicationOutcome(
            @TempDir Path namespace) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("namespace", namespace.toString());
        LocalFileStorage failingCleanup =
                new LocalFileStorage(config) {
                    @Override
                    protected void deleteIfExists(String relativePath) throws IOException {
                        throw new IOException("cleanup failed for " + relativePath);
                    }
                };

        // success path: no cleanup is needed, the commit is reported as done
        failingCleanup.writeMetadata(JOB_ID, "1", 1, MetadataEnvelope.of(bytes("first")));
        Assertions.assertArrayEquals(
                bytes("first"), failingCleanup.readMetadata(JOB_ID, "1", 1).getPayload());

        // failure path: "already committed" survives a failing cleanup, which is suppressed
        CheckpointStorageException failure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                failingCleanup.writeMetadata(
                                        JOB_ID, "1", 1, MetadataEnvelope.of(bytes("second"))));
        Assertions.assertTrue(failure.getMessage().contains("already committed"));
        Assertions.assertInstanceOf(FileAlreadyExistsException.class, failure.getCause());
        Assertions.assertEquals(1, failure.getCause().getSuppressed().length);
        Assertions.assertTrue(
                failure.getCause().getSuppressed()[0].getMessage().contains("cleanup failed"));
        Assertions.assertArrayEquals(
                bytes("first"), failingCleanup.readMetadata(JOB_ID, "1", 1).getPayload());
    }

    private static void writeRaw(String relativePath, byte[] data) throws IOException {
        Path file = Paths.get(STORAGE.getStorageParentDirectory(), relativePath);
        Files.createDirectories(file.getParent());
        Files.write(file, data);
    }

    private static List<String> tmpFilesIn(String relativeDir) throws IOException {
        try (Stream<Path> files =
                Files.list(Paths.get(STORAGE.getStorageParentDirectory(), relativeDir))) {
            return files.map(f -> f.getFileName().toString())
                    .filter(name -> name.endsWith(CheckpointFileLayout.TMP_SUFFIX))
                    .collect(Collectors.toList());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
    }
}
