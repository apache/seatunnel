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
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractChunkSupportStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.ChunkSupportStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Restore-time selection between v1 blobs and v2 {@code _metadata} checkpoints (STIP-45). */
public class DualFormatCheckpointReaderTest {

    private static final String JOB_ID = "100";
    private static final String PIPELINE_ID = "1";

    @TempDir Path namespace;

    private AbstractChunkSupportStorage storage;

    /** The two storage plugins implementing {@link ChunkSupportStorage}. */
    enum Backend {
        LOCAL_FILE,
        HADOOP_LOCAL_FS
    }

    private void use(Backend backend) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("namespace", namespace.toString());
        if (backend == Backend.LOCAL_FILE) {
            storage = new LocalFileStorage(config);
        } else {
            // as LocalFileCheckPointTest: the Hadoop local file system needs winutils on Windows
            Assumptions.assumeFalse(
                    OS.WINDOWS.isCurrentOs(), "the Hadoop local file system needs winutils");
            config.put("fs.defaultFS", "file:///");
            config.put("disable.cache", "true");
            storage = new HdfsStorage(config);
        }
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testNoCheckpointReturnsNull(Backend backend) throws Exception {
        use(backend);
        Assertions.assertNull(
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(
                DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID).isEmpty());
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testV1OnlyIsRestoredAsBefore(Backend backend) throws Exception {
        use(backend);
        storeV1(3, "v1-3");

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);

        Assertions.assertEquals(3, latest.getCheckpointId());
        Assertions.assertEquals("v1-3", text(latest));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testNewerV2WinsAndCarriesTheSamePayloadShape(Backend backend) throws Exception {
        use(backend);
        storeV1(3, "v1-3");
        storeV2(4, "v2-4");

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);

        Assertions.assertEquals(4, latest.getCheckpointId());
        Assertions.assertEquals(JOB_ID, latest.getJobId());
        Assertions.assertEquals(1, latest.getPipelineId());
        Assertions.assertEquals("v2-4", text(latest));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testNewerV1WinsOverOlderV2(Backend backend) throws Exception {
        use(backend);
        storeV2(4, "v2-4");
        storeV1(5, "v1-5");

        Assertions.assertEquals(
                "v1-5",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testCorruptedLatestMetadataFallsBackToOlderCheckpoint(Backend backend)
            throws Exception {
        use(backend);
        storeV1(2, "v1-2");
        storeV2(6, "v2-6");
        storeV2(7, "v2-7");
        corruptMetadata(7);

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals("v2-6", text(latest));

        corruptMetadata(6);
        latest = DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals("v1-2", text(latest));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testIncompleteV2CheckpointIsIgnored(Backend backend) throws Exception {
        use(backend);
        storeV1(2, "v1-2");
        // a chk dir without _metadata: the master crashed before committing it
        Files.createDirectories(
                namespace.resolve(CheckpointFileLayout.checkpointDir(JOB_ID, PIPELINE_ID, 9)));

        Assertions.assertEquals(
                "v1-2",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testOnlyIncompleteV2CheckpointsMeansNoCheckpoint(Backend backend) throws Exception {
        use(backend);
        // never committed: nothing to restore, which is not a failure
        Files.createDirectories(
                namespace.resolve(CheckpointFileLayout.checkpointDir(JOB_ID, PIPELINE_ID, 9)));

        Assertions.assertNull(
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testAllCompletedCheckpointsUnreadableFailsClosed(Backend backend) throws Exception {
        use(backend);
        storeV2(6, "v2-6");
        storeV2(7, "v2-7");
        corruptMetadata(6);
        corruptMetadata(7);

        // returning null here would restart the job from its initial position
        CheckpointStorageException latestFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpoint(
                                        storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("Job " + JOB_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("pipeline " + PIPELINE_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("v2 checkpoint 7"));
        Assertions.assertTrue(latestFailure.getMessage().contains("v2 checkpoint 6"));

        Assertions.assertThrows(
                CheckpointStorageException.class,
                () -> DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testCorruptLatestV1FallsBackToOlderV1(Backend backend) throws Exception {
        use(backend);
        storeV1(2, "v1-2");
        storeV1(3, "v1-3");
        corruptV1(3);

        Assertions.assertEquals(
                "v1-2",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
        Assertions.assertEquals(
                Collections.singletonList("v1-2"),
                texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testAllV1UnreadableAndNoV2FailsClosed(Backend backend) throws Exception {
        use(backend);
        storeV1(3, "v1-3");
        corruptV1(3);

        CheckpointStorageException latestFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpoint(
                                        storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(latestFailure.getMessage().contains("v1 checkpoint 3"));
        Assertions.assertTrue(latestFailure.getMessage().contains("Job " + JOB_ID));
        CheckpointStorageException listFailure =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getCheckpoints(
                                        storage, JOB_ID, PIPELINE_ID));
        Assertions.assertTrue(listFailure.getMessage().contains("v1 checkpoint 3"));
    }

    /**
     * A v1 file that cannot be read at all: the v1 getters catch the {@link java.io.IOException},
     * log it and report it as absent, so the reader must not rely on them.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testV1FileThatCannotBeReadFailsClosed(Backend backend) throws Exception {
        use(backend);
        storeV1(3, "v1-3");
        Path file = v1File(3);
        // checked before any POSIX call: e.g. the Windows default file system has no POSIX view
        Assumptions.assumeTrue(
                Files.getFileStore(file).supportsFileAttributeView(PosixFileAttributeView.class),
                "the file system does not support POSIX permissions");
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(file);
        Files.setPosixFilePermissions(file, Collections.emptySet());
        try {
            // privileged users (e.g. root) can still read the file
            Assumptions.assumeFalse(Files.isReadable(file), "file permissions are not enforced");
            // the existing v1 getters swallow the read failure: local file storage reports no
            // latest
            // checkpoint, the Hadoop storage skips the file when listing
            if (backend == Backend.LOCAL_FILE) {
                Assertions.assertNull(
                        storage.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID));
            } else {
                Assertions.assertTrue(
                        storage.getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID).isEmpty());
            }

            CheckpointStorageException latestFailure =
                    Assertions.assertThrows(
                            CheckpointStorageException.class,
                            () ->
                                    DualFormatCheckpointReader.getLatestCheckpoint(
                                            storage, JOB_ID, PIPELINE_ID));
            Assertions.assertTrue(latestFailure.getMessage().contains("v1 checkpoint 3"));
            Assertions.assertThrows(
                    CheckpointStorageException.class,
                    () -> DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID));

            // a readable older checkpoint is still used
            storeV1(2, "v1-2");
            Assertions.assertEquals(
                    "v1-2",
                    text(
                            DualFormatCheckpointReader.getLatestCheckpoint(
                                    storage, JOB_ID, PIPELINE_ID)));
            Assertions.assertEquals(
                    Collections.singletonList("v1-2"),
                    texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
        } finally {
            Files.setPosixFilePermissions(file, original);
        }
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testUnreadableV1DoesNotHideReadableV2(Backend backend) throws Exception {
        use(backend);
        storeV2(4, "v2-4");
        storeV1(5, "v1-5");
        corruptV1(5);

        Assertions.assertEquals(
                "v2-4",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
        Assertions.assertEquals(
                Collections.singletonList("v2-4"),
                texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testGetCheckpointsMergesBothFormatsAndSkipsCorrupted(Backend backend)
            throws Exception {
        use(backend);
        storeV1(1, "v1-1");
        storeV2(2, "v2-2");
        storeV2(3, "v2-3");
        corruptMetadata(3);

        List<String> payloads =
                DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID).stream()
                        .map(DualFormatCheckpointReaderTest::text)
                        .sorted()
                        .collect(Collectors.toList());

        Assertions.assertEquals(Arrays.asList("v1-1", "v2-2"), payloads);
    }

    @Test
    public void testStorageWithoutChunkSupportOnlyUsesV1() throws Exception {
        CheckpointStorage legacy = Mockito.mock(CheckpointStorage.class);
        PipelineState v1 =
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(1)
                        .checkpointId(3)
                        .states(new byte[0])
                        .build();
        Mockito.when(legacy.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(v1);
        Mockito.when(legacy.getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(Collections.singletonList(v1));

        Assertions.assertSame(
                v1, DualFormatCheckpointReader.getLatestCheckpoint(legacy, JOB_ID, PIPELINE_ID));
        Assertions.assertEquals(
                1, DualFormatCheckpointReader.getCheckpoints(legacy, JOB_ID, PIPELINE_ID).size());
    }

    private void storeV1(long checkpointId, String payload) throws Exception {
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(Integer.parseInt(PIPELINE_ID))
                        .checkpointId(checkpointId)
                        .states(payload.getBytes(StandardCharsets.UTF_8))
                        .build());
    }

    private void storeV2(long checkpointId, String payload) throws Exception {
        storage.writeMetadata(
                JOB_ID,
                PIPELINE_ID,
                checkpointId,
                MetadataEnvelope.of(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private void corruptMetadata(long checkpointId) throws Exception {
        Path metadata =
                namespace.resolve(
                        CheckpointFileLayout.metadataPath(JOB_ID, PIPELINE_ID, checkpointId));
        byte[] bytes = Files.readAllBytes(metadata);
        bytes[bytes.length - 1] ^= 0x01;
        Files.write(metadata, bytes);
    }

    /** Overwrites the v1 file of a checkpoint with bytes that are not a checkpoint. */
    private void corruptV1(long checkpointId) throws Exception {
        Files.write(v1File(checkpointId), "not a checkpoint".getBytes(StandardCharsets.UTF_8));
    }

    private Path v1File(long checkpointId) throws Exception {
        String suffix = "-" + PIPELINE_ID + "-" + checkpointId + ".ser";
        List<Path> files;
        try (Stream<Path> stream = Files.list(namespace.resolve(JOB_ID))) {
            files =
                    stream.filter(f -> f.getFileName().toString().endsWith(suffix))
                            .collect(Collectors.toList());
        }
        Assertions.assertEquals(1, files.size(), "v1 file of checkpoint " + checkpointId);
        return files.get(0);
    }

    private static List<String> texts(List<PipelineState> states) {
        return states.stream()
                .map(DualFormatCheckpointReaderTest::text)
                .sorted()
                .collect(Collectors.toList());
    }

    private static String text(PipelineState state) {
        return new String(state.getStates(), StandardCharsets.UTF_8);
    }
}
