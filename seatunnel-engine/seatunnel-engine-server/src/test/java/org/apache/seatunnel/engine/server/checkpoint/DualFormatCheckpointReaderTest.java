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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CorruptCheckpointException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.checkpoint.storage.state.CheckpointFileLayout;
import org.apache.seatunnel.engine.checkpoint.storage.state.ChunkDescriptor;
import org.apache.seatunnel.engine.checkpoint.storage.state.MetadataEnvelope;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

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
        Assertions.assertNull(
                DualFormatCheckpointReader.getLatestCheckpointOfType(
                        storage, JOB_ID, PIPELINE_ID, RestoreMode.SAVEPOINT));
        Assertions.assertEquals(
                -1,
                DualFormatCheckpointReader.highestCommittedCheckpointId(
                        storage, JOB_ID, PIPELINE_ID));
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
        storeV2(4);

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
        storeV2(4);
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
        storeV2(6);
        storeV2(7);
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
    public void testAllCompletedCheckpointsCorruptFailsClosed(Backend backend) throws Exception {
        use(backend);
        storeV2(6);
        storeV2(7);
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
    public void testAllV1CorruptAndNoV2FailsClosed(Backend backend) throws Exception {
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
     * A newest v1 file that cannot be read at all (here: no read permission, an {@link
     * java.io.IOException}) must fail the restore even though an older checkpoint is readable: the
     * file itself may be intact, and restoring the older checkpoint would replay data. The v1
     * getters also swallow this failure, so the reader must not rely on them.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testV1FileThatCannotBeReadFailsTheRestore(Backend backend) throws Exception {
        use(backend);
        storeV1(2, "v1-2");
        storeV1(3, "v1-3");
        Path file = v1File(3);
        Set<PosixFilePermission> original = makeUnreadable(file);
        try {
            // the existing v1 getters swallow the read failure: local file storage reports no
            // latest checkpoint, the Hadoop storage skips the file when listing
            if (backend == Backend.LOCAL_FILE) {
                Assertions.assertNull(
                        storage.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID));
            } else {
                Assertions.assertEquals(
                        1, storage.getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID).size());
            }

            assertReadFailure(
                    () ->
                            DualFormatCheckpointReader.getLatestCheckpoint(
                                    storage, JOB_ID, PIPELINE_ID),
                    "v1");
            assertReadFailure(
                    () -> DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID),
                    "v1");
            assertReadFailure(
                    () ->
                            DualFormatCheckpointReader.getLatestCheckpointOfType(
                                    storage, JOB_ID, PIPELINE_ID, RestoreMode.CHECKPOINT),
                    "v1");
        } finally {
            Files.setPosixFilePermissions(file, original);
        }
        // once readable again, the newest checkpoint is used
        Assertions.assertEquals(
                "v1-3",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
    }

    /**
     * As for v1: a v2 metadata file that cannot be read fails the restore instead of rolling back.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testV2MetadataThatCannotBeReadFailsTheRestore(Backend backend) throws Exception {
        use(backend);
        storeV2(6);
        storeV2(7);
        Path metadata =
                namespace.resolve(CheckpointFileLayout.metadataPath(JOB_ID, PIPELINE_ID, 7));
        Set<PosixFilePermission> original = makeUnreadable(metadata);
        try {
            assertReadFailure(
                    () ->
                            DualFormatCheckpointReader.getLatestCheckpoint(
                                    storage, JOB_ID, PIPELINE_ID),
                    "metadata");
            assertReadFailure(
                    () -> DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID),
                    "metadata");
        } finally {
            Files.setPosixFilePermissions(metadata, original);
        }
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testCorruptV1DoesNotHideValidV2(Backend backend) throws Exception {
        use(backend);
        storeV2(4);
        storeV1(5, "v1-5");
        corruptV1(5);

        Assertions.assertEquals(
                "v2-4",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));
        Assertions.assertEquals(
                Collections.singletonList("v2-4"),
                texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
    }

    /**
     * A valid {@code _metadata} copied into another checkpoint's or pipeline's directory keeps a
     * valid CRC, but its payload belongs elsewhere; it is skipped like corrupt content.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testV2MetadataOfAnotherCheckpointIsSkipped(Backend backend) throws Exception {
        use(backend);
        storeV2(3);
        copyMetadata(JOB_ID, PIPELINE_ID, 3, JOB_ID, PIPELINE_ID, 9);
        storage.writeMetadata(
                JOB_ID, "2", 10, MetadataEnvelope.of(payload(JOB_ID, 2, 10, CHECKPOINT)));
        copyMetadata(JOB_ID, "2", 10, JOB_ID, PIPELINE_ID, 10);
        storage.writeMetadata(
                "200", PIPELINE_ID, 11, MetadataEnvelope.of(payload("200", 1, 11, CHECKPOINT)));
        copyMetadata("200", PIPELINE_ID, 11, JOB_ID, PIPELINE_ID, 11);
        Assertions.assertEquals(
                Arrays.asList(3L, 9L, 10L, 11L),
                storage.listCompletedCheckpointIds(JOB_ID, PIPELINE_ID));

        PipelineState latest =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals(3, latest.getCheckpointId());
        Assertions.assertEquals("v2-3", text(latest));
        Assertions.assertEquals(
                Collections.singletonList("v2-3"),
                texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
    }

    /**
     * Typed restore: the type of a corrupt candidate is unknown, so it may be the requested
     * savepoint. A savepoint restore must not silently start from scratch or from an older one.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testTypedRestoreWithCorruptCandidatesFailsClosed(Backend backend) throws Exception {
        use(backend);
        storeV1Typed(3, CHECKPOINT);
        storeV1Typed(4, CHECKPOINT);
        storeV1Typed(5, CHECKPOINT);
        storeV1Typed(6, SAVEPOINT);
        corruptV1(6);

        // nothing matches but a candidate is corrupt: fail instead of starting from scratch
        CheckpointStorageException noSavepoint =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpointOfType(
                                        storage, JOB_ID, PIPELINE_ID, RestoreMode.SAVEPOINT));
        Assertions.assertTrue(noSavepoint.getMessage().contains("v1 checkpoint 6"));
        // a checkpoint restore falls back like the latest-checkpoint path
        Assertions.assertEquals(
                5,
                DualFormatCheckpointReader.getLatestCheckpointOfType(
                                storage, JOB_ID, PIPELINE_ID, RestoreMode.CHECKPOINT)
                        .getCheckpointId());

        // an older valid savepoint is not used while a newer candidate is corrupt
        storeV1Typed(2, SAVEPOINT);
        CheckpointStorageException olderSavepoint =
                Assertions.assertThrows(
                        CheckpointStorageException.class,
                        () ->
                                DualFormatCheckpointReader.getLatestCheckpointOfType(
                                        storage, JOB_ID, PIPELINE_ID, RestoreMode.SAVEPOINT));
        Assertions.assertTrue(olderSavepoint.getMessage().contains("savepoint 2"));

        // a valid savepoint newer than every corrupt candidate is used
        storeV1Typed(7, SAVEPOINT);
        Assertions.assertEquals(
                7,
                DualFormatCheckpointReader.getLatestCheckpointOfType(
                                storage, JOB_ID, PIPELINE_ID, RestoreMode.SAVEPOINT)
                        .getCheckpointId());
    }

    /**
     * After restoring past a corrupt committed checkpoint, that id stays committed (create-once),
     * so the next checkpoint id must start above it.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testNextCheckpointIdStartsAboveCorruptCommittedIds(Backend backend)
            throws Exception {
        use(backend);
        storeV2(6);
        storeV2(7);
        corruptMetadata(7);

        PipelineState restored =
                DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals(6, restored.getCheckpointId());
        long highest =
                DualFormatCheckpointReader.highestCommittedCheckpointId(
                        storage, JOB_ID, PIPELINE_ID);
        Assertions.assertEquals(7, highest);
        // restored id + 1 would be refused
        Assertions.assertThrows(
                CheckpointStorageException.class,
                () ->
                        storage.writeMetadata(
                                JOB_ID,
                                PIPELINE_ID,
                                7,
                                MetadataEnvelope.of(payload(JOB_ID, 1, 7, CHECKPOINT))));

        // the id CheckpointManager continues with is free
        long next = Math.max(restored.getCheckpointId(), highest) + 1;
        storage.writeChunk(
                new ChunkDescriptor(JOB_ID, PIPELINE_ID, next, "action", 0),
                Collections.singletonList("state".getBytes(StandardCharsets.UTF_8)));
        storeV2(next);
        Assertions.assertEquals(
                "v2-8",
                text(DualFormatCheckpointReader.getLatestCheckpoint(storage, JOB_ID, PIPELINE_ID)));

        // v1 ids count as well
        storeV1(12, "v1-12");
        corruptV1(12);
        Assertions.assertEquals(
                12,
                DualFormatCheckpointReader.highestCommittedCheckpointId(
                        storage, JOB_ID, PIPELINE_ID));
    }

    @ParameterizedTest
    @EnumSource(Backend.class)
    public void testGetCheckpointsMergesBothFormatsAndSkipsCorrupted(Backend backend)
            throws Exception {
        use(backend);
        storeV1(1, "v1-1");
        storeV2(2);
        storeV2(3);
        corruptMetadata(3);

        Assertions.assertEquals(
                Arrays.asList("v1-1", "v2-2"),
                texts(DualFormatCheckpointReader.getCheckpoints(storage, JOB_ID, PIPELINE_ID)));
    }

    @Test
    public void testStorageWithoutChunkSupportOnlyUsesV1() throws Exception {
        CheckpointStorage legacy = Mockito.mock(CheckpointStorage.class);
        PipelineState v1 =
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(1)
                        .checkpointId(3)
                        .states(payload(JOB_ID, 1, 3, SAVEPOINT))
                        .build();
        Mockito.when(legacy.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(v1);
        Mockito.when(legacy.getCheckpointsByJobIdAndPipelineId(JOB_ID, PIPELINE_ID))
                .thenReturn(Collections.singletonList(v1));

        Assertions.assertSame(
                v1, DualFormatCheckpointReader.getLatestCheckpoint(legacy, JOB_ID, PIPELINE_ID));
        Assertions.assertEquals(
                1, DualFormatCheckpointReader.getCheckpoints(legacy, JOB_ID, PIPELINE_ID).size());
        Assertions.assertSame(
                v1,
                DualFormatCheckpointReader.getLatestCheckpointOfType(
                        legacy, JOB_ID, PIPELINE_ID, RestoreMode.SAVEPOINT));
        Assertions.assertEquals(
                -1,
                DualFormatCheckpointReader.highestCommittedCheckpointId(
                        legacy, JOB_ID, PIPELINE_ID));
    }

    private static final CheckpointType CHECKPOINT = CheckpointType.CHECKPOINT_TYPE;
    private static final CheckpointType SAVEPOINT = CheckpointType.SAVEPOINT_TYPE;

    private interface Read {
        Object run() throws Exception;
    }

    /** Asserts a read failure: not a corrupt-content failure, and no fallback was returned. */
    private static void assertReadFailure(Read read, String expectedMessagePart) {
        CheckpointStorageException failure =
                Assertions.assertThrows(CheckpointStorageException.class, read::run);
        Assertions.assertFalse(failure instanceof CorruptCheckpointException, failure.toString());
        Assertions.assertTrue(
                failure.getMessage().contains(expectedMessagePart), failure.getMessage());
    }

    /**
     * Removes all permissions of a file; skips the test where POSIX permissions are not supported
     * (e.g. Windows) or not enforced (privileged users). Returns the original permissions.
     */
    private static Set<PosixFilePermission> makeUnreadable(Path file) throws Exception {
        // checked before any POSIX call: e.g. the Windows default file system has no POSIX view
        Assumptions.assumeTrue(
                Files.getFileStore(file).supportsFileAttributeView(PosixFileAttributeView.class),
                "the file system does not support POSIX permissions");
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(file);
        Files.setPosixFilePermissions(file, Collections.emptySet());
        if (Files.isReadable(file)) {
            Files.setPosixFilePermissions(file, original);
            Assumptions.abort("file permissions are not enforced");
        }
        return original;
    }

    private void storeV1(long checkpointId, String payload) throws Exception {
        storeV1Raw(checkpointId, payload.getBytes(StandardCharsets.UTF_8));
    }

    private void storeV1Typed(long checkpointId, CheckpointType type) throws Exception {
        storeV1Raw(checkpointId, payload(JOB_ID, 1, checkpointId, type));
    }

    /**
     * Stores a v1 checkpoint file. Its name carries the write time in milliseconds, and the
     * existing v1 getters pick the "latest" file by that time alone, keeping whichever file the
     * directory listing returns first on a tie. Real checkpoints are seconds apart; to give test
     * files distinct times as well, this returns only once the clock has moved past the write.
     */
    private void storeV1Raw(long checkpointId, byte[] states) throws Exception {
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(Integer.parseInt(PIPELINE_ID))
                        .checkpointId(checkpointId)
                        .states(states)
                        .build());
        // read after the write: at or after the time in the file name
        long writtenAt = System.currentTimeMillis();
        while (System.currentTimeMillis() <= writtenAt) {
            Thread.yield();
        }
    }

    /** Commits a v2 checkpoint whose payload is a real serialized {@link CompletedCheckpoint}. */
    private void storeV2(long checkpointId) throws Exception {
        storage.writeMetadata(
                JOB_ID,
                PIPELINE_ID,
                checkpointId,
                MetadataEnvelope.of(payload(JOB_ID, 1, checkpointId, CHECKPOINT)));
    }

    private static byte[] payload(
            String jobId, int pipelineId, long checkpointId, CheckpointType type) {
        return new ProtoStuffSerializer()
                .serialize(
                        new CompletedCheckpoint(
                                Long.parseLong(jobId),
                                pipelineId,
                                checkpointId,
                                1L,
                                type,
                                2L,
                                new HashMap<>(),
                                new HashMap<>()));
    }

    private void copyMetadata(
            String fromJob,
            String fromPipeline,
            long fromId,
            String toJob,
            String toPipeline,
            long toId)
            throws Exception {
        Path target = namespace.resolve(CheckpointFileLayout.metadataPath(toJob, toPipeline, toId));
        Files.createDirectories(target.getParent());
        Files.copy(
                namespace.resolve(CheckpointFileLayout.metadataPath(fromJob, fromPipeline, fromId)),
                target);
    }

    /** Flips the last byte of a committed {@code _metadata}, so its CRC check fails. */
    private void corruptMetadata(long checkpointId) throws Exception {
        Path metadata =
                namespace.resolve(
                        CheckpointFileLayout.metadataPath(JOB_ID, PIPELINE_ID, checkpointId));
        byte[] bytes = Files.readAllBytes(metadata);
        bytes[bytes.length - 1] ^= 0x01;
        writeContent(metadata, bytes);
    }

    /** Overwrites the v1 file of a checkpoint with bytes that are not a checkpoint. */
    private void corruptV1(long checkpointId) throws Exception {
        writeContent(v1File(checkpointId), "not a checkpoint".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Replaces a file's content as a faulty writer would. The Hadoop local file system keeps a
     * {@code .<name>.crc} side file and would report a changed file as an I/O error ({@code
     * ChecksumException}); it is removed so the corrupt content itself is read and validated.
     */
    private static void writeContent(Path file, byte[] content) throws Exception {
        Files.write(file, content);
        Files.deleteIfExists(file.resolveSibling("." + file.getFileName() + ".crc"));
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

    /**
     * Labels a restored state: v1 test payloads are plain text ({@code v1-<id>}), v2 payloads are
     * serialized checkpoints ({@code v2-<id>}).
     */
    private static String text(PipelineState state) {
        String text = new String(state.getStates(), StandardCharsets.UTF_8);
        if (text.startsWith("v1-")) {
            return text;
        }
        CompletedCheckpoint checkpoint =
                new ProtoStuffSerializer()
                        .deserialize(state.getStates(), CompletedCheckpoint.class);
        return "v2-" + checkpoint.getCheckpointId();
    }
}
