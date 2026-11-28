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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointEntryConsumer;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.CheckpointService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

/**
 * Unit coverage for the type-aware checkpoint cleanup that protects savepoints from the automatic
 * terminal-state cleanup.
 *
 * <p>Savepoints and periodic checkpoints share one directory per job id, and the checkpoint type
 * only lives inside the serialized {@link CompletedCheckpoint}, so the cleanup has to read every
 * stored entry before it may delete anything. The end-to-end behaviour is covered by {@code
 * SavepointSurvivesRestoredJobCancelIT}; these tests pin the classification itself, including the
 * cases the E2E test cannot create on demand (no savepoint at all, an entry whose type cannot be
 * decoded, a savepoint the listing fails to report, and a history that is too long to load at
 * once).
 */
class CheckpointStorageCleanupTest {

    private static final long JOB_ID = 424242L;

    private static final long SAVEPOINT_CHECKPOINT_ID = 4096L;

    private static final ProtoStuffSerializer SERIALIZER = new ProtoStuffSerializer();

    @TempDir Path tempDir;

    @Test
    void testKeepsSavepointsAndDeletesOnlyPeriodicCheckpoints() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.SAVEPOINT_TYPE);
        storeCheckpoint(storage, 1, 3, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 2, 1, CheckpointType.CHECKPOINT_TYPE);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertEquals(
                new TreeSet<>(Collections.singleton("1:2:SAVEPOINT_TYPE")),
                describe(storage.getAllCheckpoints(String.valueOf(JOB_ID))),
                "the savepoint must survive while every periodic checkpoint is deleted");
    }

    @Test
    void testDeletesEverythingWhenNoSavepointIsStored() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.CHECKPOINT_TYPE);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertTrue(
                storage.getAllCheckpoints(String.valueOf(JOB_ID)).isEmpty(),
                "without a savepoint the cleanup keeps its previous behaviour and removes the job"
                        + " data");
    }

    @Test
    void testKeepsAllCheckpointDataWhenAnEntryCannotBeClassified() throws Exception {
        CheckpointStorage storage = createStorage();
        storeCheckpoint(storage, 1, 1, CheckpointType.CHECKPOINT_TYPE);
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(JOB_ID))
                        .pipelineId(1)
                        .checkpointId(2)
                        .states("not-a-serialized-checkpoint".getBytes(StandardCharsets.UTF_8))
                        .build());

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("1:1:CHECKPOINT_TYPE", "1:2:UNKNOWN")),
                describe(storage.getAllCheckpoints(String.valueOf(JOB_ID))),
                "an entry whose type cannot be decoded must never be deleted, because it could be"
                        + " a savepoint");
    }

    /**
     * A savepoint the listing does not report must survive the cleanup: an omitted entry may be an
     * unreadable savepoint, so the cleanup may only delete entries it positively classified as
     * automatic checkpoints and must never remove the job directory as a whole.
     */
    @Test
    void testSavepointOmittedFromTheListingSurvivesCleanup() throws Exception {
        LocalFileStorage storage = createStorage();
        Path savepointFile = storeCheckpoint(storage, 1, 1, CheckpointType.SAVEPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.CHECKPOINT_TYPE);

        // A storage plugin that loses an unreadable entry from its listing and predates the
        // bounded visitor: it reproduces "the listing says there is no savepoint".
        CheckpointStorage lossyStorage = new LossyCheckpointStorage(storage, 1, 1L);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(
                lossyStorage, String.valueOf(JOB_ID));

        Assertions.assertTrue(
                Files.exists(savepointFile),
                "a savepoint missing from the storage listing must not be deleted");
        Assertions.assertEquals(
                Collections.singletonList(savepointFile),
                storedCheckpointFiles(),
                "only the periodic checkpoint may be deleted, the unreported savepoint must stay");
        Assertions.assertEquals(
                new TreeSet<>(Collections.singleton("1:1:SAVEPOINT_TYPE")),
                describe(storage.getAllCheckpoints(String.valueOf(JOB_ID))),
                "the classified periodic checkpoint must still be deleted");
    }

    /**
     * The same protection against a real unreadable entry: the storage lists the savepoint file but
     * cannot read it, so it never reaches the classifier. Requires a non-root user to be able to
     * drop the read permission; the test is skipped when the permission cannot be removed.
     */
    @Test
    @DisabledOnOs(WINDOWS)
    void testUnreadableSavepointSurvivesCleanup() throws Exception {
        CheckpointStorage storage = createStorage();
        Path savepointFile = storeCheckpoint(storage, 1, 1, CheckpointType.SAVEPOINT_TYPE);
        storeCheckpoint(storage, 1, 2, CheckpointType.CHECKPOINT_TYPE);
        storeCheckpoint(storage, 1, 3, CheckpointType.CHECKPOINT_TYPE);

        Files.setPosixFilePermissions(savepointFile, PosixFilePermissions.fromString("---------"));
        Assumptions.assumeFalse(
                Files.isReadable(savepointFile),
                "the savepoint file stays readable (running as root?), skipping the unreadable case");
        try {
            CheckpointService.deleteCheckpointDataKeepingSavepoints(
                    storage, String.valueOf(JOB_ID));

            Assertions.assertTrue(
                    Files.exists(savepointFile),
                    "an unreadable savepoint must survive automatic checkpoint cleanup");
            Assertions.assertEquals(
                    Collections.singletonList(savepointFile),
                    storedCheckpointFiles(),
                    "only the readable periodic checkpoints may be deleted");
        } finally {
            if (Files.exists(savepointFile)) {
                Files.setPosixFilePermissions(
                        savepointFile, PosixFilePermissions.fromString("rw-------"));
            }
        }
    }

    /**
     * A restored job can accumulate a long savepoint history under one job id. The cleanup must
     * visit it through the bounded visitor instead of loading the whole history, or the master can
     * run out of heap during an ordinary terminal cleanup. This test pins that the cleanup never
     * materializes the history and visits every entry exactly once; that the streaming storage
     * implementation really keeps one payload in memory is pinned by {@code
     * LocalFileStorageTest#testForEachCheckpointDecodesOneEntryAtATime}.
     */
    @Test
    void testCleanupVisitsALargeHistoryWithoutMaterializingIt() throws Exception {
        int automaticCheckpoints = 1024;
        StreamingCheckpointStorage storage = new StreamingCheckpointStorage(automaticCheckpoints);

        CheckpointService.deleteCheckpointDataKeepingSavepoints(storage, String.valueOf(JOB_ID));

        Assertions.assertEquals(
                1, storage.visits, "the cleanup must use the bounded checkpoint visitor");
        Assertions.assertEquals(
                0,
                storage.materializingListings,
                "the cleanup must not load the whole checkpoint history into memory");
        Assertions.assertEquals(
                automaticCheckpoints + 1,
                storage.visitedEntries,
                "every stored entry must be visited exactly once");
        Assertions.assertEquals(
                Collections.singletonMap("1", automaticCheckpointIds(automaticCheckpoints)),
                storage.deletedCheckpointIds,
                "every automatic checkpoint must be deleted and the savepoint must be kept");
    }

    private LocalFileStorage createStorage() {
        return new LocalFileStorage(
                Collections.singletonMap(StorageConstants.STORAGE_NAME_SPACE, tempDir.toString()));
    }

    private static Set<String> automaticCheckpointIds(int automaticCheckpoints) {
        Set<String> ids = new HashSet<>();
        for (long checkpointId = 1; checkpointId <= automaticCheckpoints; checkpointId++) {
            ids.add(String.valueOf(checkpointId));
        }
        return ids;
    }

    private Path storeCheckpoint(
            CheckpointStorage storage, int pipelineId, long checkpointId, CheckpointType type)
            throws Exception {
        return Paths.get(storeCheckpointState(storage, pipelineId, checkpointId, type));
    }

    private static String storeCheckpointState(
            CheckpointStorage storage, int pipelineId, long checkpointId, CheckpointType type)
            throws Exception {
        return storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(JOB_ID))
                        .pipelineId(pipelineId)
                        .checkpointId(checkpointId)
                        .states(
                                SERIALIZER.serialize(
                                        completedCheckpoint(pipelineId, checkpointId, type)))
                        .build());
    }

    private static CompletedCheckpoint completedCheckpoint(
            int pipelineId, long checkpointId, CheckpointType type) {
        long now = System.currentTimeMillis();
        return new CompletedCheckpoint(
                JOB_ID,
                pipelineId,
                checkpointId,
                now,
                type,
                now,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    /** Lists the checkpoint files that physically exist for the job, bypassing the storage API. */
    private List<Path> storedCheckpointFiles() throws Exception {
        Path jobDirectory = tempDir.resolve(String.valueOf(JOB_ID));
        if (!Files.isDirectory(jobDirectory)) {
            return Collections.emptyList();
        }
        try (Stream<Path> files = Files.list(jobDirectory)) {
            return files.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
    }

    private static TreeSet<String> describe(List<PipelineState> pipelineStates) {
        TreeSet<String> descriptions = new TreeSet<>();
        for (PipelineState pipelineState : pipelineStates) {
            String type = "UNKNOWN";
            try {
                CompletedCheckpoint checkpoint =
                        SERIALIZER.deserialize(
                                pipelineState.getStates(), CompletedCheckpoint.class);
                if (checkpoint.getCheckpointType() != null) {
                    type = checkpoint.getCheckpointType().name();
                }
            } catch (Exception ignored) {
                // Keep UNKNOWN so the assertion also covers undecodable entries.
            }
            descriptions.add(
                    pipelineState.getPipelineId()
                            + ":"
                            + pipelineState.getCheckpointId()
                            + ":"
                            + type);
        }
        return descriptions;
    }

    /**
     * Checkpoint storage that loses one entry from its listing, like a storage plugin that skips a
     * file it cannot read. It deliberately does not override {@link
     * CheckpointStorage#forEachCheckpoint(String, CheckpointEntryConsumer)}, so the default visitor
     * falls back to that lossy listing exactly like a plugin written before the bounded visitor.
     *
     * <p>Deleting the job namespace as a whole fails the test instead of silently destroying the
     * entries the listing never reported.
     */
    private static final class LossyCheckpointStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private final int hiddenPipelineId;
        private final long hiddenCheckpointId;

        private LossyCheckpointStorage(
                CheckpointStorage delegate, int hiddenPipelineId, long hiddenCheckpointId) {
            this.delegate = delegate;
            this.hiddenPipelineId = hiddenPipelineId;
            this.hiddenCheckpointId = hiddenCheckpointId;
        }

        @Override
        public List<PipelineState> getAllCheckpoints(String jobId)
                throws CheckpointStorageException {
            List<PipelineState> visible = new ArrayList<>();
            for (PipelineState pipelineState : delegate.getAllCheckpoints(jobId)) {
                if (pipelineState.getPipelineId() == hiddenPipelineId
                        && pipelineState.getCheckpointId() == hiddenCheckpointId) {
                    continue;
                }
                visible.add(pipelineState);
            }
            return visible;
        }

        @Override
        public void deleteCheckpoint(String jobId) {
            throw new AssertionError(
                    "the cleanup must not delete the job checkpoint directory as a whole");
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIds)
                throws CheckpointStorageException {
            delegate.deleteCheckpoint(jobId, pipelineId, checkpointIds);
        }

        @Override
        public String storeCheckPoint(PipelineState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void asyncStoreCheckPoint(PipelineState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<PipelineState> getLatestCheckpoint(String jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Checkpoint storage that only offers the bounded visitor: it produces its entries on the fly
     * and fails when the cleanup tries to load the whole history through {@link
     * #getAllCheckpoints(String)} or wipes the job namespace.
     */
    private static final class StreamingCheckpointStorage implements CheckpointStorage {
        private final int automaticCheckpoints;
        private final Map<String, Set<String>> deletedCheckpointIds = new HashMap<>();
        private int visits;
        private int materializingListings;
        private int visitedEntries;

        private StreamingCheckpointStorage(int automaticCheckpoints) {
            this.automaticCheckpoints = automaticCheckpoints;
        }

        @Override
        public void forEachCheckpoint(String jobId, CheckpointEntryConsumer consumer)
                throws CheckpointStorageException {
            visits++;
            for (long checkpointId = 1; checkpointId <= automaticCheckpoints; checkpointId++) {
                visitedEntries++;
                consumer.accept(storedCheckpoint(1, checkpointId, CheckpointType.CHECKPOINT_TYPE));
            }
            visitedEntries++;
            consumer.accept(
                    storedCheckpoint(2, SAVEPOINT_CHECKPOINT_ID, CheckpointType.SAVEPOINT_TYPE));
        }

        @Override
        public List<PipelineState> getAllCheckpoints(String jobId) {
            materializingListings++;
            throw new AssertionError(
                    "the cleanup must not load the whole checkpoint history into memory");
        }

        @Override
        public void deleteCheckpoint(String jobId) {
            throw new AssertionError(
                    "the cleanup must not delete the job checkpoint directory as a whole");
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIds)
                throws CheckpointStorageException {
            Set<String> deleted =
                    deletedCheckpointIds.computeIfAbsent(pipelineId, id -> new HashSet<>());
            Assertions.assertTrue(
                    Collections.disjoint(deleted, checkpointIds),
                    "a checkpoint must not be deleted twice");
            deleted.addAll(checkpointIds);
        }

        @Override
        public String storeCheckPoint(PipelineState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void asyncStoreCheckPoint(PipelineState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<PipelineState> getLatestCheckpoint(String jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw new UnsupportedOperationException();
        }

        private static PipelineState storedCheckpoint(
                int pipelineId, long checkpointId, CheckpointType type)
                throws CheckpointStorageException {
            try {
                return PipelineState.builder()
                        .jobId(String.valueOf(JOB_ID))
                        .pipelineId(pipelineId)
                        .checkpointId(checkpointId)
                        .states(
                                SERIALIZER.serialize(
                                        completedCheckpoint(pipelineId, checkpointId, type)))
                        .build();
            } catch (Exception e) {
                throw new CheckpointStorageException("Failed to build a stored checkpoint", e);
            }
        }
    }
}
