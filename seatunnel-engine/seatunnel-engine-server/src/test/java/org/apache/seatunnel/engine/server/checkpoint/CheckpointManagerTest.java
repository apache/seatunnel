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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_STATE;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointManagerTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testFinishedPipelineShouldCleanupCounterAndCheckpointState() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);
        CounterStateStore<String> checkpointCounterStore =
                server.getEngineContext().getStateStores().checkpointCounterStore();
        String counterKey = StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1);
        checkpointCounterStore.set(counterKey, 2L);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, new CheckpointConfig());

        Assertions.assertEquals(
                2L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        checkpointManager.listenPipeline(1, PipelineStatus.FINISHED).join();
        Assertions.assertNull(checkpointCounterStore.get(counterKey));
        checkpointManager.clearCheckpointIfNeed(JobStatus.FINISHED);
        Assertions.assertTrue(checkpointStorage.getAllCheckpoints(jobId + "").isEmpty());
    }

    @Test
    public void testStartWithSavepointShouldResumeNextCheckpointIdFromStoredCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long restoredCheckpointId = 7L;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(
                checkpointStorage, jobId, restoredCheckpointId, CheckpointType.SAVEPOINT_TYPE);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        checkpointStorage,
                        new CheckpointConfig());

        long nextCheckpointId =
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get();

        Assertions.assertEquals(restoredCheckpointId + 1L, nextCheckpointId);
        Assertions.assertEquals(
                restoredCheckpointId + 1L,
                checkpointManager
                        .getCheckpointCoordinator(1)
                        .getCheckpointIdCounter()
                        .getAndIncrement());
        Assertions.assertEquals(
                restoredCheckpointId + 2L,
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testStartWithCheckpointShouldResumeNextCheckpointIdFromStoredCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long restoredCheckpointId = 7L;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(
                checkpointStorage,
                jobId,
                restoredCheckpointId,
                CheckpointType.COMPLETED_POINT_TYPE);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.CHECKPOINT,
                        jobId,
                        checkpointStorage,
                        new CheckpointConfig());

        long nextCheckpointId =
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get();

        Assertions.assertEquals(restoredCheckpointId + 1L, nextCheckpointId);
        Assertions.assertEquals(
                restoredCheckpointId + 1L,
                checkpointManager
                        .getCheckpointCoordinator(1)
                        .getCheckpointIdCounter()
                        .getAndIncrement());
        Assertions.assertEquals(
                restoredCheckpointId + 2L,
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldLoadLatestCheckpointOfOwnJob() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 7L);

        // A normally submitted job has no restore source; after a master switch it is
        // re-initialized with restart = true and must still continue from its own checkpoint.
        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.NONE,
                        null,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                8L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldPreferOwnLatestCheckpointOverSavepoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 3L, CheckpointType.SAVEPOINT_TYPE);
        // checkpoint files are ordered by their store timestamp (millis)
        Thread.sleep(5);
        storeCompletedCheckpoint(checkpointStorage, jobId, 7L);

        // Job started with -r and later hit a master switch: resume from checkpoint 7, not
        // from the savepoint it was originally restored from.
        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                8L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldFallBackToSourceJobWithoutOwnCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long sourceJobId = jobId + 1;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, sourceJobId, 5L);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.CHECKPOINT,
                        sourceJobId,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                6L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    private CheckpointStorage createCheckpointStorage() throws CheckpointStorageException {
        return FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CheckpointStorageFactory.class,
                        new CheckpointStorageConfig().getStorage())
                .create(new HashMap<>());
    }

    private void storeCompletedCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, long checkpointId)
            throws CheckpointStorageException {
        storeCompletedCheckpoint(
                checkpointStorage, jobId, checkpointId, CheckpointType.COMPLETED_POINT_TYPE);
    }

    private void storeCompletedCheckpoint(
            CheckpointStorage checkpointStorage,
            long jobId,
            long checkpointId,
            CheckpointType checkpointType)
            throws CheckpointStorageException {
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        1,
                        checkpointId,
                        Instant.now().toEpochMilli(),
                        checkpointType,
                        Instant.now().toEpochMilli(),
                        new HashMap<>(),
                        new HashMap<>());
        checkpointStorage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(jobId + "")
                        .pipelineId(1)
                        .checkpointId(checkpointId)
                        .states(new ProtoStuffSerializer().serialize(completedCheckpoint))
                        .build());
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isStartWithSavePoint,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isStartWithSavePoint,
                isStartWithSavePoint ? RestoreMode.SAVEPOINT : RestoreMode.NONE,
                isStartWithSavePoint ? jobId : null,
                checkpointStorage,
                checkpointConfig);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                false,
                checkpointStorage,
                checkpointConfig);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            boolean masterFailoverRestore,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        return new CheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                masterFailoverRestore,
                nodeEngine,
                null,
                planMap,
                checkpointConfig,
                checkpointStorage,
                instance.getExecutorService("test"),
                nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                server.getEngineContext(),
                null);
    }

    @Test
    public void testRetainCheckpointAfterCancelledWhenEnabled() throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        config.setRetainAfterJobCancelled(true);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Checkpoint should be retained on CANCELED when retainAfterJobCancelled=true
        checkpointManager.clearCheckpointIfNeed(JobStatus.CANCELED);
        Assertions.assertFalse(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be retained after cancel when retain-after-job-cancelled is enabled");
    }

    @Test
    public void testDeleteCheckpointAfterFinishedEvenWhenRetainEnabled()
            throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        config.setRetainAfterJobCancelled(true);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Checkpoint should still be deleted on FINISHED even when retainAfterJobCancelled=true
        checkpointManager.clearCheckpointIfNeed(JobStatus.FINISHED);
        Assertions.assertTrue(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be cleaned up after FINISHED regardless of retain setting");
    }

    @Test
    public void testDeleteCheckpointAfterCancelledWhenRetainDisabled()
            throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        // retainAfterJobCancelled defaults to false
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Default behavior: checkpoint should be deleted on CANCELED
        checkpointManager.clearCheckpointIfNeed(JobStatus.CANCELED);
        Assertions.assertTrue(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be cleaned up after cancel when retain is disabled (default)");
    }

    /**
     * Legacy plugins implement the original SPI only and must never trigger a payload-based
     * enumeration fallback when recovering retention bookkeeping.
     */
    @Test
    public void testLegacyStorageKeepsPriorRetentionWithoutPayloadFallback() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage storage = createCheckpointStorage();
        for (long checkpointId = 1; checkpointId <= 5; checkpointId++) {
            storeCompletedCheckpoint(storage, jobId, checkpointId);
        }
        // This adapter intentionally inherits the new optional method from the interface.
        CheckpointStorage legacyStorage =
                new CheckpointStorage() {
                    @Override
                    public String storeCheckPoint(PipelineState state)
                            throws CheckpointStorageException {
                        return storage.storeCheckPoint(state);
                    }

                    @Override
                    public void asyncStoreCheckPoint(PipelineState state)
                            throws CheckpointStorageException {
                        storage.asyncStoreCheckPoint(state);
                    }

                    @Override
                    public List<PipelineState> getAllCheckpoints(String id)
                            throws CheckpointStorageException {
                        return storage.getAllCheckpoints(id);
                    }

                    @Override
                    public List<PipelineState> getLatestCheckpoint(String id)
                            throws CheckpointStorageException {
                        return storage.getLatestCheckpoint(id);
                    }

                    @Override
                    public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                            String id, String pipeline) throws CheckpointStorageException {
                        return storage.getLatestCheckpointByJobIdAndPipelineId(id, pipeline);
                    }

                    @Override
                    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                            String id, String pipeline) {
                        throw new AssertionError(
                                "Retention must not read full checkpoint payloads");
                    }

                    @Override
                    public void deleteCheckpoint(String id) {
                        storage.deleteCheckpoint(id);
                    }

                    @Override
                    public PipelineState getCheckpoint(
                            String id, String pipeline, String checkpoint)
                            throws CheckpointStorageException {
                        return storage.getCheckpoint(id, pipeline, checkpoint);
                    }

                    @Override
                    public void deleteCheckpoint(String id, String pipeline, String checkpoint)
                            throws CheckpointStorageException {
                        storage.deleteCheckpoint(id, pipeline, checkpoint);
                    }

                    @Override
                    public void deleteCheckpoint(
                            String id, String pipeline, List<String> checkpoints)
                            throws CheckpointStorageException {
                        storage.deleteCheckpoint(id, pipeline, checkpoints);
                    }
                };
        instance.getMap(IMAP_RUNNING_JOB_STATE)
                .put("checkpoint_state_" + jobId + "_1", CheckpointCoordinatorStatus.RUNNING);
        CheckpointManager manager =
                createCheckpointManager(jobId, false, legacyStorage, new CheckpointConfig());
        manager.getCheckpointCoordinator(1)
                .completePendingCheckpoint(
                        new CompletedCheckpoint(
                                jobId,
                                1,
                                6L,
                                Instant.now().toEpochMilli(),
                                CheckpointType.CHECKPOINT_TYPE,
                                Instant.now().toEpochMilli(),
                                new HashMap<>(),
                                new HashMap<>()));
        List<Long> checkpointIds =
                storage.getCheckpointIdsByJobIdAndPipelineId(String.valueOf(jobId), "1").get()
                        .stream()
                        .sorted()
                        .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), checkpointIds);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCheckpointRetentionAfterCoordinatorRecreation(boolean explicitRestore)
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        for (long checkpointId = 1; checkpointId <= 5; checkpointId++) {
            storeCompletedCheckpoint(checkpointStorage, jobId, checkpointId);
        }

        CheckpointStorageConfig storageConfig = new CheckpointStorageConfig();
        storageConfig.setMaxRetainedCheckpoints(3);
        CheckpointConfig config = new CheckpointConfig();
        config.setStorage(storageConfig);
        if (!explicitRestore) {
            // Active-master reconstruction is also identified by persisted coordinator state.
            instance.getMap(IMAP_RUNNING_JOB_STATE)
                    .put("checkpoint_state_" + jobId + "_1", CheckpointCoordinatorStatus.RUNNING);
        }
        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        explicitRestore,
                        explicitRestore ? RestoreMode.CHECKPOINT : RestoreMode.NONE,
                        explicitRestore ? jobId : null,
                        checkpointStorage,
                        config);

        checkpointManager
                .getCheckpointCoordinator(1)
                .completePendingCheckpoint(
                        new CompletedCheckpoint(
                                jobId,
                                1,
                                6L,
                                Instant.now().toEpochMilli(),
                                CheckpointType.CHECKPOINT_TYPE,
                                Instant.now().toEpochMilli(),
                                new HashMap<>(),
                                new HashMap<>()));

        List<Long> retainedCheckpointIds =
                checkpointStorage.getCheckpointsByJobIdAndPipelineId(String.valueOf(jobId), "1")
                        .stream()
                        .map(PipelineState::getCheckpointId)
                        .sorted()
                        .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(4L, 5L, 6L), retainedCheckpointIds);
    }
}
