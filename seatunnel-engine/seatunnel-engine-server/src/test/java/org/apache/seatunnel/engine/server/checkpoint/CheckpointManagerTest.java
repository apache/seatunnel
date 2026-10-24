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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageUnavailableException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.hazelcast.HazelcastCounterStateStore;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.StandaloneResourceManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.hazelcast.map.IMap;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_STATE;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointManagerTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testRetiredApplicationInitializerCannotRewindSuccessorCounter() throws Exception {
        assertRetiredInitializerCannotRewindSuccessorCounter(true, false);
    }

    @Test
    public void testRetiredStandaloneTakeoverCannotRewindSuccessorCounter() throws Exception {
        assertRetiredInitializerCannotRewindSuccessorCounter(false, false);
    }

    @Test
    public void testRetiredStartAfterTerminalCleanupCannotPublishOrAllocate() throws Exception {
        assertRetiredInitializerCannotRewindSuccessorCounter(true, true);
    }

    @Test
    public void testRetiredStandaloneStartAfterTerminalCleanupCannotPublishOrAllocate()
            throws Exception {
        assertRetiredInitializerCannotRewindSuccessorCounter(false, true);
    }

    @Test
    public void testRetiredApplicationConstructorPreservesTerminalCheckpointStatus()
            throws Exception {
        assertRetiredConstructorPreservesCheckpointStatus(true, false);
    }

    @Test
    public void testRetiredStandaloneConstructorPreservesTerminalCheckpointStatus()
            throws Exception {
        assertRetiredConstructorPreservesCheckpointStatus(false, false);
    }

    @Test
    public void testRetiredApplicationConstructorDoesNotRecreateRemovedStatus() throws Exception {
        assertRetiredConstructorPreservesCheckpointStatus(true, true);
    }

    @Test
    public void testRetiredStandaloneConstructorDoesNotRecreateRemovedStatus() throws Exception {
        assertRetiredConstructorPreservesCheckpointStatus(false, true);
    }

    private void assertRetiredConstructorPreservesCheckpointStatus(
            boolean application, boolean removed) throws Exception {
        long jobId = 77105100L + (application ? 1L : 0L) + (removed ? 2L : 0L);
        String stateKey = "checkpoint_state_" + jobId + "_1";
        IMap<Object, Object> backing = instance.getMap(IMAP_RUNNING_JOB_STATE);
        backing.put(jobId, JobStatus.RUNNING);
        backing.put(stateKey, CheckpointCoordinatorStatus.RUNNING);
        IMap<Object, Object> delayed = Mockito.spy(backing);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean firstRead = new AtomicBoolean(true);
        if (removed) {
            // The old setter has already read non-null; removal must happen before its write.
            Mockito.doAnswer(
                            call -> {
                                entered.countDown();
                                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                                backing.set(stateKey, CheckpointCoordinatorStatus.RUNNING);
                                return null;
                            })
                    .when(delayed)
                    .set(stateKey, CheckpointCoordinatorStatus.RUNNING);
        } else {
            Mockito.doAnswer(
                            call -> {
                                Object captured = backing.get(stateKey);
                                if (firstRead.compareAndSet(true, false)) {
                                    entered.countDown();
                                    Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                                }
                                return captured;
                            })
                    .when(delayed)
                    .get(stateKey);
        }
        // The replacement atomic operation is paused at the same ownership boundary.
        Mockito.doAnswer(
                        call -> {
                            entered.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return backing.computeIfPresent(stateKey, call.getArgument(1));
                        })
                .when(delayed)
                .computeIfPresent(ArgumentMatchers.eq(stateKey), ArgumentMatchers.any());
        JobMaster owner = Mockito.mock(JobMaster.class);
        Mockito.when(owner.getResourceManager())
                .thenReturn(
                        application
                                ? Mockito.mock(ApplicationResourceManager.class)
                                : Mockito.mock(StandaloneResourceManager.class));
        Mockito.when(owner.getRunningJobStateIMap()).thenReturn(backing);
        AtomicBoolean retired = new AtomicBoolean();
        Mockito.doAnswer(
                        call -> {
                            if (retired.get()) throw new InterruptedException("retired");
                            return null;
                        })
                .when(owner)
                .checkRecoveryActive();
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 12L);
        ExecutorService task = Executors.newSingleThreadExecutor();
        try {
            Future<?> initialized =
                    task.submit(
                            () ->
                                    Assertions.assertThrows(
                                            InterruptedException.class,
                                            () ->
                                                    new CheckpointManager(
                                                            jobId,
                                                            true,
                                                            RestoreMode.NONE,
                                                            null,
                                                            true,
                                                            nodeEngine,
                                                            owner,
                                                            Collections.singletonMap(
                                                                    1,
                                                                    CheckpointPlan.builder()
                                                                            .pipelineId(1)
                                                                            .build()),
                                                            new CheckpointConfig(),
                                                            storage,
                                                            instance.getExecutorService("test"),
                                                            delayed,
                                                            server.getEngineContext(),
                                                            null)));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            retired.set(true);
            if (removed) {
                backing.remove(stateKey);
            } else {
                backing.put(stateKey, CheckpointCoordinatorStatus.FINISHED);
            }
            release.countDown();
            initialized.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    removed ? null : CheckpointCoordinatorStatus.FINISHED, backing.get(stateKey));
        } finally {
            release.countDown();
            task.shutdownNow();
        }
    }

    private void assertRetiredInitializerCannotRewindSuccessorCounter(
            boolean application, boolean terminalCleanup) throws Exception {
        long jobId = (application ? 77105003L : 77105005L) + (terminalCleanup ? 10L : 0L);
        String key = StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1);
        IMap<String, Long> backing = instance.getMap("retired-application-checkpoint-counter");
        backing.put(key, 19L);
        IMap<String, Long> delayed = Mockito.spy(backing);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        if (terminalCleanup) {
            Mockito.doAnswer(
                            call -> {
                                entered.countDown();
                                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                                return backing.putIfAbsent(key, 1L);
                            })
                    .when(delayed)
                    .putIfAbsent(key, 1L);
        }
        // Delay the legacy read response, or the replacement atomic mutation, across retirement.
        Mockito.doAnswer(
                        call -> {
                            Long captured = backing.get(key);
                            entered.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return captured;
                        })
                .when(delayed)
                .get(key);
        Mockito.doAnswer(
                        call -> {
                            entered.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return backing.compute(key, call.getArgument(1));
                        })
                .when(delayed)
                .compute(ArgumentMatchers.eq(key), ArgumentMatchers.any());
        CounterStateStore<String> counters = new HazelcastCounterStateStore<>(delayed);
        SeaTunnelEngineContext context =
                Mockito.mock(SeaTunnelEngineContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getStateStores().checkpointCounterStore()).thenReturn(counters);
        JobMaster owner = Mockito.mock(JobMaster.class);
        if (application) {
            Mockito.when(owner.getResourceManager())
                    .thenReturn(Mockito.mock(ApplicationResourceManager.class));
        } else {
            Mockito.when(owner.getResourceManager())
                    .thenReturn(Mockito.mock(StandaloneResourceManager.class));
        }
        AtomicBoolean retired = new AtomicBoolean();
        Mockito.doAnswer(
                        call -> {
                            if (retired.get()) throw new InterruptedException("retired");
                            return null;
                        })
                .when(owner)
                .checkRecoveryActive();
        IMap<Object, Object> running = instance.getMap(IMAP_RUNNING_JOB_STATE);
        running.put(jobId, JobStatus.RUNNING);
        Mockito.when(owner.getRunningJobStateIMap()).thenReturn(running);
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 12L);
        ExecutorService task = Executors.newSingleThreadExecutor();
        try {
            Future<?> initialized =
                    task.submit(
                            () ->
                                    Assertions.assertThrows(
                                            InterruptedException.class,
                                            () ->
                                                    new CheckpointManager(
                                                            jobId,
                                                            true,
                                                            RestoreMode.NONE,
                                                            null,
                                                            true,
                                                            nodeEngine,
                                                            owner,
                                                            Collections.singletonMap(
                                                                    1,
                                                                    CheckpointPlan.builder()
                                                                            .pipelineId(1)
                                                                            .build()),
                                                            new CheckpointConfig(),
                                                            storage,
                                                            instance.getExecutorService("test"),
                                                            running,
                                                            context,
                                                            null)));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            // Model a worker that survives executor retirement and a successor allocating IDs.
            retired.set(true);
            if (terminalCleanup) {
                backing.remove(key);
            } else {
                backing.compute(key, (ignored, current) -> current + 5L);
            }
            String stateKey = "checkpoint_state_" + jobId + "_1";
            running.put(stateKey, CheckpointCoordinatorStatus.FINISHED);
            release.countDown();
            initialized.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(terminalCleanup ? 1L : 24L, backing.get(key));
            Assertions.assertEquals(CheckpointCoordinatorStatus.FINISHED, running.get(stateKey));
            if (terminalCleanup) {
                Mockito.verify(delayed, Mockito.never())
                        .compute(ArgumentMatchers.anyString(), ArgumentMatchers.any());
                StateStoreCheckpointIDCounter successor =
                        new StateStoreCheckpointIDCounter(
                                jobId, 1, new HazelcastCounterStateStore<>(backing));
                successor.start();
                successor.advanceToAtLeast(13L);
                Assertions.assertEquals(13L, successor.getAndIncrement());
                successor.shutdown(PipelineStatus.FINISHED).join();
                Assertions.assertNull(backing.get(key));
            }
        } finally {
            release.countDown();
            task.shutdownNow();
        }
    }

    @Test
    public void testRetirementCanProceedDuringBlockedCounterInitialization() throws Exception {
        JobMaster owner = Mockito.mock(JobMaster.class);
        Mockito.when(owner.getResourceManager())
                .thenReturn(Mockito.mock(ApplicationResourceManager.class));
        CoordinatorService coordinator = server.getCoordinatorService();
        Mockito.when(owner.getCoordinatorService()).thenReturn(coordinator);
        AtomicBoolean retired = new AtomicBoolean();
        Mockito.doAnswer(
                        call -> {
                            if (retired.get()) throw new InterruptedException("retired");
                            return null;
                        })
                .when(owner)
                .checkInitializationActive();
        SeaTunnelEngineContext context =
                Mockito.mock(SeaTunnelEngineContext.class, Mockito.RETURNS_DEEP_STUBS);
        CounterStateStore<String> counters = Mockito.mock(CounterStateStore.class);
        Mockito.when(context.getStateStores().checkpointCounterStore()).thenReturn(counters);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(
                        counters.initializeIfAbsent(
                                ArgumentMatchers.anyString(), ArgumentMatchers.anyLong()))
                .thenAnswer(
                        call -> {
                            entered.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            return true;
                        });
        ExecutorService tasks = Executors.newFixedThreadPool(2);
        try {
            Future<?> initialized =
                    tasks.submit(
                            () -> {
                                Map<Integer, CheckpointPlan> plans = new HashMap<>();
                                plans.put(1, CheckpointPlan.builder().pipelineId(1).build());
                                Assertions.assertThrows(
                                        InterruptedException.class,
                                        () ->
                                                new CheckpointManager(
                                                        77105002L,
                                                        false,
                                                        RestoreMode.NONE,
                                                        null,
                                                        false,
                                                        nodeEngine,
                                                        owner,
                                                        plans,
                                                        new CheckpointConfig(),
                                                        Mockito.mock(CheckpointStorage.class),
                                                        instance.getExecutorService("test"),
                                                        nodeEngine
                                                                .getHazelcastInstance()
                                                                .getMap(IMAP_RUNNING_JOB_STATE),
                                                        context,
                                                        null));
                            });
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            tasks.submit(
                            () -> {
                                synchronized (coordinator) {
                                    retired.set(true);
                                }
                            })
                    .get(5, TimeUnit.SECONDS);
            release.countDown();
            initialized.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            tasks.shutdownNow();
        }
    }

    @Test
    public void testNonApplicationRecoveryKeepsExistingCounterInitialization() throws Exception {
        long jobId = 77105004L;
        String key = StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1);
        CounterStateStore<String> counters = Mockito.mock(CounterStateStore.class);
        Mockito.when(counters.get(key)).thenReturn(19L);
        SeaTunnelEngineContext context =
                Mockito.mock(SeaTunnelEngineContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getStateStores().checkpointCounterStore()).thenReturn(counters);
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 12L);
        new CheckpointManager(
                jobId,
                true,
                RestoreMode.NONE,
                jobId,
                false,
                nodeEngine,
                Mockito.mock(JobMaster.class),
                Collections.singletonMap(1, CheckpointPlan.builder().pipelineId(1).build()),
                new CheckpointConfig(),
                storage,
                instance.getExecutorService("test"),
                instance.getMap(IMAP_RUNNING_JOB_STATE),
                context,
                null);

        Mockito.verify(counters).initializeIfAbsent(key, 1L);
        Mockito.verify(counters).get(key);
        Mockito.verify(counters).set(key, 19L);
        Mockito.verify(counters, Mockito.never())
                .advanceToAtLeast(ArgumentMatchers.anyString(), ArgumentMatchers.anyLong());
    }

    @Test
    public void testExplicitRestoreRetainsTerminalStatusOverwrite() throws Exception {
        long jobId = 77105110L;
        String stateKey = "checkpoint_state_" + jobId + "_1";
        IMap<Object, Object> running = instance.getMap(IMAP_RUNNING_JOB_STATE);
        running.put(stateKey, CheckpointCoordinatorStatus.FINISHED);
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 12L, CheckpointType.SAVEPOINT_TYPE);

        CheckpointManager restored =
                createCheckpointManager(
                        jobId, true, RestoreMode.SAVEPOINT, jobId, storage, new CheckpointConfig());
        CheckpointCoordinator coordinator = restored.getCheckpointCoordinator(1);
        Assertions.assertEquals(13L, coordinator.getCheckpointIdCounter().get());
        Assertions.assertEquals(CheckpointCoordinatorStatus.RUNNING, running.get(stateKey));
        Assertions.assertFalse(coordinator.waitCheckpointCoordinatorComplete().isDone());
    }

    @Test
    public void testTakeoverRejectsStoreWithoutAtomicAdvancement() throws Exception {
        long jobId = 77105007L;
        CounterStateStore<String> counters =
                Mockito.mock(CounterStateStore.class, Mockito.CALLS_REAL_METHODS);
        SeaTunnelEngineContext context =
                Mockito.mock(SeaTunnelEngineContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getStateStores().checkpointCounterStore()).thenReturn(counters);
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 12L);
        IMap<Object, Object> running = instance.getMap(IMAP_RUNNING_JOB_STATE);
        running.put(jobId, JobStatus.RUNNING);
        JobMaster owner = Mockito.mock(JobMaster.class);
        Mockito.when(owner.getRunningJobStateIMap()).thenReturn(running);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        new CheckpointManager(
                                jobId,
                                true,
                                RestoreMode.NONE,
                                null,
                                true,
                                nodeEngine,
                                owner,
                                Collections.singletonMap(
                                        1, CheckpointPlan.builder().pipelineId(1).build()),
                                new CheckpointConfig(),
                                storage,
                                instance.getExecutorService("test"),
                                running,
                                context,
                                null));
        Mockito.verify(counters, Mockito.never()).get(ArgumentMatchers.anyString());
        Mockito.verify(counters, Mockito.never())
                .set(ArgumentMatchers.anyString(), ArgumentMatchers.anyLong());
        Assertions.assertNull(running.get("checkpoint_state_" + jobId + "_1"));
    }

    @Test
    public void testCheckpointConstructionDoesNotHoldCoordinatorMonitor() throws Exception {
        JobMaster owner = Mockito.mock(JobMaster.class);
        Mockito.when(owner.getCoordinatorService()).thenReturn(server.getCoordinatorService());
        SeaTunnelEngineContext context =
                Mockito.mock(SeaTunnelEngineContext.class, Mockito.RETURNS_DEEP_STUBS);
        CounterStateStore<String> counters = Mockito.mock(CounterStateStore.class);
        Mockito.when(context.getStateStores().checkpointCounterStore()).thenReturn(counters);
        Mockito.when(
                        counters.initializeIfAbsent(
                                ArgumentMatchers.anyString(), ArgumentMatchers.anyLong()))
                .thenAnswer(
                        call -> {
                            Assertions.assertFalse(
                                    Thread.holdsLock(server.getCoordinatorService()),
                                    "Distributed counter initialization must not hold coordinator monitor");
                            return true;
                        });
        Map<Integer, CheckpointPlan> plans = new HashMap<>();
        plans.put(1, CheckpointPlan.builder().pipelineId(1).build());
        new CheckpointManager(
                77105001L,
                false,
                RestoreMode.NONE,
                null,
                false,
                nodeEngine,
                owner,
                plans,
                new CheckpointConfig(),
                Mockito.mock(CheckpointStorage.class),
                instance.getExecutorService("test"),
                nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                context,
                null);
        Mockito.verify(counters)
                .initializeIfAbsent(ArgumentMatchers.anyString(), ArgumentMatchers.anyLong());
    }

    @Test
    public void testTakeoverDoesNotReadOlderCorruptPayloads() throws Exception {
        long jobId = 77103000L;
        CheckpointStorage storage = Mockito.spy(createCheckpointStorage());
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(jobId))
                        .pipelineId(1)
                        .checkpointId(7L)
                        .states(new byte[] {1, 2, 3})
                        .build());
        storeCompletedCheckpoint(storage, jobId, 12L);
        CheckpointManager recovered =
                createCheckpointManager(
                        jobId, true, RestoreMode.NONE, null, storage, new CheckpointConfig(), true);
        Assertions.assertEquals(
                13L, recovered.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        Mockito.verify(storage, Mockito.never())
                .getCheckpointsByJobIdAndPipelineId(String.valueOf(jobId), "1");
    }

    @Test
    public void testRecoveryPrefersCurrentJobCheckpointOverOriginalSavepoint() throws Exception {
        long jobId = 77103001L;
        long originalJobId = 77103002L;
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, originalJobId, 7L, CheckpointType.SAVEPOINT_TYPE);
        storeCompletedCheckpoint(storage, jobId, 12L, CheckpointType.COMPLETED_POINT_TYPE);
        CheckpointManager recovered =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        originalJobId,
                        storage,
                        new CheckpointConfig(),
                        true);
        Assertions.assertEquals(
                13L, recovered.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        CheckpointManager ownOnly =
                createCheckpointManager(
                        jobId, true, RestoreMode.NONE, null, storage, new CheckpointConfig(), true);
        Assertions.assertEquals(
                13L, ownOnly.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testRecoveryFallsBackToOriginalSavepointOnlyAfterSuccessfulEmptyRead()
            throws Exception {
        long jobId = 77103003L;
        long originalJobId = 77103004L;
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, originalJobId, 7L, CheckpointType.SAVEPOINT_TYPE);
        CheckpointManager recovered =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        originalJobId,
                        storage,
                        new CheckpointConfig(),
                        true);
        Assertions.assertEquals(
                8L, recovered.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testRecoveryReadFailureDoesNotFallBackToOriginalSource() throws Exception {
        long jobId = 77103005L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1"))
                .thenThrow(new CheckpointStorageException("retained checkpoint read denied"));
        Assertions.assertThrows(
                Exception.class,
                () ->
                        createCheckpointManager(
                                jobId,
                                true,
                                RestoreMode.SAVEPOINT,
                                77103006L,
                                storage,
                                new CheckpointConfig(),
                                true));
        Mockito.verify(storage, Mockito.never())
                .getCheckpointsByJobIdAndPipelineId("77103006", "1");
        Mockito.verify(storage).getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1");
    }

    @Test
    public void testTakeoverRetriesTemporaryListingFailureAndPreservesCounter() throws Exception {
        long jobId = 77103101L;
        CheckpointStorage storage = Mockito.spy(createCheckpointStorage());
        storeCompletedCheckpoint(storage, jobId, 12L);
        Mockito.doThrow(
                        new CheckpointStorageUnavailableException(
                                "listing unavailable", new SocketTimeoutException()))
                .doCallRealMethod()
                .when(storage)
                .getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1");
        CounterStateStore<String> counters =
                server.getEngineContext().getStateStores().checkpointCounterStore();
        counters.set(StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1), 19L);
        CheckpointManager recovered =
                createCheckpointManager(
                        jobId, true, RestoreMode.NONE, null, storage, new CheckpointConfig(), true);
        Assertions.assertEquals(
                19L, recovered.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        Mockito.verify(storage, Mockito.times(2))
                .getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1");
    }

    @Test
    public void testColdRestoreRetirementAfterReadDoesNotCreateCoordinators() throws Exception {
        long jobId = 77103106L;
        JobMaster owner = Mockito.mock(JobMaster.class);
        Mockito.when(owner.getCoordinatorService()).thenReturn(server.getCoordinatorService());
        AtomicBoolean retired = new AtomicBoolean();
        Mockito.doAnswer(
                        invocation -> {
                            if (retired.get()) {
                                throw new InterruptedException("retired during storage read");
                            }
                            return null;
                        })
                .when(owner)
                .checkInitializationActive();
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getCheckpointsByJobIdAndPipelineId("77103107", "1"))
                .thenAnswer(
                        invocation -> {
                            retired.set(true);
                            return Collections.emptyList();
                        });
        Map<Integer, CheckpointPlan> plans = new HashMap<>();
        plans.put(1, CheckpointPlan.builder().pipelineId(1).build());
        Assertions.assertThrows(
                InterruptedException.class,
                () ->
                        new CheckpointManager(
                                jobId,
                                true,
                                RestoreMode.SAVEPOINT,
                                77103107L,
                                false,
                                nodeEngine,
                                owner,
                                plans,
                                new CheckpointConfig(),
                                storage,
                                instance.getExecutorService("test"),
                                nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                                server.getEngineContext(),
                                null));
        Assertions.assertTrue(retired.get(), "the storage read must actually complete first");
        Assertions.assertNull(
                server.getEngineContext()
                        .getStateStores()
                        .checkpointCounterStore()
                        .get(StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1)));
        Assertions.assertNull(
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(IMAP_RUNNING_JOB_STATE)
                        .get("checkpoint_state_" + jobId + "_1"));
    }

    @Test
    public void testTakeoverStorageTimeoutDoesNotCreateStateOrFallBack() throws Exception {
        long jobId = 77103102L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1"))
                .thenThrow(
                        new CheckpointStorageUnavailableException(
                                "listing unavailable", new ConnectException()));
        CheckpointConfig config = new CheckpointConfig();
        config.setCheckpointTimeout(20);
        Assertions.assertThrows(
                CheckpointStorageUnavailableException.class,
                () ->
                        createCheckpointManager(
                                jobId,
                                true,
                                RestoreMode.SAVEPOINT,
                                77103999L,
                                storage,
                                config,
                                true));
        Assertions.assertNull(
                server.getEngineContext()
                        .getStateStores()
                        .checkpointCounterStore()
                        .get(StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1)));
        Assertions.assertNull(
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(IMAP_RUNNING_JOB_STATE)
                        .get("checkpoint_state_" + jobId + "_1"));
        Mockito.verify(storage, Mockito.never())
                .getCheckpointsByJobIdAndPipelineId("77103999", "1");
    }

    @Test
    public void testColdRestoreDoesNotRetryUnavailableStorage() throws Exception {
        long jobId = 77103103L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getCheckpointsByJobIdAndPipelineId("77103999", "1"))
                .thenThrow(
                        new CheckpointStorageUnavailableException(
                                "listing unavailable", new SocketTimeoutException()));
        Assertions.assertThrows(
                CheckpointStorageUnavailableException.class,
                () ->
                        createCheckpointManager(
                                jobId,
                                true,
                                RestoreMode.SAVEPOINT,
                                77103999L,
                                storage,
                                new CheckpointConfig(),
                                false));
        Mockito.verify(storage).getCheckpointsByJobIdAndPipelineId("77103999", "1");
    }

    @Test
    public void testTakeoverReadsAllPipelinesBeforeChangingAnyCounters() throws Exception {
        long jobId = 77103104L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1"))
                .thenReturn(null);
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "2"))
                .thenThrow(new CheckpointStorageException("corrupt checkpoint"));
        Map<Integer, CheckpointPlan> plans = new LinkedHashMap<>();
        plans.put(1, CheckpointPlan.builder().pipelineId(1).build());
        plans.put(2, CheckpointPlan.builder().pipelineId(2).build());
        Assertions.assertThrows(
                CheckpointStorageException.class,
                () ->
                        new CheckpointManager(
                                jobId,
                                true,
                                RestoreMode.NONE,
                                null,
                                true,
                                nodeEngine,
                                null,
                                plans,
                                new CheckpointConfig(),
                                storage,
                                instance.getExecutorService("test"),
                                nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                                server.getEngineContext(),
                                null));
        for (int pipelineId : plans.keySet()) {
            Assertions.assertNull(
                    server.getEngineContext()
                            .getStateStores()
                            .checkpointCounterStore()
                            .get(
                                    StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                            jobId, pipelineId)));
            Assertions.assertNull(
                    nodeEngine
                            .getHazelcastInstance()
                            .getMap(IMAP_RUNNING_JOB_STATE)
                            .get("checkpoint_state_" + jobId + "_" + pipelineId));
        }
        Mockito.verify(storage).getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "2");
    }

    @Test
    public void testTakeoverInterruptAbandonsReadWithoutChangingState() throws Exception {
        long jobId = 77103105L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1"))
                .thenAnswer(
                        call -> {
                            Thread.currentThread().interrupt();
                            throw new CheckpointStorageUnavailableException(
                                    "listing unavailable", new SocketTimeoutException());
                        });
        try {
            Assertions.assertThrows(
                    InterruptedException.class,
                    () ->
                            createCheckpointManager(
                                    jobId,
                                    true,
                                    RestoreMode.NONE,
                                    null,
                                    storage,
                                    new CheckpointConfig(),
                                    true));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        Mockito.verify(storage).getLatestCheckpointByJobIdAndPipelineId(String.valueOf(jobId), "1");
        Assertions.assertNull(
                server.getEngineContext()
                        .getStateStores()
                        .checkpointCounterStore()
                        .get(StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1)));
    }

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
    public void testTypedRestoreSelectsHighestMatchingIdEvenWhenLatestFileHasLowerId()
            throws Exception {
        long jobId = 77103110L;
        long source = 77103111L;
        CheckpointStorage storage = Mockito.mock(CheckpointStorage.class);
        PipelineState[] states = new PipelineState[2];
        for (int i = 0; i < states.length; i++) {
            long id = i == 0 ? 12L : 7L;
            CompletedCheckpoint checkpoint =
                    new CompletedCheckpoint(
                            source,
                            1,
                            id,
                            1L,
                            CheckpointType.SAVEPOINT_TYPE,
                            2L,
                            new HashMap<>(),
                            new HashMap<>());
            states[i] =
                    PipelineState.builder()
                            .jobId(String.valueOf(source))
                            .pipelineId(1)
                            .checkpointId(id)
                            .states(new ProtoStuffSerializer().serialize(checkpoint))
                            .build();
        }
        Mockito.when(storage.getLatestCheckpointByJobIdAndPipelineId(String.valueOf(source), "1"))
                .thenReturn(states[1]);
        Mockito.when(storage.getCheckpointsByJobIdAndPipelineId(String.valueOf(source), "1"))
                .thenReturn(Arrays.asList(states));
        CheckpointManager restored =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        source,
                        storage,
                        new CheckpointConfig(),
                        false);
        Assertions.assertEquals(
                13L, restored.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        Mockito.verify(storage).getCheckpointsByJobIdAndPipelineId(String.valueOf(source), "1");
    }

    @Test
    public void testTypedRestoreStillSelectsOlderMatchingSavepoint() throws Exception {
        long jobId = 77103108L;
        CheckpointStorage storage = createCheckpointStorage();
        storeCompletedCheckpoint(storage, jobId, 7L, CheckpointType.SAVEPOINT_TYPE);
        storeCompletedCheckpoint(storage, jobId, 12L, CheckpointType.COMPLETED_POINT_TYPE);
        CheckpointManager restored =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        storage,
                        new CheckpointConfig(),
                        false);
        Assertions.assertEquals(
                8L, restored.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testTypedRestoreRetainsLegacyBulkValidation() throws Exception {
        long jobId = 77103109L;
        CheckpointStorage storage = Mockito.spy(createCheckpointStorage());
        storeCompletedCheckpoint(storage, jobId, 7L, CheckpointType.SAVEPOINT_TYPE);
        storage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(String.valueOf(jobId))
                        .pipelineId(1)
                        .checkpointId(12L)
                        .states(new byte[] {1, 2, 3})
                        .build());
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        createCheckpointManager(
                                jobId,
                                true,
                                RestoreMode.SAVEPOINT,
                                jobId,
                                storage,
                                new CheckpointConfig(),
                                false));
        Mockito.verify(storage).getCheckpointsByJobIdAndPipelineId(String.valueOf(jobId), "1");
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

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            boolean masterFailoverRestore,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                checkpointStorage,
                checkpointConfig,
                masterFailoverRestore);
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
                checkpointStorage,
                checkpointConfig,
                false);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig,
            boolean recoverCurrentJob) {
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        return new CheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                recoverCurrentJob,
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
}
