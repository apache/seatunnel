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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_STATE;

public class CheckpointCoordinatorTest
        extends AbstractSeaTunnelServerTest<CheckpointCoordinatorTest> {

    @Test
    void testACKNotExistPendingCheckpoint() {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        CheckpointManager checkpointManager =
                new CheckpointManager(
                        1L,
                        false,
                        nodeEngine,
                        null,
                        planMap,
                        checkpointConfig,
                        server.getCheckpointService().getCheckpointStorage(),
                        instance.getExecutorService("test"),
                        nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                        null);
        checkpointManager.acknowledgeTask(
                new TaskAcknowledgeOperation(
                        new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1),
                        new CheckpointBarrier(
                                999, System.currentTimeMillis(), CheckpointType.CHECKPOINT_TYPE),
                        new ArrayList<>()));
    }

    @Test
    void testSchedulerThreadShouldNotBeInterruptedBeforeJobMasterCleaned()
            throws ExecutionException, InterruptedException, TimeoutException {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        // quickly fail the checkpoint
        checkpointConfig.setCheckpointTimeout(5000);
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(
                1,
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(Collections.singleton(new TaskLocation()))
                        .build());
        CompletableFuture<Boolean> threadIsInterrupted = new CompletableFuture<>();
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            CheckpointManager checkpointManager =
                    new CheckpointManager(
                            1L,
                            false,
                            nodeEngine,
                            null,
                            planMap,
                            checkpointConfig,
                            server.getCheckpointService().getCheckpointStorage(),
                            executorService,
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                            null) {

                        @Override
                        protected void handleCheckpointError(int pipelineId, boolean neverRestore) {
                            threadIsInterrupted.complete(Thread.interrupted());
                        }
                    };
            checkpointManager.reportedPipelineRunning(1, true);
            Assertions.assertFalse(threadIsInterrupted.get(1, TimeUnit.MINUTES));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testCheckpointContinuesWorkAfterClockDrift()
            throws ExecutionException, InterruptedException, TimeoutException {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        checkpointConfig.setCheckpointTimeout(5000);
        checkpointConfig.setCheckpointInterval(5000);
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(
                1,
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(Collections.singleton(new TaskLocation()))
                        .build());
        ExecutorService executorService = Executors.newCachedThreadPool();
        CompletableFuture<Boolean> invokedHandleCheckpointError = new CompletableFuture<>();
        Instant now = Instant.now();
        Instant startTime = now.minusSeconds(10);
        try (MockedStatic<Instant> mockedInstant = Mockito.mockStatic(Instant.class)) {
            mockedInstant.when(Instant::now).thenReturn(startTime);
            CheckpointManager checkpointManager =
                    new CheckpointManager(
                            1L,
                            false,
                            nodeEngine,
                            null,
                            planMap,
                            checkpointConfig,
                            server.getCheckpointService().getCheckpointStorage(),
                            executorService,
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                            null) {
                        @Override
                        protected void handleCheckpointError(int pipelineId, boolean neverRestore) {
                            invokedHandleCheckpointError.complete(true);
                        }
                    };
            ReflectionUtils.setField(
                    checkpointManager.getCheckpointCoordinator(1),
                    "latestTriggerTimestamp",
                    new AtomicLong(startTime.toEpochMilli()));
            checkpointManager.reportedPipelineRunning(1, true);
            Assertions.assertTrue(invokedHandleCheckpointError.get(1, TimeUnit.MINUTES));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testCheckpointMinPause() {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        checkpointConfig.setCheckpointInterval(10000); // 10 seconds
        checkpointConfig.setCheckpointMinPause(5000); // 5 seconds min-pause
        checkpointConfig.setCheckpointTimeout(30000);

        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
        planMap.put(
                1,
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(Collections.singleton(taskLocation))
                        .startingSubtasks(Collections.singleton(taskLocation))
                        .build());

        ExecutorService executorService = Executors.newCachedThreadPool();
        JobMaster mockJobMaster = Mockito.mock(JobMaster.class);
        Mockito.when(mockJobMaster.getJobId()).thenReturn(1L);
        Mockito.when(mockJobMaster.isNeedRestore()).thenReturn(false);
        Mockito.when(mockJobMaster.queryTaskGroupAddress(Mockito.any(TaskGroupLocation.class)))
                .thenReturn(nodeEngine.getThisAddress());

        // Simulate the scenario: checkpoint starts at 0s, completes at 8s, next should trigger at
        // 13s
        Instant time0s = Instant.ofEpochMilli(0);
        // Checkpoint completes at 8s
        Instant time8s = Instant.ofEpochMilli(8000);
        Instant time10s = Instant.ofEpochMilli(10000);

        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        1L,
                        1,
                        1L,
                        time0s.toEpochMilli(), // triggerTimestamp (started at 0s)
                        CheckpointType.CHECKPOINT_TYPE,
                        time8s.toEpochMilli(), // completedTimestamp (completed at 8s)
                        new HashMap<>(),
                        new HashMap<>());

        try (MockedStatic<Instant> mockedInstant = Mockito.mockStatic(Instant.class)) {
            mockedInstant.when(Instant::now).thenReturn(time10s);

            CheckpointManager checkpointManager =
                    new CheckpointManager(
                            1L,
                            false,
                            nodeEngine,
                            mockJobMaster,
                            planMap,
                            checkpointConfig,
                            server.getCheckpointService().getCheckpointStorage(),
                            executorService,
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                            null) {

                        @Override
                        public void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
                            mockedInstant.when(Instant::now).thenReturn(time8s);
                            super.acknowledgeTask(ackOperation);
                        }

                        @Override
                        public CheckpointCoordinator getCheckpointCoordinator(int pipelineId) {

                            CheckpointCoordinator originalCoordinator =
                                    super.getCheckpointCoordinator(pipelineId);
                            CheckpointCoordinator spyCheckpointCoordinator =
                                    Mockito.spy(originalCoordinator);
                            Mockito.doAnswer(
                                            invocation -> {
                                                Object argument = invocation.getArgument(1);
                                                Assertions.assertEquals(
                                                        3000,
                                                        Integer.parseInt(argument.toString()),
                                                        "Checkpoint should be delayed by exactly 3 seconds (from 10s to 13s)");
                                                return invocation.callRealMethod();
                                            })
                                    .when(spyCheckpointCoordinator)
                                    .scheduleTriggerPendingCheckpoint(
                                            Mockito.any(CheckpointType.class), Mockito.anyLong());

                            Mockito.doReturn(new InvocationFuture[0])
                                    .when(spyCheckpointCoordinator)
                                    .notifyCheckpointCompleted(completedCheckpoint);
                            Mockito.doReturn(new InvocationFuture[0])
                                    .when(spyCheckpointCoordinator)
                                    .notifyCheckpointEnd(completedCheckpoint);

                            ReflectionUtils.setField(
                                    spyCheckpointCoordinator,
                                    "latestCompletedCheckpoint",
                                    completedCheckpoint);

                            return spyCheckpointCoordinator;
                        }
                    };

            ReflectionUtils.setField(
                    checkpointManager.getCheckpointCoordinator(1),
                    "latestTriggerTimestamp",
                    new AtomicLong(time0s.toEpochMilli()));
            checkpointManager.reportedPipelineRunning(1, true);

        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testFilteringClosedTasksAndActions() {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        planMap.put(1, CheckpointPlan.builder().pipelineId(1).build());
        TestCheckpointManager checkpointManager =
                new TestCheckpointManager(
                        1L,
                        nodeEngine,
                        planMap,
                        checkpointConfig,
                        server.getCheckpointService().getCheckpointStorage(),
                        instance.getExecutorService("test"),
                        nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                        null);

        TaskGroupLocation group1 = new TaskGroupLocation(1L, 1, 1);
        TaskLocation task1 = new TaskLocation(group1, 1, 1);
        TaskLocation task2 = new TaskLocation(group1, 2, 1);

        ActionStateKey actionKey1 = new ActionStateKey("action1");
        ActionStateKey actionKey2 = new ActionStateKey("action2");

        Map<TaskLocation, Set<Tuple2<ActionStateKey, Integer>>> subtaskActions = new HashMap<>();
        subtaskActions.put(task1, new HashSet<>(Arrays.asList(Tuple2.tuple2(actionKey1, 0))));
        subtaskActions.put(task2, new HashSet<>(Arrays.asList(Tuple2.tuple2(actionKey2, 0))));

        Map<ActionStateKey, Integer> pipelineActions = new HashMap<>();
        pipelineActions.put(actionKey1, 1);
        pipelineActions.put(actionKey2, 1);

        CheckpointPlan plan =
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(new HashSet<>(Arrays.asList(task1, task2)))
                        .startingSubtasks(new HashSet<>(Arrays.asList(task1, task2)))
                        .subtaskActions(subtaskActions)
                        .pipelineActions(pipelineActions)
                        .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CheckpointCoordinator coordinator =
                new CheckpointCoordinator(
                        checkpointManager,
                        null,
                        checkpointConfig,
                        1L,
                        plan,
                        null,
                        null,
                        executor,
                        Mockito.mock(com.hazelcast.map.IMap.class),
                        false,
                        null);

        Map<Long, SeaTunnelTaskState> taskStatus = coordinator.getPipelineTaskStatus();
        taskStatus.put(task1.getTaskID(), SeaTunnelTaskState.RUNNING);
        taskStatus.put(task2.getTaskID(), SeaTunnelTaskState.CLOSED);

        Map<ActionStateKey, ActionState> actionStates =
                (Map<ActionStateKey, ActionState>)
                        ReflectionUtils.invoke(coordinator, "getActionStates");
        Assertions.assertTrue(actionStates.containsKey(actionKey1));
        Assertions.assertFalse(actionStates.containsKey(actionKey2));

        Map<Long, TaskStatistics> stats =
                (Map<Long, TaskStatistics>)
                        ReflectionUtils.invoke(coordinator, "getTaskStatistics");
        Assertions.assertTrue(stats.containsKey(task1.getTaskID()));
        Assertions.assertFalse(stats.containsKey(task2.getTaskID()));

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        1L, System.currentTimeMillis(), CheckpointType.CHECKPOINT_TYPE);
        coordinator.triggerCheckpoint(barrier);
        Assertions.assertEquals(1, checkpointManager.operations.size());

        executor.shutdownNow();
    }

    @Test
    void testReadyToClosePartialProgressPersistedAndRestoredCorrectly() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            TaskLocation task1 = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
            TaskLocation task2 = new TaskLocation(new TaskGroupLocation(1L, 1, 2), 2, 2);
            TaskLocation task3 = new TaskLocation(new TaskGroupLocation(1L, 1, 3), 3, 3);
            Set<TaskLocation> allStarting = new HashSet<>(Arrays.asList(task1, task2, task3));

            CheckpointConfig checkpointConfig = new CheckpointConfig();
            checkpointConfig.setStorage(new CheckpointStorageConfig());

            CheckpointPlan plan =
                    CheckpointPlan.builder()
                            .pipelineId(1)
                            .pipelineSubtasks(allStarting)
                            .startingSubtasks(allStarting)
                            .build();

            IMap<Object, Object> realIMap =
                    nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE);
            String readyToCloseKey = "checkpoint_state_1_1_ready_to_close";
            realIMap.remove(readyToCloseKey);

            CheckpointManager mockManager = Mockito.mock(CheckpointManager.class);
            CheckpointStorage mockStorage = Mockito.mock(CheckpointStorage.class);
            CheckpointIDCounter mockIdCounter = Mockito.mock(CheckpointIDCounter.class);

            // Phase 1: partial readyToClose (1 of 3)
            CheckpointCoordinator coord1 =
                    new CheckpointCoordinator(
                            mockManager,
                            mockStorage,
                            checkpointConfig,
                            1L,
                            plan,
                            mockIdCounter,
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy1 = Mockito.spy(coord1);
            Mockito.doNothing()
                    .when(spy1)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            spy1.readyToClose(task1);

            Object stored = realIMap.get(readyToCloseKey);
            Assertions.assertInstanceOf(Set.class, stored);
            Assertions.assertEquals(
                    1,
                    ((Set<?>) stored).size(),
                    "After 1 of 3 readyToClose, IMap should contain exactly 1 task");
            Mockito.verify(spy1, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);

            // Phase 2: restore with partial progress → should trigger normal checkpoint
            CheckpointCoordinator coord2 =
                    new CheckpointCoordinator(
                            mockManager,
                            mockStorage,
                            checkpointConfig,
                            1L,
                            plan,
                            mockIdCounter,
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy2 = Mockito.spy(coord2);
            Mockito.doReturn(true).when(spy2).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(spy2)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            spy2.restoreCoordinator(true);

            Mockito.verify(spy2).tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);
            Mockito.verify(spy2, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);

            // Phase 3: all tasks readyToClose (3 of 3)
            spy1.readyToClose(task2);
            spy1.readyToClose(task3);

            stored = realIMap.get(readyToCloseKey);
            Assertions.assertInstanceOf(Set.class, stored);
            Assertions.assertEquals(
                    3,
                    ((Set<?>) stored).size(),
                    "After 3 of 3 readyToClose, IMap should contain all 3 tasks");
            Mockito.verify(spy1).tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);

            // Phase 4: restore with full progress → should trigger COMPLETED_POINT
            CheckpointCoordinator coord3 =
                    new CheckpointCoordinator(
                            mockManager,
                            mockStorage,
                            checkpointConfig,
                            1L,
                            plan,
                            mockIdCounter,
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy3 = Mockito.spy(coord3);
            Mockito.doReturn(true).when(spy3).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(spy3)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            spy3.restoreCoordinator(true);

            Mockito.verify(spy3).tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
            Mockito.verify(spy3, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);

        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testRestoreCoordinatorShouldBeIdempotentWithPartialReadyToCloseProgress() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            TaskLocation task1 = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
            TaskLocation task2 = new TaskLocation(new TaskGroupLocation(1L, 1, 2), 2, 2);
            TaskLocation task3 = new TaskLocation(new TaskGroupLocation(1L, 1, 3), 3, 3);
            Set<TaskLocation> allStarting = new HashSet<>(Arrays.asList(task1, task2, task3));

            CheckpointConfig checkpointConfig = new CheckpointConfig();
            checkpointConfig.setStorage(new CheckpointStorageConfig());
            CheckpointPlan plan =
                    CheckpointPlan.builder()
                            .pipelineId(1)
                            .pipelineSubtasks(allStarting)
                            .startingSubtasks(allStarting)
                            .build();

            IMap<Object, Object> realIMap =
                    nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE);
            String readyToCloseKey = "checkpoint_state_1_1_ready_to_close";
            realIMap.put(readyToCloseKey, new HashSet<>(Collections.singleton(task1)));

            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(
                            Mockito.mock(CheckpointManager.class),
                            Mockito.mock(CheckpointStorage.class),
                            checkpointConfig,
                            1L,
                            plan,
                            Mockito.mock(CheckpointIDCounter.class),
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy = Mockito.spy(coordinator);
            Mockito.doReturn(true).when(spy).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(spy)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            Assertions.assertDoesNotThrow(() -> spy.restoreCoordinator(true));
            Assertions.assertDoesNotThrow(() -> spy.restoreCoordinator(true));

            Mockito.verify(spy, Mockito.times(2))
                    .tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);
            Mockito.verify(spy, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);

            Set<TaskLocation> restoredReadyToClose =
                    (Set<TaskLocation>)
                            ReflectionUtils.getField(spy, "readyToCloseStartingTask")
                                    .orElse(Collections.emptySet());
            Assertions.assertEquals(
                    1,
                    restoredReadyToClose.size(),
                    "Repeated restore should not duplicate partial readyToClose progress");
            Assertions.assertTrue(restoredReadyToClose.contains(task1));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testRestoreCoordinatorShouldFallbackToCheckpointWhenReadyToCloseKeyMissing() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            TaskLocation task1 = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
            TaskLocation task2 = new TaskLocation(new TaskGroupLocation(1L, 1, 2), 2, 2);
            Set<TaskLocation> allStarting = new HashSet<>(Arrays.asList(task1, task2));

            CheckpointConfig checkpointConfig = new CheckpointConfig();
            checkpointConfig.setStorage(new CheckpointStorageConfig());
            CheckpointPlan plan =
                    CheckpointPlan.builder()
                            .pipelineId(1)
                            .pipelineSubtasks(allStarting)
                            .startingSubtasks(allStarting)
                            .build();

            IMap<Object, Object> realIMap =
                    nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE);
            String readyToCloseKey = "checkpoint_state_1_1_ready_to_close";
            realIMap.remove(readyToCloseKey);

            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(
                            Mockito.mock(CheckpointManager.class),
                            Mockito.mock(CheckpointStorage.class),
                            checkpointConfig,
                            1L,
                            plan,
                            Mockito.mock(CheckpointIDCounter.class),
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy = Mockito.spy(coordinator);
            Mockito.doReturn(true).when(spy).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(spy)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            Assertions.assertDoesNotThrow(() -> spy.restoreCoordinator(true));
            Mockito.verify(spy).tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);
            Mockito.verify(spy, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testRestoreCoordinatorShouldFallbackToCheckpointWhenReadyToCloseValueCorrupted() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            TaskLocation task1 = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
            TaskLocation task2 = new TaskLocation(new TaskGroupLocation(1L, 1, 2), 2, 2);
            Set<TaskLocation> allStarting = new HashSet<>(Arrays.asList(task1, task2));

            CheckpointConfig checkpointConfig = new CheckpointConfig();
            checkpointConfig.setStorage(new CheckpointStorageConfig());
            CheckpointPlan plan =
                    CheckpointPlan.builder()
                            .pipelineId(1)
                            .pipelineSubtasks(allStarting)
                            .startingSubtasks(allStarting)
                            .build();

            IMap<Object, Object> realIMap =
                    nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE);
            String readyToCloseKey = "checkpoint_state_1_1_ready_to_close";
            realIMap.set(readyToCloseKey, "corrupted_ready_to_close_payload");

            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(
                            Mockito.mock(CheckpointManager.class),
                            Mockito.mock(CheckpointStorage.class),
                            checkpointConfig,
                            1L,
                            plan,
                            Mockito.mock(CheckpointIDCounter.class),
                            null,
                            executorService,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy = Mockito.spy(coordinator);
            Mockito.doReturn(true).when(spy).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(spy)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            Assertions.assertDoesNotThrow(() -> spy.restoreCoordinator(true));
            Mockito.verify(spy).tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);
            Mockito.verify(spy, Mockito.never())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testUpdateReadyToCloseStartingTaskWithTenTaskLocationsConcurrently() throws Exception {
        ExecutorService coordinatorExecutor = Executors.newCachedThreadPool();
        ExecutorService concurrentExecutor = Executors.newFixedThreadPool(10);
        try {
            Set<TaskLocation> allStarting = new HashSet<>();
            for (int i = 1; i <= 10; i++) {
                allStarting.add(new TaskLocation(new TaskGroupLocation(1L, 1, i), i, i));
            }

            CheckpointConfig checkpointConfig = new CheckpointConfig();
            checkpointConfig.setStorage(new CheckpointStorageConfig());
            CheckpointPlan plan =
                    CheckpointPlan.builder()
                            .pipelineId(1)
                            .pipelineSubtasks(allStarting)
                            .startingSubtasks(allStarting)
                            .build();

            IMap<Object, Object> realIMap = Mockito.mock(IMap.class);
            String readyToCloseKey = "checkpoint_state_1_1_ready_to_close";
            Map<Object, Object> simulatedImapStorage = new ConcurrentHashMap<>();
            Mockito.when(realIMap.get(Mockito.any()))
                    .thenAnswer(invocation -> simulatedImapStorage.get(invocation.getArgument(0)));

            CountDownLatch firstSnapshotReady = new CountDownLatch(1);
            CountDownLatch fullSnapshotWritten = new CountDownLatch(1);
            CountDownLatch allowFirstSnapshotWrite = new CountDownLatch(1);
            AtomicBoolean blockedFirstSnapshot = new AtomicBoolean(false);

            AtomicInteger computeCallCount = new AtomicInteger(0);
            AtomicInteger setCallCount = new AtomicInteger(0);

            Mockito.doAnswer(
                            invocation -> {
                                Object key = invocation.getArgument(0);
                                java.util.function.BiFunction<Object, Object, Object>
                                        remappingFunction = invocation.getArgument(1);
                                if (!readyToCloseKey.equals(key)) {
                                    return simulatedImapStorage.compute(key, remappingFunction);
                                }
                                computeCallCount.incrementAndGet();
                                if (blockedFirstSnapshot.compareAndSet(false, true)) {
                                    firstSnapshotReady.countDown();
                                    if (!allowFirstSnapshotWrite.await(30, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "Timed out while waiting to release first snapshot write");
                                    }
                                }
                                Object result =
                                        simulatedImapStorage.compute(key, remappingFunction);
                                @SuppressWarnings("unchecked")
                                Set<TaskLocation> snapshot = (Set<TaskLocation>) result;
                                if (snapshot.size() == allStarting.size()) {
                                    fullSnapshotWritten.countDown();
                                }
                                return result;
                            })
                    .when(realIMap)
                    .compute(Mockito.any(), Mockito.any());

            Mockito.doAnswer(
                            invocation -> {
                                Object key = invocation.getArgument(0);
                                Object value = invocation.getArgument(1);
                                if (readyToCloseKey.equals(key)) {
                                    setCallCount.incrementAndGet();
                                }
                                simulatedImapStorage.put(key, value);
                                return null;
                            })
                    .when(realIMap)
                    .set(Mockito.any(), Mockito.any());

            CheckpointCoordinator coordinator =
                    new CheckpointCoordinator(
                            Mockito.mock(CheckpointManager.class),
                            Mockito.mock(CheckpointStorage.class),
                            checkpointConfig,
                            1L,
                            plan,
                            Mockito.mock(CheckpointIDCounter.class),
                            null,
                            coordinatorExecutor,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator spy = Mockito.spy(coordinator);
            Mockito.doNothing()
                    .when(spy)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));

            CountDownLatch finishLatch = new CountDownLatch(allStarting.size());
            List<Future<?>> futures = new ArrayList<>(allStarting.size());
            List<TaskLocation> taskLocations = new ArrayList<>(allStarting);
            TaskLocation firstTask = taskLocations.get(0);

            Future<?> firstFuture =
                    concurrentExecutor.submit(
                            () -> {
                                try {
                                    spy.readyToClose(firstTask);
                                } finally {
                                    finishLatch.countDown();
                                }
                            });
            futures.add(firstFuture);

            Assertions.assertTrue(
                    firstSnapshotReady.await(30, TimeUnit.SECONDS),
                    "Should observe the first snapshot write");

            for (int i = 1; i < taskLocations.size(); i++) {
                TaskLocation taskLocation = taskLocations.get(i);
                Future<?> future =
                        concurrentExecutor.submit(
                                () -> {
                                    try {
                                        spy.readyToClose(taskLocation);
                                    } finally {
                                        finishLatch.countDown();
                                    }
                                });
                futures.add(future);
            }

            Assertions.assertTrue(
                    fullSnapshotWritten.await(30, TimeUnit.SECONDS),
                    "Should observe full snapshot write before releasing first write");
            allowFirstSnapshotWrite.countDown();
            Assertions.assertTrue(
                    finishLatch.await(30, TimeUnit.SECONDS),
                    "All concurrent readyToClose calls should finish in time");
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

            Object persistedValue = simulatedImapStorage.get(readyToCloseKey);
            Assertions.assertInstanceOf(
                    Set.class, persistedValue, "Persisted ready-to-close state should be a Set");
            Set<TaskLocation> persisted = (Set<TaskLocation>) persistedValue;
            Assertions.assertEquals(10, persisted.size());
            Assertions.assertTrue(persisted.containsAll(allStarting));
            Mockito.verify(spy, Mockito.atLeastOnce())
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);

            Assertions.assertTrue(
                    computeCallCount.get() >= 10,
                    "Production code must use atomic compute() for ready-to-close persistence,"
                            + " actual compute calls: "
                            + computeCallCount.get());
            Assertions.assertEquals(
                    0,
                    setCallCount.get(),
                    "Production code must NOT use non-atomic set() for ready-to-close persistence;"
                            + " set() would cause lost-update under concurrency");

            // Restore from persisted IMap state and verify merged set is fully recoverable.
            CheckpointCoordinator restoredCoordinator =
                    new CheckpointCoordinator(
                            Mockito.mock(CheckpointManager.class),
                            Mockito.mock(CheckpointStorage.class),
                            checkpointConfig,
                            1L,
                            plan,
                            Mockito.mock(CheckpointIDCounter.class),
                            null,
                            coordinatorExecutor,
                            realIMap,
                            false,
                            null);
            CheckpointCoordinator restoreSpy = Mockito.spy(restoredCoordinator);
            Mockito.doReturn(true).when(restoreSpy).notifyCompleted(Mockito.any());
            Mockito.doNothing()
                    .when(restoreSpy)
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));
            restoreSpy.restoreCoordinator(true);

            Set<TaskLocation> restoredReadyToClose =
                    (Set<TaskLocation>)
                            ReflectionUtils.getField(restoreSpy, "readyToCloseStartingTask")
                                    .orElse(Collections.emptySet());
            Assertions.assertEquals(
                    allStarting,
                    restoredReadyToClose,
                    "Restored ready-to-close state should recover the full merged set");
            Mockito.verify(restoreSpy)
                    .tryTriggerPendingCheckpoint(CheckpointType.COMPLETED_POINT_TYPE);
        } finally {
            concurrentExecutor.shutdownNow();
            coordinatorExecutor.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Regression tests: notifyCompleted returns false → callers bail out
    // ------------------------------------------------------------------

    /**
     * Helper: build a minimal {@link CheckpointCoordinator} whose external dependencies are all
     * mocked, so the test never touches Hazelcast / Hadoop I/O.
     */
    private CheckpointCoordinator buildMinimalCoordinator(ExecutorService executorService) {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());

        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
        CheckpointPlan plan =
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(Collections.singleton(taskLocation))
                        .startingSubtasks(Collections.singleton(taskLocation))
                        .build();

        CheckpointManager mockManager = Mockito.mock(CheckpointManager.class);
        CheckpointStorage mockStorage = Mockito.mock(CheckpointStorage.class);
        CheckpointIDCounter mockIdCounter = Mockito.mock(CheckpointIDCounter.class);
        @SuppressWarnings("unchecked")
        IMap<Object, Object> mockIMap = Mockito.mock(IMap.class);

        return new CheckpointCoordinator(
                mockManager,
                mockStorage,
                checkpointConfig,
                1L,
                plan,
                mockIdCounter,
                null,
                executorService,
                mockIMap,
                false,
                null);
    }

    /**
     * Regression: when {@code notifyCompleted()} fails (returns {@code false}), {@code
     * completePendingCheckpoint} must return immediately without decrementing {@code
     * pendingCounter} or executing any other "success path" logic.
     *
     * <p>Before the fix the chained call on the {@code null} result of {@code
     * pendingCheckpoints.remove()} caused a {@link NullPointerException}.
     */
    @Test
    void testCompletePendingCheckpointShouldReturnEarlyWhenNotifyCompletedFails() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            CheckpointCoordinator coordinator = buildMinimalCoordinator(executorService);
            CheckpointCoordinator spy = Mockito.spy(coordinator);

            // Mock notifyCompleted to simulate failure: clear pendingCheckpoints (as
            // handleCoordinatorError would) and return false.
            Mockito.doAnswer(
                            invocation -> {
                                @SuppressWarnings("unchecked")
                                ConcurrentHashMap<Long, PendingCheckpoint> map =
                                        (ConcurrentHashMap<Long, PendingCheckpoint>)
                                                ReflectionUtils.getField(spy, "pendingCheckpoints")
                                                        .orElse(null);
                                if (map != null) {
                                    map.clear();
                                }
                                return false;
                            })
                    .when(spy)
                    .notifyCompleted(Mockito.any());

            long checkpointId = 1L;
            CompletedCheckpoint completedCheckpoint =
                    new CompletedCheckpoint(
                            1L,
                            1,
                            checkpointId,
                            System.currentTimeMillis(),
                            CheckpointType.CHECKPOINT_TYPE,
                            System.currentTimeMillis(),
                            new HashMap<>(),
                            new HashMap<>());

            PendingCheckpoint pendingCheckpoint =
                    new PendingCheckpoint(
                            1L,
                            1,
                            checkpointId,
                            System.currentTimeMillis(),
                            CheckpointType.CHECKPOINT_TYPE,
                            new HashSet<>(),
                            new HashMap<>(),
                            new HashMap<>());

            @SuppressWarnings("unchecked")
            ConcurrentHashMap<Long, PendingCheckpoint> pendingCheckpoints =
                    (ConcurrentHashMap<Long, PendingCheckpoint>)
                            ReflectionUtils.getField(spy, "pendingCheckpoints")
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "pendingCheckpoints field not found"));
            pendingCheckpoints.put(checkpointId, pendingCheckpoint);

            // Set pendingCounter to 1 so we can verify it is NOT decremented after failure.
            AtomicInteger pendingCounter =
                    (AtomicInteger)
                            ReflectionUtils.getField(spy, "pendingCounter")
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "pendingCounter field not found"));
            pendingCounter.set(1);

            // Must not throw, and must not execute the success path.
            Assertions.assertDoesNotThrow(
                    () -> spy.completePendingCheckpoint(completedCheckpoint),
                    "completePendingCheckpoint must not throw when notifyCompleted fails");

            // pendingCounter must remain 1 – the success path (decrementAndGet) was skipped.
            Assertions.assertEquals(
                    1,
                    pendingCounter.get(),
                    "pendingCounter must not be decremented when notifyCompleted fails");
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Regression: when {@code notifyCompleted()} fails inside {@code allTaskReady()}, the method
     * must return immediately and must NOT schedule the next checkpoint trigger.
     */
    @Test
    void testAllTaskReadyShouldNotScheduleCheckpointWhenNotifyCompletedFails() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            CheckpointCoordinator coordinator = buildMinimalCoordinator(executorService);
            CheckpointCoordinator spy = Mockito.spy(coordinator);

            // notifyTaskStart() must return a non-null empty array so allOf(...).join() succeeds.
            Mockito.doReturn(new com.hazelcast.spi.impl.operationservice.impl.InvocationFuture[0])
                    .when(spy)
                    .notifyTaskStart();

            // Simulate notifyCompleted failure.
            Mockito.doReturn(false).when(spy).notifyCompleted(Mockito.any());

            // Set all tasks to READY_START so allTaskReady() passes the guard checks.
            Map<Long, SeaTunnelTaskState> taskStatus = spy.getPipelineTaskStatus();
            CheckpointPlan plan =
                    (CheckpointPlan)
                            ReflectionUtils.getField(spy, "plan")
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "plan field not found"));
            plan.getPipelineSubtasks()
                    .forEach(t -> taskStatus.put(t.getTaskID(), SeaTunnelTaskState.READY_START));

            // Invoke allTaskReady() via the package-private reportedTask path by directly calling
            // the protected restoreCoordinator and then manipulating state, or call allTaskReady
            // via reflection since it is private.
            ReflectionUtils.invoke(spy, "allTaskReady");

            // scheduleTriggerPendingCheckpoint must NOT have been called.
            Mockito.verify(spy, Mockito.never())
                    .scheduleTriggerPendingCheckpoint(
                            Mockito.any(CheckpointType.class), Mockito.anyLong());
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Regression: when {@code notifyCompleted()} fails inside {@code restoreCoordinator(true)}, the
     * method must return immediately and must NOT call {@code tryTriggerPendingCheckpoint}.
     */
    @Test
    void testRestoreCoordinatorShouldNotTriggerCheckpointWhenNotifyCompletedFails() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            CheckpointCoordinator coordinator = buildMinimalCoordinator(executorService);
            CheckpointCoordinator spy = Mockito.spy(coordinator);

            // Simulate notifyCompleted failure.
            Mockito.doReturn(false).when(spy).notifyCompleted(Mockito.any());

            // alreadyStarted=true is the branch that calls notifyCompleted.
            spy.restoreCoordinator(true);

            // tryTriggerPendingCheckpoint must NOT have been called.
            Mockito.verify(spy, Mockito.never())
                    .tryTriggerPendingCheckpoint(Mockito.any(CheckpointType.class));
        } finally {
            executorService.shutdownNow();
        }
    }
}

class TestCheckpointManager extends CheckpointManager {
    public List<TaskOperation> operations = new ArrayList<>();

    public TestCheckpointManager(
            long jobId,
            NodeEngine nodeEngine,
            Map<Integer, CheckpointPlan> checkpointPlanMap,
            CheckpointConfig checkpointConfig,
            CheckpointStorage checkpointStorage,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            CheckpointMonitorService checkpointMonitorService) {
        super(
                jobId,
                false,
                nodeEngine,
                null,
                checkpointPlanMap,
                checkpointConfig,
                checkpointStorage,
                executorService,
                runningJobStateIMap,
                checkpointMonitorService);
    }

    @Override
    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        this.operations.add(operation);
        return null;
    }
}
