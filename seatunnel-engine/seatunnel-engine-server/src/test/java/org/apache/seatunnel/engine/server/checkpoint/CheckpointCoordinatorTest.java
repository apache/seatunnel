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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
                        nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE));
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
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE)) {

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
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE)) {
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
                            nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE)) {

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
                        nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE));

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
                        false);

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
            IMap<Object, Object> runningJobStateIMap) {
        super(
                jobId,
                false,
                nodeEngine,
                null,
                checkpointPlanMap,
                checkpointConfig,
                checkpointStorage,
                executorService,
                runningJobStateIMap);
    }

    @Override
    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        this.operations.add(operation);
        return null;
    }
}
