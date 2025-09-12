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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    void testACKNotExistPendingCheckpoint() throws CheckpointStorageException {
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
            throws CheckpointStorageException, ExecutionException, InterruptedException,
                    TimeoutException {
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
            throws CheckpointStorageException, ExecutionException, InterruptedException,
                    TimeoutException {
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
    void testCheckpointMinPause()
            throws CheckpointStorageException, ExecutionException, InterruptedException,
                    TimeoutException {
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
}
