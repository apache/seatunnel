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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
}
