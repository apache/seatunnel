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

import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointIDCounter;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class CheckpointCoordinatorScheduledTriggerTest {

    @Test
    void unexpectedPeriodicTriggerErrorFailsCoordinator() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        CheckpointManager checkpointManager = Mockito.mock(CheckpointManager.class);
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setStorage(new CheckpointStorageConfig());
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
        CheckpointPlan plan =
                CheckpointPlan.builder()
                        .pipelineId(1)
                        .pipelineSubtasks(Collections.singleton(taskLocation))
                        .startingSubtasks(Collections.singleton(taskLocation))
                        .build();
        @SuppressWarnings("unchecked")
        IMap<Object, Object> runningJobStateIMap = Mockito.mock(IMap.class);
        Mockito.when(runningJobStateIMap.get(Mockito.any()))
                .thenReturn(CheckpointCoordinatorStatus.RUNNING);
        CheckpointCoordinator coordinator =
                Mockito.spy(
                        new CheckpointCoordinator(
                                checkpointManager,
                                Mockito.mock(CheckpointStorage.class),
                                checkpointConfig,
                                1L,
                                plan,
                                Mockito.mock(CheckpointIDCounter.class),
                                null,
                                executorService,
                                runningJobStateIMap,
                                false,
                                null));
        try {
            Mockito.doThrow(new IllegalStateException("periodic trigger failed"))
                    .when(coordinator)
                    .tryTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE);

            coordinator.scheduleTriggerPendingCheckpoint(CheckpointType.CHECKPOINT_TYPE, 0);

            CheckpointCoordinatorState state =
                    coordinator.waitCheckpointCoordinatorComplete().get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    CheckpointCoordinatorStatus.FAILED, state.getCheckpointCoordinatorStatus());
            Assertions.assertTrue(state.getThrowableMsg().contains("periodic trigger failed"));
            Mockito.verify(checkpointManager, Mockito.timeout(5000))
                    .handleCheckpointError(1, false);
        } finally {
            coordinator.cancelCheckpoint();
            executorService.shutdownNow();
        }
    }
}
