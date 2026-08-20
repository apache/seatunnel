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

package org.apache.seatunnel.engine.server.task.operation.checkpoint;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.record.Barrier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class BarrierFlowOperationTest {

    @Test
    void testBarrierOperationWaitsForReaderBarrierProcessing() throws Exception {
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 1, 1L);
        TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 1L, 1);
        Barrier barrier =
                new CheckpointBarrier(
                        1L, System.currentTimeMillis(), CheckpointType.CHECKPOINT_TYPE);

        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        TaskGroup taskGroup = Mockito.mock(TaskGroup.class);
        Task task = Mockito.mock(Task.class);
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        CompletableFuture<Void> barrierProcessingFuture = new CompletableFuture<>();
        AtomicReference<Runnable> submittedBarrier = new AtomicReference<>();

        Mockito.when(server.getTaskExecutionService()).thenReturn(taskExecutionService);
        Mockito.when(taskExecutionService.getExecutionContext(taskGroupLocation))
                .thenReturn(new TaskGroupContext(taskGroup, null, null));
        Mockito.when(taskGroup.getTask(taskLocation.getTaskID())).thenReturn(task);
        Mockito.when(task.getExecutionContext()).thenReturn(taskExecutionContext);
        Mockito.when(taskExecutionContext.getTaskExecutionService())
                .thenReturn(taskExecutionService);
        Mockito.when(
                        taskExecutionService.asyncExecuteFunction(
                                Mockito.eq(taskGroupLocation), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            submittedBarrier.set(invocation.getArgument(1));
                            return barrierProcessingFuture;
                        });

        BarrierFlowOperation operation = new BarrierFlowOperation(barrier, taskLocation);
        operation.setService(server);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            Future<?> operationFuture =
                    executorService.submit(
                            () -> {
                                operation.runInternal();
                                return null;
                            });

            await().atMost(5, TimeUnit.SECONDS).until(() -> submittedBarrier.get() != null);
            Assertions.assertFalse(operationFuture.isDone());

            submittedBarrier.get().run();
            Mockito.verify(task).triggerBarrier(barrier);
            Assertions.assertFalse(operationFuture.isDone());

            barrierProcessingFuture.complete(null);
            operationFuture.get(5, TimeUnit.SECONDS);
        } finally {
            executorService.shutdownNow();
        }
    }
}
