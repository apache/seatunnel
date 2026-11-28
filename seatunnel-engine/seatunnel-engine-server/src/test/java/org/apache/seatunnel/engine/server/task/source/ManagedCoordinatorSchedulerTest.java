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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.api.source.scheduler.AsyncTaskKey;
import org.apache.seatunnel.engine.server.TaskExecutionService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ManagedCoordinatorSchedulerTest {

    @Test
    void shouldReplacePendingTimerWithSameKey() {
        TaskExecutionService executionService = Mockito.mock(TaskExecutionService.class);
        List<Runnable> timerCallbacks = new ArrayList<>();
        List<ScheduledFuture<?>> timerFutures = new ArrayList<>();
        Mockito.when(
                        executionService.scheduleManagedSourceCoordinatorTimer(
                                Mockito.any(Runnable.class), Mockito.anyLong()))
                .thenAnswer(
                        invocation -> {
                            timerCallbacks.add(invocation.getArgument(0));
                            ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
                            timerFutures.add(future);
                            return future;
                        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger wakeups = new AtomicInteger();
        ManagedCoordinatorScheduler scheduler =
                new ManagedCoordinatorScheduler(
                        executionService,
                        "epoch",
                        getClass().getClassLoader(),
                        2,
                        16,
                        4,
                        failure::set,
                        ignored -> wakeups.incrementAndGet());
        AtomicInteger executions = new AtomicInteger();
        AsyncTaskKey key = AsyncTaskKey.of("discovery-tick");

        scheduler.scheduleInCoordinatorThread(
                key, Duration.ofSeconds(1), () -> executions.addAndGet(1));
        scheduler.scheduleInCoordinatorThread(
                key, Duration.ofSeconds(1), () -> executions.addAndGet(10));

        Mockito.verify(timerFutures.get(0)).cancel(false);
        timerCallbacks.get(0).run();
        timerCallbacks.get(1).run();
        Assertions.assertEquals(2, wakeups.get());
        Assertions.assertTrue(scheduler.drainOneCallback());
        Assertions.assertTrue(scheduler.drainOneCallback());
        Assertions.assertEquals(10, executions.get());
        Assertions.assertNull(failure.get());

        scheduler.close();
    }

    @Test
    void shouldReconcileCancellationWithoutUsingCallbackCapacity() {
        TaskExecutionService executionService = Mockito.mock(TaskExecutionService.class);
        Future<?> workerFuture = Mockito.mock(Future.class);
        Mockito.doReturn(workerFuture)
                .when(executionService)
                .submitManagedSourceAsync(
                        Mockito.any(), Mockito.any(java.util.concurrent.Callable.class));
        Mockito.when(
                        executionService.scheduleManagedSourceCoordinatorTimer(
                                Mockito.any(Runnable.class), Mockito.anyLong()))
                .thenReturn(Mockito.mock(ScheduledFuture.class));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ManagedCoordinatorScheduler scheduler =
                new ManagedCoordinatorScheduler(
                        executionService,
                        "epoch",
                        getClass().getClassLoader(),
                        1,
                        2,
                        1,
                        failure::set,
                        ignored -> {});

        scheduler
                .callAsync(
                        AsyncTaskKey.of("cancelled"),
                        () -> "unused",
                        (result, error) -> Assertions.fail("Cancelled callback must not run"),
                        org.apache.seatunnel.api.source.scheduler.AsyncTaskOptions.defaults())
                .cancel();

        Assertions.assertEquals(1, scheduler.runningCount());
        Assertions.assertTrue(scheduler.hasPendingCallbacks());
        Assertions.assertTrue(scheduler.drainOneCallback());
        Assertions.assertEquals(0, scheduler.runningCount());
        Assertions.assertFalse(scheduler.hasPendingCallbacks());
        Assertions.assertNull(failure.get());

        scheduler.close();
    }
}
