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
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises restore diagnostic ownership through the pipeline lifecycle, including stale callbacks,
 * rejected scheduling and the race between checkpoint storage and timeout failure.
 */
class CheckpointRestoreProgressConcurrencyTest {
    private final List<Runnable> callbacks = new CopyOnWriteArrayList<>();
    private final TaskLocation task = new TaskLocation(new TaskGroupLocation(1L, 1, 1), 1, 1);
    private ExecutorService executor;
    private CheckpointConfig config;
    private CheckpointCoordinator coordinator;
    private CheckpointManager manager;
    private SubPlan pipeline;
    private ScheduledExecutorService scheduler;
    private CheckpointStorage storage;
    private final Map<Object, Object> states = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(2);
        config = new CheckpointConfig();
        config.setStorage(new CheckpointStorageConfig());
        config.setRestoreProgressTimeout(60_000);
        config.setRestoreProgressFailFast(true);

        pipeline = mock(SubPlan.class, Mockito.CALLS_REAL_METHODS);
        doReturn(PipelineStatus.RUNNING).when(pipeline).getPipelineState();
        doReturn(new PipelineLocation(1L, 1)).when(pipeline).getPipelineLocation();
        doNothing().when(pipeline).handleCheckpointError();
        PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
        when(physicalPlan.getPipelineList()).thenReturn(Collections.singletonList(pipeline));
        JobMaster master = mock(JobMaster.class, Mockito.CALLS_REAL_METHODS);
        ReflectionUtils.setField(master, "physicalPlan", physicalPlan);
        manager = mock(CheckpointManager.class, Mockito.CALLS_REAL_METHODS);
        ReflectionUtils.setField(manager, "jobMaster", master);
        InvocationFuture<?> invocation = mock(InvocationFuture.class, Mockito.CALLS_REAL_METHODS);
        invocation.obtrudeValue(null);
        doReturn(invocation).when(manager).sendOperationToMemberNode(any());

        IMap<Object, Object> stateMap = mock(IMap.class);
        states.put("checkpoint_state_1_1", CheckpointCoordinatorStatus.RUNNING);
        when(stateMap.get(any()))
                .thenAnswer(invocationOnMock -> states.get(invocationOnMock.getArgument(0)));
        doAnswer(
                        invocationOnMock ->
                                states.put(
                                        invocationOnMock.getArgument(0),
                                        invocationOnMock.getArgument(1)))
                .when(stateMap)
                .set(any(), any());
        when(stateMap.remove(any()))
                .thenAnswer(invocationOnMock -> states.remove(invocationOnMock.getArgument(0)));
        storage = mock(CheckpointStorage.class);
        coordinator =
                new CheckpointCoordinator(
                        manager,
                        storage,
                        config,
                        1L,
                        CheckpointPlan.builder()
                                .pipelineId(1)
                                .pipelineSubtasks(Collections.singleton(task))
                                .startingSubtasks(Collections.singleton(task))
                                .build(),
                        mock(CheckpointIDCounter.class),
                        null,
                        executor,
                        stateMap,
                        false,
                        null);
        coordinator = Mockito.spy(coordinator);
        doReturn(new InvocationFuture<?>[0]).when(coordinator).notifyTaskStart();
        currentScheduler().shutdownNow();
        scheduler = mock(ScheduledExecutorService.class);
        doAnswer(
                        invocationOnMock -> {
                            callbacks.add(invocationOnMock.getArgument(0));
                            return mock(ScheduledFuture.class);
                        })
                .when(scheduler)
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        ReflectionUtils.setField(coordinator, "scheduler", scheduler);
        armWindow();
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.cleanPendingCheckpoint(CheckpointCloseReason.PIPELINE_END);
            currentScheduler().shutdownNow();
        }
        executor.shutdownNow();
    }

    @Test
    void oldCallbackCannotFailANewRestore() {
        Runnable staleCallback = callbacks.get(0);

        coordinator.restoreCoordinator(false);
        long restoreTimestamp = coordinator.getLastRestoreTimestamp().get();
        staleCallback.run();

        assertHealthyRestore(restoreTimestamp);
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void oldReadinessCallbackCannotFailCheckpointPhase() {
        Runnable readinessCallback = callbacks.get(0);
        coordinator.reportedTask(
                new TaskReportStatusOperation(task, SeaTunnelTaskState.READY_START));
        await().atMost(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                coordinator.getAllTasksReadyAfterRestoreTimestamp().get() > 0
                                        && callbacks.size() >= 2);

        readinessCallback.run();
        assertFalse(coordinator.getRestoreProgressStalled().get());
        verify(pipeline, never()).handleCheckpointError();

        callbacks.get(1).run();
        assertTrue(coordinator.getRestoreProgressStalled().get());
        verify(pipeline, times(1)).handleCheckpointError();
        callbacks.get(1).run();
        verify(pipeline, times(1)).handleCheckpointError();
    }

    @Test
    void completedCheckpointInvalidatesScheduledCallback() {
        Runnable staleCallback = callbacks.get(0);
        CompletedCheckpoint completed = mock(CompletedCheckpoint.class);
        when(completed.getCompletedTimestamp()).thenReturn(System.currentTimeMillis());
        ReflectionUtils.invoke(
                coordinator,
                "markPostRestoreCheckpointProgress",
                new Class<?>[] {CompletedCheckpoint.class},
                new Object[] {completed});

        staleCallback.run();

        assertFalse(coordinator.getRestoreProgressTracking().get());
        assertFalse(coordinator.getRestoreProgressStalled().get());
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void cleanupInvalidatesScheduledCallback() {
        Runnable staleCallback = callbacks.get(0);
        coordinator.cleanPendingCheckpoint(CheckpointCloseReason.PIPELINE_END);

        staleCallback.run();

        assertFalse(coordinator.getRestoreProgressTracking().get());
        assertFalse(coordinator.getRestoreProgressStalled().get());
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void rejectedDiagnosticScheduleDoesNotAbortTaskStart() {
        doAnswer(
                        invocation -> {
                            throw new RejectedExecutionException(
                                    "diagnostic scheduler unavailable");
                        })
                .when(scheduler)
                .schedule(any(Runnable.class), eq(60_000L), eq(TimeUnit.MILLISECONDS));

        coordinator.reportedTask(
                new TaskReportStatusOperation(task, SeaTunnelTaskState.READY_START));

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            verify(coordinator).notifyTaskStart();
                            verify(scheduler)
                                    .schedule(
                                            any(Runnable.class),
                                            eq(config.getCheckpointInterval()),
                                            eq(TimeUnit.MILLISECONDS));
                        });
        assertFalse(coordinator.getRestoreProgressTracking().get());
        assertFalse(coordinator.getRestoreProgressStalled().get());
        assertFalse(coordinatorFuture().isDone());
        verify(manager, never()).handleCheckpointError(anyInt(), Mockito.anyBoolean());
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void timeoutWaitingForPipelineCannotFailRestoreHoldingPipelineMonitor() throws Exception {
        Runnable staleCallback = callbacks.get(0);
        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        Future<?> timeout;
        synchronized (pipeline) {
            timeout =
                    executor.submit(
                            () -> {
                                callbackThread.set(Thread.currentThread());
                                entered.countDown();
                                staleCallback.run();
                            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> callbackThread.get().getState() == Thread.State.BLOCKED);

            coordinator.restoreCoordinator(false);
        }
        timeout.get(5, TimeUnit.SECONDS);

        assertHealthyRestore(coordinator.getLastRestoreTimestamp().get());
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void delayedTimeoutCleanupCannotClearNewRestore() {
        doAnswer(
                        invocation -> {
                            boolean failed = (Boolean) invocation.callRealMethod();
                            assertTrue(failed);
                            coordinator.restoreCoordinator(false);
                            return true;
                        })
                .when(manager)
                .handleRestoreProgressTimeout(eq(1), any());

        callbacks.get(0).run();

        assertHealthyRestore(coordinator.getLastRestoreTimestamp().get());
        assertFalse(currentScheduler().isShutdown());
        verify(pipeline, times(1)).handleCheckpointError();
    }

    @Test
    void terminalPipelineRejectsTimeoutFailure() {
        doReturn(PipelineStatus.FINISHED).when(pipeline).getPipelineState();

        callbacks.get(0).run();

        assertFalse(coordinator.getRestoreProgressStalled().get());
        assertFalse(coordinatorFuture().isDone());
        verify(pipeline, never()).handleCheckpointError();
    }

    @Test
    void timeoutDuringCheckpointStorageCannotBeOverwrittenBySuccess() throws Exception {
        completeCheckpointWhileTimeoutRuns(true);
    }

    @Test
    void warningDuringCheckpointStorageStillAllowsSuccess() throws Exception {
        completeCheckpointWhileTimeoutRuns(false);
    }

    @Test
    void realTimerReachesPipelineFailure() {
        config.setRestoreProgressTimeout(30);
        coordinator.restoreCoordinator(false);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(coordinator.getRestoreProgressStalled().get());
                            assertTrue(coordinatorFuture().isDone());
                            assertEquals(
                                    CheckpointCoordinatorStatus.FAILED,
                                    coordinatorFuture().join().getCheckpointCoordinatorStatus());
                            verify(pipeline, times(1)).handleCheckpointError();
                        });
    }

    @SuppressWarnings("unchecked")
    private void completeCheckpointWhileTimeoutRuns(boolean failFast) throws Exception {
        config.setRestoreProgressFailFast(failFast);
        CountDownLatch storing = new CountDownLatch(1);
        CountDownLatch releaseStorage = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            storing.countDown();
                            assertTrue(releaseStorage.await(5, TimeUnit.SECONDS));
                            return null;
                        })
                .when(storage)
                .storeCheckPoint(any());
        Map<Long, PendingCheckpoint> pending =
                (Map<Long, PendingCheckpoint>)
                        ReflectionUtils.getField(coordinator, "pendingCheckpoints").get();
        pending.put(7L, mock(PendingCheckpoint.class));
        ((AtomicInteger) ReflectionUtils.getField(coordinator, "pendingCounter").get()).set(1);
        long now = System.currentTimeMillis();
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        1L,
                        1,
                        7L,
                        now,
                        CheckpointType.SAVEPOINT_TYPE,
                        now,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        Future<?> completion =
                executor.submit(() -> coordinator.completePendingCheckpoint(checkpoint));
        try {
            assertTrue(storing.await(5, TimeUnit.SECONDS));
            callbacks.get(0).run();
            if (failFast) {
                assertEquals(
                        CheckpointCoordinatorStatus.FAILED,
                        coordinatorFuture().join().getCheckpointCoordinatorStatus());
            } else {
                assertFalse(coordinatorFuture().isDone());
            }
        } finally {
            releaseStorage.countDown();
        }
        completion.get(5, TimeUnit.SECONDS);

        CheckpointCoordinatorStatus expected =
                failFast ? CheckpointCoordinatorStatus.FAILED : CheckpointCoordinatorStatus.SUSPEND;
        assertEquals(expected, states.get("checkpoint_state_1_1"));
        assertEquals(expected, coordinatorFuture().join().getCheckpointCoordinatorStatus());
        assertTrue(pending.isEmpty());
        verify(pipeline, times(failFast ? 1 : 0)).handleCheckpointError();
        verify(manager, times(failFast ? 0 : 1)).sendOperationToMemberNode(any());
    }

    private void armWindow() {
        ReflectionUtils.invoke(
                coordinator,
                "startRestoreProgressTracking",
                new Class<?>[] {boolean.class},
                new Object[] {false});
    }

    private void assertHealthyRestore(long restoreTimestamp) {
        assertEquals(restoreTimestamp, coordinator.getLastRestoreTimestamp().get());
        assertTrue(coordinator.getRestoreProgressTracking().get());
        assertFalse(coordinator.getRestoreProgressStalled().get());
        assertEquals(0, coordinator.getRestoreStalledTimestamp().get());
        assertFalse(coordinatorFuture().isDone());
    }

    private ScheduledExecutorService currentScheduler() {
        return (ScheduledExecutorService) ReflectionUtils.getField(coordinator, "scheduler").get();
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<CheckpointCoordinatorState> coordinatorFuture() {
        return (CompletableFuture<CheckpointCoordinatorState>)
                ReflectionUtils.getField(coordinator, "checkpointCoordinatorFuture").get();
    }
}
