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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.flow.AbstractFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SeaTunnelTaskStateTest {

    private SeaTunnelTask task;
    private TaskExecutionService mockTaskExecutionService;
    private TaskExecutionContext mockTaskExecutionContext;
    private SourceFlowLifeCycle<?, ?> sourceLifeCycle;

    @BeforeEach
    void setUp() throws Exception {
        task = mock(SeaTunnelTask.class, Mockito.CALLS_REAL_METHODS);

        mockTaskExecutionService = mock(TaskExecutionService.class);
        when(mockTaskExecutionService.registerTimerFlushTask(any(), any(), anyLong()))
                .thenReturn(mock(ScheduledFuture.class));

        mockTaskExecutionContext = mock(TaskExecutionContext.class);
        when(mockTaskExecutionContext.getTaskExecutionService())
                .thenReturn(mockTaskExecutionService);
        when(mockTaskExecutionContext.registerTimerFlushTask(any(), any(), anyLong()))
                .thenReturn(mock(ScheduledFuture.class));

        sourceLifeCycle = mock(SourceFlowLifeCycle.class);
        doCallRealMethod().when(sourceLifeCycle).hook();
        setField(AbstractFlowLifeCycle.class, "runningTask", sourceLifeCycle, task);
        setField(SourceFlowLifeCycle.class, "flushIntervalMs", sourceLifeCycle, 200L);
        setField(
                SourceFlowLifeCycle.class,
                "currentTaskLocation",
                sourceLifeCycle,
                mock(TaskLocation.class));

        when(task.getExecutionContext()).thenReturn(mockTaskExecutionContext);

        List<FlowLifeCycle> cycles = new ArrayList<>();
        cycles.add(sourceLifeCycle);

        setField(AbstractTask.class, "progress", task, new Progress());
        setField(AbstractTask.class, "startCalled", task, false);
        setField(AbstractTask.class, "closeCalled", task, false);
        setField(AbstractTask.class, "prepareCloseStatus", task, false);

        CompletableFuture<Void> restoreComplete = new CompletableFuture<>();
        restoreComplete.complete(null);
        setField(AbstractTask.class, "restoreComplete", task, restoreComplete);

        setField(SeaTunnelTask.class, "currState", task, SeaTunnelTaskState.INIT);
        setField(SeaTunnelTask.class, "allCycles", task, cycles);

        doNothing().when(task).reportTaskStatus(any());
        doNothing().when(task).collect();
        doNothing().when(task).close();
    }

    // ==================== State Machine Transition Tests ====================

    @Test
    void testFullStateMachineTransition() throws Exception {
        Assertions.assertEquals(SeaTunnelTaskState.INIT, getState());

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.WAITING_RESTORE, getState());

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.READY_START, getState());

        task.startCall();
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.STARTING, getState());

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());

        setField(AbstractTask.class, "prepareCloseStatus", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.PREPARE_CLOSE, getState());

        setField(AbstractTask.class, "closeCalled", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.CLOSED, getState());
    }

    @Test
    void testInitTransitionsToWaitingRestore() throws Exception {
        Assertions.assertEquals(SeaTunnelTaskState.INIT, getState());
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.WAITING_RESTORE, getState());
        verify(task, times(1)).reportTaskStatus(SeaTunnelTaskState.WAITING_RESTORE);
    }

    @Test
    void testWaitingRestoreBlocksWhenRestoreNotDone() throws Exception {
        CompletableFuture<Void> pendingRestore = new CompletableFuture<>();
        setField(AbstractTask.class, "restoreComplete", task, pendingRestore);

        advanceTo(SeaTunnelTaskState.WAITING_RESTORE);

        task.stateProcess();
        Assertions.assertEquals(
                SeaTunnelTaskState.WAITING_RESTORE,
                getState(),
                "Should stay in WAITING_RESTORE when restore is not complete");
        verify(sourceLifeCycle, never()).open();
    }

    @Test
    void testWaitingRestoreToReadyStartCallsOpen() throws Exception {
        advanceTo(SeaTunnelTaskState.WAITING_RESTORE);

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.READY_START, getState());
        verify(sourceLifeCycle, times(1)).open();
        verify(task, times(1)).reportTaskStatus(SeaTunnelTaskState.READY_START);
    }

    @Test
    void testReadyStartBlocksUntilStartCalled() throws Exception {
        advanceTo(SeaTunnelTaskState.READY_START);

        task.stateProcess();
        Assertions.assertEquals(
                SeaTunnelTaskState.READY_START,
                getState(),
                "Should stay in READY_START when startCalled is false");

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.READY_START, getState());
    }

    @Test
    void testReadyStartToStartingAfterStartCall() throws Exception {
        advanceTo(SeaTunnelTaskState.READY_START);

        task.startCall();
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.STARTING, getState());
    }

    @Test
    void testStartingToRunningCallsHook() throws Exception {
        advanceTo(SeaTunnelTaskState.STARTING);

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(sourceLifeCycle, times(1)).hook();
    }

    @Test
    void testRunningCallsCollect() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(task, times(1)).collect();
    }

    @Test
    void testRunningStaysWhenPrepareCloseIsFalse() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);

        task.stateProcess();
        task.stateProcess();
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(task, times(3)).collect();
    }

    @Test
    void testRunningToPrepareCloseWhenFlagSet() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);

        setField(AbstractTask.class, "prepareCloseStatus", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.PREPARE_CLOSE, getState());
    }

    @Test
    void testPrepareCloseBlocksUntilCloseCalled() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);
        setField(AbstractTask.class, "prepareCloseStatus", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.PREPARE_CLOSE, getState());

        task.stateProcess();
        Assertions.assertEquals(
                SeaTunnelTaskState.PREPARE_CLOSE,
                getState(),
                "Should stay in PREPARE_CLOSE when closeCalled is false");
    }

    @Test
    void testPrepareCloseToClosedWhenCloseCalledSet() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);
        setField(AbstractTask.class, "prepareCloseStatus", task, true);
        task.stateProcess();

        setField(AbstractTask.class, "closeCalled", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.CLOSED, getState());
    }

    @Test
    void testClosedCallsCloseAndMarksDone() throws Exception {
        advanceTo(SeaTunnelTaskState.RUNNING);
        setField(AbstractTask.class, "prepareCloseStatus", task, true);
        task.stateProcess();
        setField(AbstractTask.class, "closeCalled", task, true);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.CLOSED, getState());

        Progress progress = getProgress();
        task.stateProcess();
        Assertions.assertTrue(progress.toState().isDone(), "Progress should be marked done");
        verify(task, times(1)).close();
    }

    @Test
    void testCancellingToCancel() throws Exception {
        setField(SeaTunnelTask.class, "currState", task, SeaTunnelTaskState.CANCELLING);

        Progress progress = getProgress();
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.CANCELED, getState());
        Assertions.assertTrue(progress.toState().isDone(), "Progress should be marked done");
        verify(task, times(1)).close();
    }

    // ==================== Timer Registration Tests ====================

    /**
     * Verifies that {@code SourceFlowLifeCycle.hook()} → {@code startFlushTimer()} → {@code
     * registerTimerFlushTask()} is called exactly once, and only during the STARTING → RUNNING
     * transition.
     */
    @Test
    void testTimerRegistrationOnlyAtStartingToRunning() throws Exception {
        // INIT → WAITING_RESTORE
        task.stateProcess();
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());

        // WAITING_RESTORE → READY_START
        task.stateProcess();
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());

        // READY_START stays (startCalled = false)
        task.stateProcess();
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());

        task.startCall();
        // READY_START → STARTING
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.STARTING, getState());
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());

        // STARTING → RUNNING (hook → startFlushTimer → registerTimerFlushTask)
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(mockTaskExecutionContext, times(1)).registerTimerFlushTask(any(), any(), anyLong());

        // Multiple RUNNING iterations — registerTimerFlushTask stays at 1
        task.stateProcess();
        task.stateProcess();
        task.stateProcess();
        verify(mockTaskExecutionContext, times(1)).registerTimerFlushTask(any(), any(), anyLong());
    }

    @Test
    void testTimerNotRegisteredBeforeRunning() throws Exception {
        advanceTo(SeaTunnelTaskState.READY_START);

        task.stateProcess();
        task.stateProcess();

        Assertions.assertEquals(SeaTunnelTaskState.READY_START, getState());
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());
    }

    @Test
    void testTimerNotRegisteredWhenFlushIntervalIsZero() throws Exception {
        setField(SourceFlowLifeCycle.class, "flushIntervalMs", sourceLifeCycle, 0L);

        advanceTo(SeaTunnelTaskState.STARTING);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());
    }

    @Test
    void testTimerNotRegisteredWhenFlushIntervalIsNegative() throws Exception {
        setField(SourceFlowLifeCycle.class, "flushIntervalMs", sourceLifeCycle, -1L);

        advanceTo(SeaTunnelTaskState.STARTING);
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(mockTaskExecutionContext, never()).registerTimerFlushTask(any(), any(), anyLong());
    }

    @Test
    void testTimerRegisteredWithCorrectInterval() throws Exception {
        long expectedInterval = 500L;
        setField(SourceFlowLifeCycle.class, "flushIntervalMs", sourceLifeCycle, expectedInterval);

        advanceTo(SeaTunnelTaskState.STARTING);
        task.stateProcess();

        verify(mockTaskExecutionContext, times(1))
                .registerTimerFlushTask(
                        any(TaskLocation.class), any(Runnable.class), Mockito.eq(expectedInterval));
    }

    // ==================== reportTaskStatus Order Tests ====================

    @Test
    void testReportTaskStatusCalledInOrder() throws Exception {
        InOrder ordered = inOrder(task);

        // INIT → WAITING_RESTORE
        task.stateProcess();
        ordered.verify(task).reportTaskStatus(SeaTunnelTaskState.WAITING_RESTORE);

        // WAITING_RESTORE → READY_START
        task.stateProcess();
        ordered.verify(task).reportTaskStatus(SeaTunnelTaskState.READY_START);
    }

    @Test
    void testReportStatusNotCalledOnReadyStartToStarting() throws Exception {
        advanceTo(SeaTunnelTaskState.READY_START);
        Mockito.clearInvocations(task);

        task.startCall();
        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.STARTING, getState());
        verify(task, never()).reportTaskStatus(any());
    }

    @Test
    void testReportStatusNotCalledOnStartingToRunning() throws Exception {
        advanceTo(SeaTunnelTaskState.STARTING);
        Mockito.clearInvocations(task);

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.RUNNING, getState());
        verify(task, never()).reportTaskStatus(any());
    }

    // ==================== open() / hook() Ordering Tests ====================

    @Test
    void testOpenCalledBeforeHook() throws Exception {
        InOrder ordered = inOrder(sourceLifeCycle);

        advanceTo(SeaTunnelTaskState.RUNNING);

        ordered.verify(sourceLifeCycle).open();
        ordered.verify(sourceLifeCycle).hook();
    }

    @Test
    void testHookNotCalledDuringOpen() throws Exception {
        advanceTo(SeaTunnelTaskState.WAITING_RESTORE);

        task.stateProcess();
        Assertions.assertEquals(SeaTunnelTaskState.READY_START, getState());

        verify(sourceLifeCycle, times(1)).open();
        verify(sourceLifeCycle, never()).hook();
    }

    // ==================== Multiple FlowLifeCycle Tests ====================

    @Test
    void testMultipleCyclesAllReceiveOpenAndHook() throws Exception {
        FlowLifeCycle secondCycle = mock(FlowLifeCycle.class);
        List<FlowLifeCycle> cycles = new ArrayList<>();
        cycles.add(sourceLifeCycle);
        cycles.add(secondCycle);
        setField(SeaTunnelTask.class, "allCycles", task, cycles);

        advanceTo(SeaTunnelTaskState.RUNNING);

        verify(sourceLifeCycle, times(1)).open();
        verify(secondCycle, times(1)).open();
        verify(sourceLifeCycle, times(1)).hook();
        verify(secondCycle, times(1)).hook();
    }

    private void advanceTo(SeaTunnelTaskState target) throws Exception {
        if (target == SeaTunnelTaskState.INIT) {
            return;
        }

        // INIT → WAITING_RESTORE
        task.stateProcess();
        if (target == SeaTunnelTaskState.WAITING_RESTORE) {
            return;
        }

        // WAITING_RESTORE → READY_START
        task.stateProcess();
        if (target == SeaTunnelTaskState.READY_START) {
            return;
        }

        // READY_START → STARTING
        task.startCall();
        task.stateProcess();
        if (target == SeaTunnelTaskState.STARTING) {
            return;
        }

        // STARTING → RUNNING
        task.stateProcess();
        if (target == SeaTunnelTaskState.RUNNING) {
            return;
        }

        throw new IllegalArgumentException("advanceTo does not support state: " + target);
    }

    private SeaTunnelTaskState getState() throws Exception {
        Field f = SeaTunnelTask.class.getDeclaredField("currState");
        f.setAccessible(true);
        return (SeaTunnelTaskState) f.get(task);
    }

    private Progress getProgress() throws Exception {
        Field f = AbstractTask.class.getDeclaredField("progress");
        f.setAccessible(true);
        return (Progress) f.get(task);
    }

    private static void setField(Class<?> clazz, String name, Object target, Object value)
            throws Exception {
        Field f = clazz.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
