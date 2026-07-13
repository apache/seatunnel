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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.server.execution.ExecutionState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that distributed state updates preserve concurrent winners and publish transition side
 * effects only for successful compare-and-set operations.
 */
class DistributedStateTransitionTest {

    private static final Object STATE_KEY = "state-key";

    @Test
    void testMissingStateUsesAtomicInsert() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.FAILED)).thenReturn(null);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertTrue(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.FAILED, result.getCurrentState());
        verify(stateMap).putIfAbsent(STATE_KEY, ExecutionState.FAILED);
        verify(stateMap, never()).set(STATE_KEY, ExecutionState.FAILED);
        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertNotNull(timestamps.getValue()[ExecutionState.FAILED.ordinal()]);
        InOrder persistenceOrder = inOrder(timestampMap, stateMap);
        persistenceOrder.verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        persistenceOrder.verify(stateMap).putIfAbsent(STATE_KEY, ExecutionState.FAILED);
    }

    @Test
    void testConcurrentTerminalStateWinsMissingStateRace() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.FAILED))
                .thenReturn(ExecutionState.CANCELED);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.CANCELED, result.getCurrentState());
        verify(stateMap, never()).set(STATE_KEY, ExecutionState.FAILED);
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap).remove(STATE_KEY);
    }

    @Test
    void testConcurrentTerminalStateWinsCompareAndSetRace() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY))
                .thenReturn(ExecutionState.RUNNING)
                .thenReturn(ExecutionState.CANCELED);
        when(stateMap.replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED))
                .thenReturn(false);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.CANCELED, result.getCurrentState());
        verify(stateMap, never()).set(STATE_KEY, ExecutionState.FAILED);
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap).remove(STATE_KEY);
    }

    @Test
    void testPendingCleanupBlocksMissingStateRecreation() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, false);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertTrue(result.isPersistenceBlocked());
        verify(stateMap, never()).putIfAbsent(STATE_KEY, ExecutionState.FAILED);
        verify(timestampMap, never()).set(eq(STATE_KEY), any(Long[].class));
    }

    @Test
    void testGenerationFenceWinsBeforeDifferentTerminalState() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.CANCELED);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, false);

        Assertions.assertTrue(result.isPersistenceBlocked());
        Assertions.assertEquals(ExecutionState.CANCELED, result.getCurrentState());
        verify(timestampMap, never()).get(STATE_KEY);
        verify(timestampMap, never()).set(eq(STATE_KEY), any(Long[].class));
    }

    @Test
    void testExistingStateTransitionDoesNotTakeMissingKeyFence() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Runnable lockMissingStatePersistence = mock(Runnable.class);
        Runnable unlockMissingStatePersistence = mock(Runnable.class);
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.RUNNING);
        when(stateMap.replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED))
                .thenReturn(true);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(
                        stateMap,
                        timestampMap,
                        true,
                        lockMissingStatePersistence,
                        unlockMissingStatePersistence);

        Assertions.assertTrue(result.isTransitioned());
        verify(lockMissingStatePersistence, never()).run();
        verify(unlockMissingStatePersistence, never()).run();
        verify(stateMap).lock(STATE_KEY);
        verify(stateMap).unlock(STATE_KEY);
    }

    @Test
    void testMissingStateTransitionTakesFenceBeforeReacquiringStateLock() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Runnable lockMissingStatePersistence = mock(Runnable.class);
        Runnable unlockMissingStatePersistence = mock(Runnable.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.FAILED)).thenReturn(null);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(
                        stateMap,
                        timestampMap,
                        true,
                        lockMissingStatePersistence,
                        unlockMissingStatePersistence);

        Assertions.assertTrue(result.isTransitioned());
        verify(lockMissingStatePersistence).run();
        verify(unlockMissingStatePersistence).run();
        verify(stateMap, times(2)).lock(STATE_KEY);
        verify(stateMap, times(2)).unlock(STATE_KEY);
        InOrder lockOrder = inOrder(stateMap, lockMissingStatePersistence);
        lockOrder.verify(stateMap).unlock(STATE_KEY);
        lockOrder.verify(lockMissingStatePersistence).run();
        lockOrder.verify(stateMap).lock(STATE_KEY);
        InOrder releaseOrder = inOrder(stateMap, unlockMissingStatePersistence);
        releaseOrder.verify(stateMap, times(2)).unlock(STATE_KEY);
        releaseOrder.verify(unlockMissingStatePersistence).run();
    }

    @Test
    void testConcurrentNonTerminalStateIsRecheckedBeforeCompareAndSet() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.FAILED))
                .thenReturn(ExecutionState.RUNNING);
        when(stateMap.replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED))
                .thenReturn(true);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertTrue(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.FAILED, result.getCurrentState());
        verify(stateMap).replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);
        verify(timestampMap, times(2)).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap).remove(STATE_KEY);
    }

    @Test
    void testUnexpectedConcurrentStateTypeFailsFast() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.FAILED)).thenReturn("invalid-state");

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> transition(stateMap, timestampMap, true));

        Assertions.assertTrue(exception.getMessage().contains(ExecutionState.class.getName()));
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap).remove(STATE_KEY);
    }

    @Test
    void testTimestampFailureLeavesStateUntouchedBeforeRetry() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        previousTimestamps[ExecutionState.RUNNING.ordinal()] = 100L;
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.RUNNING);
        when(stateMap.replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED))
                .thenReturn(true);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);
        doThrow(new RetryableHazelcastException("timestamp unavailable"))
                .doNothing()
                .when(timestampMap)
                .set(eq(STATE_KEY), any(Long[].class));

        Assertions.assertThrows(
                RetryableHazelcastException.class, () -> transition(stateMap, timestampMap, true));
        DistributedStateTransition.Result<ExecutionState> retried =
                transition(stateMap, timestampMap, true);

        Assertions.assertTrue(retried.isTransitioned());
        verify(stateMap).replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);
        verify(timestampMap, times(2)).set(eq(STATE_KEY), any(Long[].class));
    }

    @Test
    void testStateFailureRestoresTimestampBeforeRetry() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        previousTimestamps[ExecutionState.RUNNING.ordinal()] = 100L;
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.RUNNING);
        doThrow(new RetryableHazelcastException("state unavailable"))
                .doReturn(true)
                .when(stateMap)
                .replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);

        Assertions.assertThrows(
                RetryableHazelcastException.class, () -> transition(stateMap, timestampMap, true));
        DistributedStateTransition.Result<ExecutionState> retried =
                transition(stateMap, timestampMap, true);

        Assertions.assertTrue(retried.isTransitioned());
        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap, times(3)).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertSame(previousTimestamps, timestamps.getAllValues().get(1));
    }

    @Test
    void testIndeterminateTransitionRetainsTimestampWhenStateCommitted() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        AtomicReference<Object> persistedState = new AtomicReference<>(ExecutionState.RUNNING);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        when(stateMap.get(STATE_KEY)).thenAnswer(invocation -> persistedState.get());
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);
        doAnswer(
                        invocation -> {
                            persistedState.set(ExecutionState.FAILED);
                            throw new IndeterminateOperationStateException("reply lost");
                        })
                .when(stateMap)
                .replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.FAILED, result.getCurrentState());
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap, never()).remove(STATE_KEY);
    }

    @Test
    void testUnknownStateOutcomeRetainsTimestampWhenConfirmationFails() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        when(stateMap.get(STATE_KEY))
                .thenReturn(ExecutionState.RUNNING)
                .thenThrow(new RetryableHazelcastException("confirmation unavailable"));
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);
        doThrow(new RetryableHazelcastException("state write unavailable"))
                .when(stateMap)
                .replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);

        RetryableHazelcastException failure =
                Assertions.assertThrows(
                        RetryableHazelcastException.class,
                        () -> transition(stateMap, timestampMap, true));

        Assertions.assertEquals("state write unavailable", failure.getMessage());
        Assertions.assertEquals(1, failure.getSuppressed().length);
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap, never()).remove(STATE_KEY);
    }

    @Test
    void testExistingTargetStateRepairsMissingTimestamp() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.FAILED);
        when(timestampMap.get(STATE_KEY)).thenReturn(null);

        DistributedStateTransition.Result<ExecutionState> result =
                transition(stateMap, timestampMap, true);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.FAILED, result.getCurrentState());
        verify(stateMap, never()).replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);
        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertNotNull(timestamps.getValue()[ExecutionState.FAILED.ordinal()]);
        for (ExecutionState state : ExecutionState.values()) {
            if (state != ExecutionState.FAILED) {
                Assertions.assertNull(timestamps.getValue()[state.ordinal()]);
            }
        }
    }

    @Test
    void testExistingTargetTimestampRepairCanBeRetried() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.FAILED);
        when(timestampMap.get(STATE_KEY)).thenReturn(null);
        doThrow(new RetryableHazelcastException("timestamp unavailable"))
                .doNothing()
                .when(timestampMap)
                .set(eq(STATE_KEY), any(Long[].class));

        Assertions.assertThrows(
                RetryableHazelcastException.class, () -> transition(stateMap, timestampMap, true));
        DistributedStateTransition.Result<ExecutionState> retried =
                transition(stateMap, timestampMap, true);

        Assertions.assertFalse(retried.isTransitioned());
        Assertions.assertEquals(ExecutionState.FAILED, retried.getCurrentState());
        verify(timestampMap, times(2)).set(eq(STATE_KEY), any(Long[].class));
        verify(stateMap, never()).replace(STATE_KEY, ExecutionState.RUNNING, ExecutionState.FAILED);
    }

    @Test
    void testInitializationLoserDoesNotPublishCreatedTimestamp() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        previousTimestamps[ExecutionState.INITIALIZING.ordinal()] = 10L;
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.CREATED))
                .thenReturn(ExecutionState.CANCELED);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);

        ExecutionState initialized = initialize(stateMap, timestampMap);

        Assertions.assertEquals(ExecutionState.CANCELED, initialized);
        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap, times(2)).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertNotNull(
                timestamps.getAllValues().get(0)[ExecutionState.CREATED.ordinal()]);
        Assertions.assertNull(timestamps.getAllValues().get(1)[ExecutionState.CREATED.ordinal()]);
    }

    @Test
    void testInitializationPreservesExistingTimestamps() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        previousTimestamps[ExecutionState.INITIALIZING.ordinal()] = 10L;
        previousTimestamps[ExecutionState.RUNNING.ordinal()] = 20L;
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        when(stateMap.putIfAbsent(STATE_KEY, ExecutionState.CREATED)).thenReturn(null);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);

        Assertions.assertEquals(ExecutionState.CREATED, initialize(stateMap, timestampMap));

        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertEquals(
                Long.valueOf(10L), timestamps.getValue()[ExecutionState.INITIALIZING.ordinal()]);
        Assertions.assertEquals(
                Long.valueOf(20L), timestamps.getValue()[ExecutionState.RUNNING.ordinal()]);
        Assertions.assertNotNull(timestamps.getValue()[ExecutionState.CREATED.ordinal()]);
        InOrder persistenceOrder = inOrder(timestampMap, stateMap);
        persistenceOrder.verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        persistenceOrder.verify(stateMap).putIfAbsent(STATE_KEY, ExecutionState.CREATED);
    }

    @Test
    void testInitializationTimestampFailureDoesNotInsertState() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(null);
        doThrow(new RetryableHazelcastException("timestamp unavailable"))
                .when(timestampMap)
                .set(eq(STATE_KEY), any(Long[].class));

        Assertions.assertThrows(
                RetryableHazelcastException.class, () -> initialize(stateMap, timestampMap));

        verify(stateMap, never()).putIfAbsent(STATE_KEY, ExecutionState.CREATED);
        verify(timestampMap, never()).remove(STATE_KEY);
    }

    @Test
    void testIndeterminateInitializationRetainsTimestampWhenStateCommitted() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        AtomicReference<Object> persistedState = new AtomicReference<>();
        when(stateMap.get(STATE_KEY)).thenAnswer(invocation -> persistedState.get());
        doAnswer(
                        invocation -> {
                            persistedState.set(ExecutionState.CREATED);
                            throw new IndeterminateOperationStateException("reply lost");
                        })
                .when(stateMap)
                .putIfAbsent(STATE_KEY, ExecutionState.CREATED);

        Assertions.assertEquals(ExecutionState.CREATED, initialize(stateMap, timestampMap));

        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap, never()).remove(STATE_KEY);
    }

    @Test
    void testInitializationRepairsCreatedTimestampAndExpandsOldArray() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[] {null, 10L};
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.CREATED);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);

        Assertions.assertEquals(ExecutionState.CREATED, initialize(stateMap, timestampMap));

        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertTrue(timestamps.getValue().length >= ExecutionState.values().length);
        Assertions.assertEquals(Long.valueOf(10L), timestamps.getValue()[1]);
        Assertions.assertNotNull(timestamps.getValue()[ExecutionState.CREATED.ordinal()]);
    }

    @Test
    void testResetPersistsTimestampBeforeState() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.FAILED);
        when(stateMap.replace(STATE_KEY, ExecutionState.FAILED, ExecutionState.CREATED))
                .thenReturn(true);

        DistributedStateTransition.Result<ExecutionState> result =
                reset(stateMap, timestampMap, true);

        Assertions.assertTrue(result.isTransitioned());
        InOrder persistenceOrder = inOrder(timestampMap, stateMap);
        persistenceOrder.verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        persistenceOrder
                .verify(stateMap)
                .replace(STATE_KEY, ExecutionState.FAILED, ExecutionState.CREATED);
    }

    @Test
    void testResetFenceAndMissingStateDoNotPersist() {
        IMap<Object, Object> fencedStateMap = mock(IMap.class);
        IMap<Object, Long[]> fencedTimestampMap = mock(IMap.class);
        when(fencedStateMap.get(STATE_KEY)).thenReturn(ExecutionState.FAILED);

        DistributedStateTransition.Result<ExecutionState> fenced =
                reset(fencedStateMap, fencedTimestampMap, false);

        Assertions.assertTrue(fenced.isPersistenceBlocked());
        verify(fencedTimestampMap, never()).set(eq(STATE_KEY), any(Long[].class));
        verify(fencedStateMap, never())
                .replace(STATE_KEY, ExecutionState.FAILED, ExecutionState.CREATED);

        IMap<Object, Object> missingStateMap = mock(IMap.class);
        IMap<Object, Long[]> missingTimestampMap = mock(IMap.class);
        when(missingStateMap.get(STATE_KEY)).thenReturn(null);

        DistributedStateTransition.Result<ExecutionState> missing =
                reset(missingStateMap, missingTimestampMap, true);

        Assertions.assertTrue(missing.isPersistenceBlocked());
        verify(missingTimestampMap, never()).set(eq(STATE_KEY), any(Long[].class));
        verify(missingStateMap, never()).replace(eq(STATE_KEY), any(), eq(ExecutionState.CREATED));
    }

    @Test
    void testResetStateFailureRestoresTimestamp() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        previousTimestamps[ExecutionState.FAILED.ordinal()] = 100L;
        when(stateMap.get(STATE_KEY)).thenReturn(ExecutionState.FAILED);
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);
        doThrow(new RetryableHazelcastException("state unavailable"))
                .when(stateMap)
                .replace(STATE_KEY, ExecutionState.FAILED, ExecutionState.CREATED);

        Assertions.assertThrows(
                RetryableHazelcastException.class, () -> reset(stateMap, timestampMap, true));

        ArgumentCaptor<Long[]> timestamps = ArgumentCaptor.forClass(Long[].class);
        verify(timestampMap, times(2)).set(eq(STATE_KEY), timestamps.capture());
        Assertions.assertSame(previousTimestamps, timestamps.getAllValues().get(1));
    }

    @Test
    void testIndeterminateResetRetainsTimestampWhenStateCommitted() {
        IMap<Object, Object> stateMap = mock(IMap.class);
        IMap<Object, Long[]> timestampMap = mock(IMap.class);
        AtomicReference<Object> persistedState = new AtomicReference<>(ExecutionState.FAILED);
        Long[] previousTimestamps = new Long[ExecutionState.values().length];
        when(stateMap.get(STATE_KEY)).thenAnswer(invocation -> persistedState.get());
        when(timestampMap.get(STATE_KEY)).thenReturn(previousTimestamps);
        doAnswer(
                        invocation -> {
                            persistedState.set(ExecutionState.CREATED);
                            throw new IndeterminateOperationStateException("reply lost");
                        })
                .when(stateMap)
                .replace(STATE_KEY, ExecutionState.FAILED, ExecutionState.CREATED);

        DistributedStateTransition.Result<ExecutionState> result =
                reset(stateMap, timestampMap, true);

        Assertions.assertFalse(result.isTransitioned());
        Assertions.assertEquals(ExecutionState.CREATED, result.getCurrentState());
        verify(timestampMap).set(eq(STATE_KEY), any(Long[].class));
        verify(timestampMap, never()).remove(STATE_KEY);
    }

    private DistributedStateTransition.Result<ExecutionState> transition(
            IMap<Object, Object> stateMap,
            IMap<Object, Long[]> timestampMap,
            boolean allowMissingStateRecreation) {
        return DistributedStateTransition.transition(
                stateMap,
                STATE_KEY,
                ExecutionState.RUNNING,
                ExecutionState.CREATED,
                ExecutionState.FAILED,
                ExecutionState.class,
                ExecutionState::isEndState,
                () -> allowMissingStateRecreation,
                timestampMap,
                ExecutionState.values().length,
                ExecutionState.FAILED.ordinal());
    }

    private DistributedStateTransition.Result<ExecutionState> transition(
            IMap<Object, Object> stateMap,
            IMap<Object, Long[]> timestampMap,
            boolean allowMissingStateRecreation,
            Runnable lockMissingStatePersistence,
            Runnable unlockMissingStatePersistence) {
        return DistributedStateTransition.transition(
                stateMap,
                STATE_KEY,
                ExecutionState.RUNNING,
                ExecutionState.CREATED,
                ExecutionState.FAILED,
                ExecutionState.class,
                ExecutionState::isEndState,
                () -> allowMissingStateRecreation,
                lockMissingStatePersistence,
                unlockMissingStatePersistence,
                timestampMap,
                ExecutionState.values().length,
                ExecutionState.FAILED.ordinal());
    }

    private ExecutionState initialize(
            IMap<Object, Object> stateMap, IMap<Object, Long[]> timestampMap) {
        return DistributedStateTransition.initialize(
                stateMap,
                STATE_KEY,
                ExecutionState.CREATED,
                ExecutionState.class,
                timestampMap,
                ExecutionState.values().length,
                ExecutionState.INITIALIZING.ordinal(),
                10L,
                ExecutionState.CREATED.ordinal());
    }

    private DistributedStateTransition.Result<ExecutionState> reset(
            IMap<Object, Object> stateMap,
            IMap<Object, Long[]> timestampMap,
            boolean allowStatePersistence) {
        return DistributedStateTransition.reset(
                stateMap,
                STATE_KEY,
                ExecutionState.CREATED,
                ExecutionState.class,
                ExecutionState::isEndState,
                () -> allowStatePersistence,
                timestampMap,
                ExecutionState.values().length,
                ExecutionState.CREATED.ordinal());
    }
}
