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

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.map.IMap;

import java.util.Arrays;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * Atomically persists one distributed execution-state transition without overwriting a concurrent
 * winner.
 *
 * <p>The state map is the source of truth. Missing entries are recreated with {@link
 * IMap#putIfAbsent(Object, Object)}, while existing entries use compare-and-set replacement. The
 * state-key lock serializes each state/timestamp pair. The timestamp is persisted first so a
 * process exit cannot expose a new state without its metadata; a failed state write restores the
 * prior timestamp snapshot only when the state outcome can be confirmed. An ambiguous outcome
 * retains the extra timestamp because that is safer than exposing committed state without metadata.
 */
final class DistributedStateTransition {

    /**
     * Bounds recovery from unexpected compare-and-set failures while the state key is locked.
     *
     * <p>Reaching this limit indicates a state-map implementation that violated key-lock ordering.
     */
    private static final int MAX_COMPARE_AND_SET_ATTEMPTS = 16;

    /**
     * Marker returned when a failed state write cannot be confirmed by a follow-up read.
     *
     * <p>This distinguishes an unknown outcome from a confirmed missing state value.
     */
    private static final Object STATE_OUTCOME_UNKNOWN = new Object();

    private DistributedStateTransition() {}

    /**
     * Attempts to move one distributed state entry to {@code targetState}.
     *
     * <p>Every concurrent value is type-checked and terminal states always win. If a missing entry
     * is protected by pending cleanup, persistence is rejected. State and timestamp updates are
     * serialized by the state-key lock. A confirmed failed state write restores the prior
     * timestamps; an indeterminate or unreadable outcome retains the prepared timestamp so a
     * committed state can never lose its metadata.
     */
    static <S> Result<S> transition(
            IMap<Object, Object> stateMap,
            Object stateKey,
            S localFallback,
            S defaultFallback,
            S targetState,
            Class<S> stateType,
            Predicate<S> terminalState,
            BooleanSupplier allowStatePersistence,
            IMap<Object, Long[]> timestampMap,
            int stateCount,
            int targetStateOrdinal) {
        return transition(
                stateMap,
                stateKey,
                localFallback,
                defaultFallback,
                targetState,
                stateType,
                terminalState,
                allowStatePersistence,
                () -> {},
                () -> {},
                timestampMap,
                stateCount,
                targetStateOrdinal);
    }

    /**
     * Attempts a transition while selectively fencing only missing-key recreation.
     *
     * <p>An existing key remains independently synchronized by its state-key lock. When the first
     * locked read finds no entry, that lock is released before taking the job cleanup fence and
     * reacquiring the state-key lock. This preserves the {@code cleanup -> state} lock order
     * without serializing unrelated task and pipeline transitions.
     */
    static <S> Result<S> transition(
            IMap<Object, Object> stateMap,
            Object stateKey,
            S localFallback,
            S defaultFallback,
            S targetState,
            Class<S> stateType,
            Predicate<S> terminalState,
            BooleanSupplier allowStatePersistence,
            Runnable lockMissingStatePersistence,
            Runnable unlockMissingStatePersistence,
            IMap<Object, Long[]> timestampMap,
            int stateCount,
            int targetStateOrdinal) {
        boolean stateLocked = false;
        boolean missingStateFenceLocked = false;
        try {
            stateMap.lock(stateKey);
            stateLocked = true;
            Object distributedValue = stateMap.get(stateKey);
            if (distributedValue == null) {
                stateMap.unlock(stateKey);
                stateLocked = false;
                lockMissingStatePersistence.run();
                missingStateFenceLocked = true;
                stateMap.lock(stateKey);
                stateLocked = true;
                distributedValue = stateMap.get(stateKey);
            }
            boolean stateEntryMissing = distributedValue == null;
            S previousState =
                    stateEntryMissing
                            ? localFallback != null ? localFallback : defaultFallback
                            : castState(stateKey, distributedValue, stateType);
            S currentState = previousState;

            for (int attempt = 0; attempt < MAX_COMPARE_AND_SET_ATTEMPTS; attempt++) {
                if (distributedValue != null) {
                    currentState = castState(stateKey, distributedValue, stateType);
                }
                if (!allowStatePersistence.getAsBoolean()) {
                    return Result.persistenceBlocked(
                            previousState, currentState, stateEntryMissing);
                }
                if (currentState.equals(targetState)) {
                    repairMissingTargetTimestamp(
                            timestampMap, stateKey, stateCount, targetStateOrdinal);
                    return Result.notTransitioned(
                            previousState, currentState, stateEntryMissing, false);
                }
                if (terminalState.test(currentState)) {
                    return Result.notTransitioned(
                            previousState, currentState, stateEntryMissing, false);
                }

                Long[] previousTimestamps = timestampMap.get(stateKey);
                Long[] updatedTimestamps =
                        withTimestamp(
                                previousTimestamps,
                                stateCount,
                                targetStateOrdinal,
                                System.currentTimeMillis());
                timestampMap.set(stateKey, updatedTimestamps);
                boolean stateUpdated;
                try {
                    if (distributedValue == null) {
                        Object concurrentState = stateMap.putIfAbsent(stateKey, targetState);
                        stateUpdated = concurrentState == null;
                        distributedValue = concurrentState;
                    } else {
                        stateUpdated = stateMap.replace(stateKey, distributedValue, targetState);
                        if (!stateUpdated) {
                            distributedValue = stateMap.get(stateKey);
                        }
                    }
                } catch (RuntimeException | Error stateFailure) {
                    Object persistedState =
                            readStateAfterFailedWrite(stateMap, stateKey, stateFailure);
                    if (persistedState != STATE_OUTCOME_UNKNOWN
                            && persistedState != null
                            && stateType.isInstance(persistedState)
                            && stateType.cast(persistedState).equals(targetState)) {
                        return Result.notTransitioned(
                                previousState, targetState, stateEntryMissing, false);
                    }
                    if (persistedState != STATE_OUTCOME_UNKNOWN
                            && !isIndeterminateOperationState(stateFailure)) {
                        rollbackTimestamp(timestampMap, stateKey, previousTimestamps, stateFailure);
                    }
                    throw stateFailure;
                }
                if (!stateUpdated) {
                    if (distributedValue != null
                            && stateType.isInstance(distributedValue)
                            && stateType.cast(distributedValue).equals(targetState)) {
                        return Result.notTransitioned(
                                previousState, targetState, stateEntryMissing, false);
                    }
                    restoreTimestamp(timestampMap, stateKey, previousTimestamps);
                    continue;
                }
                return Result.transitioned(previousState, targetState, stateEntryMissing);
            }
            throw new IllegalStateException(
                    String.format(
                            "State entry %s could not transition to %s after %s compare-and-set attempts",
                            stateKey, targetState, MAX_COMPARE_AND_SET_ATTEMPTS));
        } finally {
            try {
                if (stateLocked) {
                    stateMap.unlock(stateKey);
                }
            } finally {
                if (missingStateFenceLocked) {
                    unlockMissingStatePersistence.run();
                }
            }
        }
    }

    /**
     * Initializes a missing state entry without overwriting existing timestamps or terminal state.
     *
     * <p>INITIALIZING and CREATED timestamps are persisted before a missing state is inserted. A
     * later constructor can therefore finish the insert after a process exit without exposing a
     * timestamp-less CREATED state. Existing states keep their timestamps, and an existing CREATED
     * state repairs missing CREATED metadata.
     */
    static <S> S initialize(
            IMap<Object, Object> stateMap,
            Object stateKey,
            S initialState,
            Class<S> stateType,
            IMap<Object, Long[]> timestampMap,
            int stateCount,
            int initializingStateOrdinal,
            long initializationTimestamp,
            int createdStateOrdinal) {
        stateMap.lock(stateKey);
        try {
            Object distributedValue = stateMap.get(stateKey);
            Long[] previousTimestamps = timestampMap.get(stateKey);
            Long[] initializationTimestamps =
                    withTimestampIfMissing(
                            previousTimestamps,
                            stateCount,
                            initializingStateOrdinal,
                            initializationTimestamp);
            if (distributedValue != null) {
                S currentState = castState(stateKey, distributedValue, stateType);
                Long[] recoveredTimestamps =
                        currentState.equals(initialState)
                                ? withTimestampIfMissing(
                                        initializationTimestamps,
                                        stateCount,
                                        createdStateOrdinal,
                                        System.currentTimeMillis())
                                : initializationTimestamps;
                if (!Arrays.equals(previousTimestamps, recoveredTimestamps)) {
                    timestampMap.set(stateKey, recoveredTimestamps);
                }
                return currentState;
            }

            Long[] preparedTimestamps =
                    withTimestampIfMissing(
                            initializationTimestamps,
                            stateCount,
                            createdStateOrdinal,
                            System.currentTimeMillis());
            boolean timestampChanged = !Arrays.equals(previousTimestamps, preparedTimestamps);
            if (timestampChanged) {
                timestampMap.set(stateKey, preparedTimestamps);
            }
            try {
                Object concurrentState = stateMap.putIfAbsent(stateKey, initialState);
                if (concurrentState == null) {
                    return initialState;
                }
                S currentState = castState(stateKey, concurrentState, stateType);
                Long[] winnerTimestamps =
                        currentState.equals(initialState)
                                ? preparedTimestamps
                                : initializationTimestamps;
                if (!Arrays.equals(preparedTimestamps, winnerTimestamps)) {
                    timestampMap.set(stateKey, winnerTimestamps);
                }
                return currentState;
            } catch (RuntimeException | Error stateFailure) {
                Object persistedState = readStateAfterFailedWrite(stateMap, stateKey, stateFailure);
                if (persistedState != STATE_OUTCOME_UNKNOWN
                        && persistedState != null
                        && stateType.isInstance(persistedState)
                        && stateType.cast(persistedState).equals(initialState)) {
                    return initialState;
                }
                if (timestampChanged
                        && persistedState != STATE_OUTCOME_UNKNOWN
                        && !isIndeterminateOperationState(stateFailure)) {
                    rollbackTimestamp(timestampMap, stateKey, previousTimestamps, stateFailure);
                }
                throw stateFailure;
            }
        } finally {
            stateMap.unlock(stateKey);
        }
    }

    /**
     * Resets one terminal distributed state to its initial value under the normal persistence
     * fence.
     *
     * <p>The target timestamp is persisted before the state so a process exit cannot expose the
     * reset state without its metadata. A confirmed failed state write restores the previous
     * timestamp array, while an indeterminate outcome retains the prepared timestamp and a blocked
     * generation leaves both maps unchanged.
     */
    static <S> Result<S> reset(
            IMap<Object, Object> stateMap,
            Object stateKey,
            S targetState,
            Class<S> stateType,
            Predicate<S> resettableState,
            BooleanSupplier allowStatePersistence,
            IMap<Object, Long[]> timestampMap,
            int stateCount,
            int targetStateOrdinal) {
        stateMap.lock(stateKey);
        try {
            Object distributedValue = stateMap.get(stateKey);
            if (distributedValue == null) {
                return Result.persistenceBlocked(targetState, targetState, true);
            }
            S currentState = castState(stateKey, distributedValue, stateType);
            if (!allowStatePersistence.getAsBoolean()) {
                return Result.persistenceBlocked(currentState, currentState, false);
            }
            if (currentState.equals(targetState)) {
                repairMissingTargetTimestamp(
                        timestampMap, stateKey, stateCount, targetStateOrdinal);
                return Result.notTransitioned(currentState, currentState, false, false);
            }
            if (!resettableState.test(currentState)) {
                throw new IllegalStateException(
                        String.format(
                                "State entry %s can only reset from a terminal state, current is %s",
                                stateKey, currentState));
            }

            Long[] previousTimestamps = timestampMap.get(stateKey);
            Long[] updatedTimestamps =
                    withTimestamp(
                            previousTimestamps,
                            stateCount,
                            targetStateOrdinal,
                            System.currentTimeMillis());
            timestampMap.set(stateKey, updatedTimestamps);
            boolean stateUpdated;
            try {
                stateUpdated = stateMap.replace(stateKey, distributedValue, targetState);
            } catch (RuntimeException | Error stateFailure) {
                Object persistedState = readStateAfterFailedWrite(stateMap, stateKey, stateFailure);
                if (persistedState != STATE_OUTCOME_UNKNOWN
                        && persistedState != null
                        && stateType.isInstance(persistedState)
                        && stateType.cast(persistedState).equals(targetState)) {
                    return Result.notTransitioned(currentState, targetState, false, false);
                }
                if (persistedState != STATE_OUTCOME_UNKNOWN
                        && !isIndeterminateOperationState(stateFailure)) {
                    rollbackTimestamp(timestampMap, stateKey, previousTimestamps, stateFailure);
                }
                throw stateFailure;
            }
            if (!stateUpdated) {
                Object persistedState = stateMap.get(stateKey);
                if (persistedState != null
                        && stateType.isInstance(persistedState)
                        && stateType.cast(persistedState).equals(targetState)) {
                    return Result.notTransitioned(currentState, targetState, false, false);
                }
                restoreTimestamp(timestampMap, stateKey, previousTimestamps);
                if (persistedState != null) {
                    castState(stateKey, persistedState, stateType);
                }
                throw new IllegalStateException(
                        String.format("State entry %s changed while its key was locked", stateKey));
            }
            return Result.transitioned(currentState, targetState, false);
        } finally {
            stateMap.unlock(stateKey);
        }
    }

    /**
     * Returns an independent timestamp array with the requested state timestamp replaced.
     *
     * <p>The existing array is never mutated. The result is expanded when older persisted arrays
     * predate newly added states.
     */
    private static Long[] withTimestamp(
            Long[] previousTimestamps, int stateCount, int stateOrdinal, long timestamp) {
        int targetLength =
                previousTimestamps == null
                        ? stateCount
                        : Math.max(stateCount, previousTimestamps.length);
        Long[] updatedTimestamps =
                previousTimestamps == null
                        ? new Long[targetLength]
                        : Arrays.copyOf(previousTimestamps, targetLength);
        updatedTimestamps[stateOrdinal] = timestamp;
        return updatedTimestamps;
    }

    /**
     * Returns the original timestamps when the requested state already has a value, otherwise a
     * copied array containing the supplied timestamp.
     */
    private static Long[] withTimestampIfMissing(
            Long[] previousTimestamps, int stateCount, int stateOrdinal, long timestamp) {
        if (previousTimestamps != null
                && previousTimestamps.length > stateOrdinal
                && previousTimestamps[stateOrdinal] != null) {
            return previousTimestamps;
        }
        return withTimestamp(previousTimestamps, stateCount, stateOrdinal, timestamp);
    }

    /**
     * Repairs metadata for a state that already equals the requested target.
     *
     * <p>The caller holds the state-key lock, so this repair cannot overwrite a concurrent
     * transition for the same state entry.
     */
    private static void repairMissingTargetTimestamp(
            IMap<Object, Long[]> timestampMap,
            Object stateKey,
            int stateCount,
            int targetStateOrdinal) {
        Long[] previousTimestamps = timestampMap.get(stateKey);
        Long[] repairedTimestamps =
                withTimestampIfMissing(
                        previousTimestamps,
                        stateCount,
                        targetStateOrdinal,
                        System.currentTimeMillis());
        if (!Arrays.equals(previousTimestamps, repairedTimestamps)) {
            timestampMap.set(stateKey, repairedTimestamps);
        }
    }

    /**
     * Restores or removes the timestamp entry after a failed timestamp-first transaction.
     *
     * <p>Rollback failures are suppressed onto the original persistence failure.
     */
    private static void rollbackTimestamp(
            IMap<Object, Long[]> timestampMap,
            Object stateKey,
            Long[] previousTimestamps,
            Throwable timestampFailure) {
        try {
            if (previousTimestamps == null) {
                timestampMap.remove(stateKey);
            } else {
                timestampMap.set(stateKey, previousTimestamps);
            }
        } catch (RuntimeException | Error rollbackFailure) {
            timestampFailure.addSuppressed(rollbackFailure);
        }
    }

    /**
     * Restores the timestamp snapshot after a compare-and-set loses without throwing.
     *
     * <p>Unlike rollback after an exception, any restoration failure is propagated directly to the
     * outer retry policy.
     */
    private static void restoreTimestamp(
            IMap<Object, Long[]> timestampMap, Object stateKey, Long[] previousTimestamps) {
        if (previousTimestamps == null) {
            timestampMap.remove(stateKey);
        } else {
            timestampMap.set(stateKey, previousTimestamps);
        }
    }

    /**
     * Reads the state after a failed write without hiding the original persistence exception.
     *
     * <p>If confirmation itself fails, the confirmation failure is suppressed and the original
     * exception remains authoritative. The timestamp is intentionally retained because the state
     * outcome cannot be proven.
     */
    private static Object readStateAfterFailedWrite(
            IMap<Object, Object> stateMap, Object stateKey, Throwable stateFailure) {
        try {
            return stateMap.get(stateKey);
        } catch (RuntimeException | Error confirmationFailure) {
            stateFailure.addSuppressed(confirmationFailure);
            return STATE_OUTCOME_UNKNOWN;
        }
    }

    /**
     * Returns whether Hazelcast reports that a state mutation may already have committed.
     *
     * <p>The exception can be wrapped by an invocation or retry layer, so the complete cause chain
     * is inspected before deciding whether timestamp rollback is safe.
     */
    private static boolean isIndeterminateOperationState(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof IndeterminateOperationStateException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Validates and casts a distributed state value.
     *
     * <p>Unexpected values fail fast with the state key and both actual and expected types.
     */
    private static <S> S castState(Object stateKey, Object state, Class<S> stateType) {
        if (!stateType.isInstance(state)) {
            throw new IllegalStateException(
                    String.format(
                            "State entry %s has unexpected type %s, expected %s",
                            stateKey, state.getClass().getName(), stateType.getName()));
        }
        return stateType.cast(state);
    }

    /**
     * Describes the distributed winner and whether this invocation changed the state entry.
     *
     * <p>{@code previousState} is the state observed before retrying, while {@code currentState} is
     * the final distributed winner. Missing-entry and cleanup flags let callers report recovery
     * without mistaking a blocked recreation for a successful transition.
     */
    static final class Result<S> {
        /**
         * State observed before the first compare-and-set attempt.
         *
         * <p>This remains stable while retries inspect later distributed winners.
         */
        private final S previousState;

        /**
         * State that won after all required compare-and-set attempts.
         *
         * <p>Callers synchronize their local state to this value after a lost race.
         */
        private final S currentState;

        /**
         * Whether this invocation persisted the target state.
         *
         * <p>A false value means another distributed state already won, the target was already
         * present, or pending cleanup blocked recreation. Timestamp repair for an already-present
         * target does not count as a state transition.
         */
        private final boolean transitioned;

        /**
         * Whether the distributed entry was absent at the start of the transition.
         *
         * <p>Callers use this flag to report state-map recovery explicitly.
         */
        private final boolean stateEntryMissing;

        /**
         * Whether the generation fence prevented state or timestamp persistence.
         *
         * <p>When true, callers must leave both local and distributed state untouched.
         */
        private final boolean persistenceBlocked;

        private Result(
                S previousState,
                S currentState,
                boolean transitioned,
                boolean stateEntryMissing,
                boolean persistenceBlocked) {
            this.previousState = previousState;
            this.currentState = currentState;
            this.transitioned = transitioned;
            this.stateEntryMissing = stateEntryMissing;
            this.persistenceBlocked = persistenceBlocked;
        }

        private static <S> Result<S> transitioned(
                S previousState, S currentState, boolean stateEntryMissing) {
            return new Result<>(previousState, currentState, true, stateEntryMissing, false);
        }

        private static <S> Result<S> notTransitioned(
                S previousState,
                S currentState,
                boolean stateEntryMissing,
                boolean persistenceBlocked) {
            return new Result<>(
                    previousState, currentState, false, stateEntryMissing, persistenceBlocked);
        }

        private static <S> Result<S> persistenceBlocked(
                S previousState, S currentState, boolean stateEntryMissing) {
            return new Result<>(previousState, currentState, false, stateEntryMissing, true);
        }

        S getPreviousState() {
            return previousState;
        }

        S getCurrentState() {
            return currentState;
        }

        boolean isTransitioned() {
            return transitioned;
        }

        boolean isStateEntryMissing() {
            return stateEntryMissing;
        }

        boolean isPersistenceBlocked() {
            return persistenceBlocked;
        }
    }
}
