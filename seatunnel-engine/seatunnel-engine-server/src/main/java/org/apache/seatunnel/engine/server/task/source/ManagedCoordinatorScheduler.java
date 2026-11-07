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

import org.apache.seatunnel.api.source.scheduler.AsyncFailurePolicy;
import org.apache.seatunnel.api.source.scheduler.AsyncTaskKey;
import org.apache.seatunnel.api.source.scheduler.AsyncTaskOptions;
import org.apache.seatunnel.api.source.scheduler.Cancellable;
import org.apache.seatunnel.api.source.scheduler.CoordinatorScheduler;
import org.apache.seatunnel.engine.server.TaskExecutionService;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Epoch-fenced implementation of the public Source coordinator scheduler.
 *
 * <p>Worker and timer threads only enqueue callbacks. Connector result handlers always execute from
 * the coordinator event loop that calls {@link #drainOneCallback()}.
 */
public final class ManagedCoordinatorScheduler implements CoordinatorScheduler, AutoCloseable {
    private final TaskExecutionService taskExecutionService;
    private final String coordinatorEpoch;
    private final ClassLoader connectorClassLoader;
    private final int maxConcurrency;
    private final int maxCallbacks;
    private final int normalMaxCallbacks;
    private final Consumer<Throwable> failureHandler;
    private final Consumer<Boolean> wakeup;
    private final Map<AsyncTaskKey, KeyState> keyStates = new HashMap<>();
    private final Queue<Callback> callbacks = new ArrayDeque<>();
    private final Queue<Submission<?>> waitingSubmissions = new ArrayDeque<>();
    private final Map<AsyncTaskKey, TimerHandle> timers = new ConcurrentHashMap<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicLong timeoutCount = new AtomicLong();
    private final AtomicLong coalescedCount = new AtomicLong();
    private final AtomicLong skippedCount = new AtomicLong();
    private final AtomicLong staleCallbackCount = new AtomicLong();
    private final AtomicLong queueNanos = new AtomicLong();
    private final AtomicLong executionNanos = new AtomicLong();
    private final AtomicLong cancellationSignals = new AtomicLong();
    private int runningCount;
    private int normalCallbacks;
    private Long ownerThreadId;

    public ManagedCoordinatorScheduler(
            TaskExecutionService taskExecutionService,
            String coordinatorEpoch,
            ClassLoader connectorClassLoader,
            int maxConcurrency,
            int maxCallbacks,
            int reservedCallbacks,
            Consumer<Throwable> failureHandler,
            Consumer<Boolean> wakeup) {
        this.taskExecutionService = taskExecutionService;
        this.coordinatorEpoch = coordinatorEpoch;
        this.connectorClassLoader = connectorClassLoader;
        this.maxConcurrency = maxConcurrency;
        this.maxCallbacks = maxCallbacks;
        this.normalMaxCallbacks = maxCallbacks - reservedCallbacks;
        if (maxConcurrency <= 0
                || maxCallbacks <= 0
                || reservedCallbacks < 0
                || reservedCallbacks >= maxCallbacks
                || reservedCallbacks < maxConcurrency) {
            throw new IllegalArgumentException("Managed coordinator scheduler limits are invalid");
        }
        this.failureHandler = failureHandler;
        this.wakeup = wakeup;
    }

    /**
     * Runs blocking connector work off the coordinator event loop and applies its result back on
     * that loop.
     *
     * <p><b>Thread contract, and the reason this method exists:</b> {@code callable} executes on a
     * shared engine worker thread, so it must be free of side effects on enumerator state. It may
     * only read immutable configuration and call thread-safe clients. Anything it discovers must be
     * returned as a value. {@code resultHandler} then runs on the coordinator event loop, which is
     * the only thread permitted to mutate enumerator state, the assignment ledger, or anything else
     * that a checkpoint can observe. Writing connector fields directly from {@code callable}
     * defeats the single-owner guarantee the managed lane exists to provide.
     *
     * <p>Submissions are keyed. A second submission for a key that is already running or queued is
     * resolved by {@link AsyncTaskOptions#getOverlapPolicy()}, and results whose coordinator epoch
     * is stale are discarded rather than applied.
     *
     * <p>Must be called from the coordinator event loop.
     *
     * @param key identity used for overlap, coalescing and cancellation
     * @param callable blocking work; runs on an engine worker thread, must not touch coordinator
     *     state
     * @param resultHandler applied on the coordinator event loop with the result or the failure
     * @param options worker class, timeout, overlap and failure policy
     * @return handle that cancels the submission and its pending callback
     */
    @Override
    public <T> Cancellable callAsync(
            AsyncTaskKey key,
            Callable<T> callable,
            BiConsumer<T, Throwable> resultHandler,
            AsyncTaskOptions options) {
        checkOwnerThread();
        ensureActive();
        Submission<T> submission =
                new Submission<>(key, callable, resultHandler, options, this::enqueueCancellation);
        if (!keyStates.containsKey(key) && keyStates.size() >= maxCallbacks) {
            throw new IllegalStateException(
                    "Managed coordinator async submission capacity exhausted");
        }
        KeyState state = keyStates.computeIfAbsent(key, ignored -> new KeyState());
        if (state.running || state.queued) {
            switch (options.getOverlapPolicy()) {
                case COALESCE_ONE:
                    coalescedCount.incrementAndGet();
                    if (state.coalesced != null) {
                        state.coalesced.cancelWithoutReconciliation();
                    }
                    state.coalesced = submission;
                    return submission;
                case SKIP:
                    skippedCount.incrementAndGet();
                    submission.cancelWithoutReconciliation();
                    return submission;
                case FAIL:
                    throw new IllegalStateException(
                            "Managed coordinator async task overlaps for key " + key);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported async overlap policy " + options.getOverlapPolicy());
            }
        }
        state.queued = true;
        if (runningCount < maxConcurrency) {
            startSubmission(submission, state);
        } else {
            waitingSubmissions.add(submission);
        }
        return submission;
    }

    /**
     * Schedules a delayed callback that runs on the coordinator event loop.
     *
     * <p>Unlike {@link #callAsync}, {@code task} is allowed to touch enumerator state because it
     * executes on the owner thread. The engine timer only enqueues it; it never runs connector code
     * itself. Use this for retry and interval ticks, and {@link #callAsync} for anything that
     * blocks.
     *
     * <p>Must be called from the coordinator event loop.
     *
     * @param key identity used for cancellation and overlap accounting
     * @param delay delay before the callback is enqueued
     * @param task callback executed on the coordinator event loop
     * @return handle that cancels the pending callback
     */
    @Override
    public Cancellable scheduleInCoordinatorThread(
            AsyncTaskKey key, Duration delay, Runnable task) {
        checkOwnerThread();
        ensureActive();
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("Managed coordinator delay must not be negative");
        }
        if (key == null || task == null) {
            throw new IllegalArgumentException(
                    "Managed coordinator timer arguments must not be null");
        }
        if (!timers.containsKey(key) && timers.size() >= normalMaxCallbacks) {
            throw new IllegalStateException("Managed coordinator timer capacity exhausted");
        }
        TimerHandle handle = new TimerHandle(key, timer -> timers.remove(timer.key, timer));
        ScheduledFuture<?> future =
                taskExecutionService.scheduleManagedSourceCoordinatorTimer(
                        () ->
                                enqueueCallback(
                                        new Callback(
                                                coordinatorEpoch,
                                                () -> {
                                                    if (timers.remove(key, handle)) {
                                                        task.run();
                                                    }
                                                },
                                                handle,
                                                false,
                                                false)),
                        delay.toMillis());
        handle.setFuture(future);
        TimerHandle previous = timers.put(key, handle);
        if (previous != null) {
            previous.cancel();
        }
        return handle;
    }

    /** Executes at most one epoch-valid result or timer callback on the event-loop owner. */
    public boolean drainOneCallback() {
        checkOwnerThread();
        if (consumeCancellationSignal()) {
            Submission<?> cancelled = findCancelledSubmission();
            if (cancelled == null) {
                staleCallbackCount.incrementAndGet();
            } else {
                try {
                    completeCancelledSubmission(cancelled);
                } catch (Throwable t) {
                    failureHandler.accept(t);
                }
            }
            return true;
        }
        Callback callback;
        synchronized (callbacks) {
            callback = callbacks.poll();
            if (callback != null && !callback.reserved) {
                normalCallbacks--;
            }
        }
        if (callback == null) {
            return false;
        }
        if (!active.get()
                || !coordinatorEpoch.equals(callback.epoch)
                || (!callback.runWhenCancelled && callback.handle.isCancelled())) {
            staleCallbackCount.incrementAndGet();
            return true;
        }
        try {
            callback.runnable.run();
        } catch (Throwable t) {
            failureHandler.accept(t);
        }
        return true;
    }

    @Override
    public void close() {
        if (!active.compareAndSet(true, false)) {
            return;
        }
        for (KeyState state : keyStates.values()) {
            if (state.runningSubmission != null) {
                state.runningSubmission.cancel();
            }
            if (state.coalesced != null) {
                state.coalesced.cancel();
            }
        }
        for (Submission<?> submission : waitingSubmissions) {
            submission.cancel();
        }
        waitingSubmissions.clear();
        timers.values().forEach(TimerHandle::cancel);
        timers.clear();
        synchronized (callbacks) {
            callbacks.clear();
            normalCallbacks = 0;
        }
        cancellationSignals.set(0L);
        keyStates.clear();
        runningCount = 0;
    }

    public boolean hasPendingCallbacks() {
        synchronized (callbacks) {
            return cancellationSignals.get() > 0L || !callbacks.isEmpty();
        }
    }

    int runningCount() {
        checkOwnerThread();
        return runningCount;
    }

    int waitingCount() {
        checkOwnerThread();
        return waitingSubmissions.size();
    }

    int callbackCount() {
        synchronized (callbacks) {
            return Math.toIntExact(
                    Math.min(Integer.MAX_VALUE, callbacks.size() + cancellationSignals.get()));
        }
    }

    long timeoutCount() {
        return timeoutCount.get();
    }

    long coalescedCount() {
        return coalescedCount.get();
    }

    long skippedCount() {
        return skippedCount.get();
    }

    long staleCallbackCount() {
        return staleCallbackCount.get();
    }

    long queueNanos() {
        return queueNanos.get();
    }

    long executionNanos() {
        return executionNanos.get();
    }

    private <T> void startSubmission(Submission<T> submission, KeyState state) {
        if (submission.isCancelled() || !active.get()) {
            state.queued = false;
            scheduleNext();
            return;
        }
        state.queued = false;
        state.running = true;
        state.runningSubmission = submission;
        runningCount++;
        queueNanos.addAndGet(Math.max(0L, System.nanoTime() - submission.submittedNanos));
        try {
            submission.workerFuture =
                    taskExecutionService.submitManagedSourceAsync(
                            submission.options.getWorkerClass(),
                            () -> {
                                ClassLoader previous =
                                        Thread.currentThread().getContextClassLoader();
                                Thread.currentThread().setContextClassLoader(connectorClassLoader);
                                long startedNanos = System.nanoTime();
                                try {
                                    T result = submission.callable.call();
                                    enqueueCompletion(submission, result, null);
                                } catch (Throwable t) {
                                    enqueueCompletion(submission, null, t);
                                } finally {
                                    executionNanos.addAndGet(
                                            Math.max(0L, System.nanoTime() - startedNanos));
                                    Thread.currentThread().setContextClassLoader(previous);
                                }
                                return null;
                            });
            submission.timeoutFuture =
                    taskExecutionService.scheduleManagedSourceCoordinatorTimer(
                            () -> {
                                if (!submission.isCancelled()
                                        && submission.markTimedOut()
                                        && enqueueCompletion(
                                                submission,
                                                null,
                                                new java.util.concurrent.TimeoutException(
                                                        "Managed Source async task timed out: "
                                                                + submission.key))) {
                                    timeoutCount.incrementAndGet();
                                    Future<?> worker = submission.workerFuture;
                                    if (worker != null) {
                                        worker.cancel(true);
                                    }
                                }
                            },
                            submission.options.getTimeout().toMillis());
        } catch (Throwable submissionFailure) {
            Future<?> worker = submission.workerFuture;
            if (worker != null) {
                worker.cancel(true);
            }
            completeSubmission(submission, null, submissionFailure);
        }
    }

    private <T> boolean enqueueCompletion(Submission<T> submission, T result, Throwable failure) {
        if (!submission.markCompletionEnqueued()) {
            return false;
        }
        enqueueCallback(
                new Callback(
                        coordinatorEpoch,
                        () -> completeSubmission(submission, result, failure),
                        submission,
                        true,
                        true));
        return true;
    }

    /**
     * Enqueues cancellation reconciliation exactly once so queued and running slots are released by
     * the coordinator owner.
     */
    private void enqueueCancellation(Submission<?> submission) {
        if (!active.get() || !submission.markCompletionEnqueued()) {
            return;
        }
        cancellationSignals.incrementAndGet();
        wakeup.accept(true);
    }

    private boolean consumeCancellationSignal() {
        while (true) {
            long current = cancellationSignals.get();
            if (current == 0L) {
                return false;
            }
            if (cancellationSignals.compareAndSet(current, current - 1L)) {
                return true;
            }
        }
    }

    private Submission<?> findCancelledSubmission() {
        for (KeyState state : keyStates.values()) {
            if (state.runningSubmission != null && state.runningSubmission.isCancelled()) {
                return state.runningSubmission;
            }
        }
        for (Submission<?> submission : waitingSubmissions) {
            if (submission.isCancelled()) {
                return submission;
            }
        }
        for (KeyState state : keyStates.values()) {
            if (state.coalesced != null && state.coalesced.isCancelled()) {
                return state.coalesced;
            }
        }
        return null;
    }

    /** Reconciles a cancelled submission in every possible queue or running state. */
    private void completeCancelledSubmission(Submission<?> submission) {
        KeyState state = keyStates.get(submission.key);
        if (state == null) {
            return;
        }
        if (state.runningSubmission == submission) {
            completeUnchecked(submission, null, null);
            return;
        }
        waitingSubmissions.remove(submission);
        if (state.coalesced == submission) {
            state.coalesced = null;
        }
        if (!state.running) {
            state.queued = false;
        }
        cleanupKeyState(submission.key, state);
        scheduleNext();
    }

    private <T> void completeSubmission(Submission<T> submission, T result, Throwable failure) {
        KeyState state = keyStates.get(submission.key);
        if (state == null || state.runningSubmission != submission) {
            return;
        }
        if (submission.timeoutFuture != null) {
            submission.timeoutFuture.cancel(false);
        }
        state.running = false;
        state.runningSubmission = null;
        runningCount--;
        try {
            if (!submission.isCancelled()) {
                submission.resultHandler.accept(result, failure);
                if (failure != null
                        && submission.options.getFailurePolicy()
                                == AsyncFailurePolicy.FAIL_SOURCE) {
                    failureHandler.accept(failure);
                }
            }
        } catch (Throwable handlerFailure) {
            failureHandler.accept(handlerFailure);
        } finally {
            Submission<?> coalesced = state.coalesced;
            state.coalesced = null;
            if (coalesced != null && !coalesced.isCancelled()) {
                state.queued = true;
                waitingSubmissions.add(coalesced);
            }
            scheduleNext();
            cleanupKeyState(submission.key, state);
        }
    }

    private void scheduleNext() {
        while (runningCount < maxConcurrency && !waitingSubmissions.isEmpty()) {
            Submission<?> next = waitingSubmissions.poll();
            KeyState state = keyStates.get(next.key);
            if (state == null) {
                continue;
            }
            if (next.isCancelled()) {
                if (!state.running) {
                    state.queued = false;
                }
                cleanupKeyState(next.key, state);
                continue;
            }
            startUnchecked(next, state);
        }
    }

    private void cleanupKeyState(AsyncTaskKey key, KeyState state) {
        if (!state.running && !state.queued && state.coalesced == null) {
            keyStates.remove(key, state);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void startUnchecked(Submission<?> submission, KeyState state) {
        startSubmission((Submission) submission, state);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void completeUnchecked(Submission<?> submission, Object result, Throwable failure) {
        completeSubmission((Submission) submission, result, failure);
    }

    private void enqueueCallback(Callback callback) {
        if (!active.get()) {
            callback.handle.cancel();
            return;
        }
        synchronized (callbacks) {
            if (callbacks.size() >= maxCallbacks
                    || (!callback.reserved && normalCallbacks >= normalMaxCallbacks)) {
                callback.handle.cancel();
                failureHandler.accept(
                        new IllegalStateException(
                                "Managed coordinator scheduler callback mailbox exhausted"));
                return;
            }
            callbacks.add(callback);
            if (!callback.reserved) {
                normalCallbacks++;
            }
        }
        wakeup.accept(callback.reserved);
    }

    private void ensureActive() {
        if (!active.get()) {
            throw new IllegalStateException("Managed coordinator scheduler is closed");
        }
    }

    private void checkOwnerThread() {
        long current = Thread.currentThread().getId();
        if (ownerThreadId == null) {
            ownerThreadId = current;
        } else if (ownerThreadId != current) {
            throw new IllegalStateException(
                    "CoordinatorScheduler API called outside the coordinator event loop");
        }
    }

    private static final class KeyState {
        private boolean running;
        private boolean queued;
        private Submission<?> runningSubmission;
        private Submission<?> coalesced;
    }

    private static final class Callback {
        private final String epoch;
        private final Runnable runnable;
        private final Cancellable handle;
        private final boolean runWhenCancelled;
        private final boolean reserved;

        private Callback(
                String epoch,
                Runnable runnable,
                Cancellable handle,
                boolean runWhenCancelled,
                boolean reserved) {
            this.epoch = epoch;
            this.runnable = runnable;
            this.handle = handle;
            this.runWhenCancelled = runWhenCancelled;
            this.reserved = reserved;
        }
    }

    private static final class Submission<T> implements Cancellable {
        private final AsyncTaskKey key;
        private final Callable<T> callable;
        private final BiConsumer<T, Throwable> resultHandler;
        private final AsyncTaskOptions options;
        private final Consumer<Submission<?>> cancellationHandler;
        private final long submittedNanos = System.nanoTime();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean completionEnqueued = new AtomicBoolean();
        private final AtomicBoolean timedOut = new AtomicBoolean();
        private volatile Future<?> workerFuture;
        private volatile ScheduledFuture<?> timeoutFuture;

        private Submission(
                AsyncTaskKey key,
                Callable<T> callable,
                BiConsumer<T, Throwable> resultHandler,
                AsyncTaskOptions options,
                Consumer<Submission<?>> cancellationHandler) {
            if (key == null
                    || callable == null
                    || resultHandler == null
                    || options == null
                    || cancellationHandler == null) {
                throw new IllegalArgumentException("Managed async task arguments must not be null");
            }
            this.key = key;
            this.callable = callable;
            this.resultHandler = resultHandler;
            this.options = options;
            this.cancellationHandler = cancellationHandler;
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                Future<?> worker = workerFuture;
                if (worker != null) {
                    worker.cancel(true);
                }
                ScheduledFuture<?> timeout = timeoutFuture;
                if (timeout != null) {
                    timeout.cancel(false);
                }
                cancellationHandler.accept(this);
            }
        }

        private void cancelWithoutReconciliation() {
            if (cancelled.compareAndSet(false, true)) {
                Future<?> worker = workerFuture;
                if (worker != null) {
                    worker.cancel(true);
                }
                ScheduledFuture<?> timeout = timeoutFuture;
                if (timeout != null) {
                    timeout.cancel(false);
                }
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        private boolean markCompletionEnqueued() {
            return completionEnqueued.compareAndSet(false, true);
        }

        private boolean markTimedOut() {
            return timedOut.compareAndSet(false, true);
        }
    }

    private static final class TimerHandle implements Cancellable {
        private final AsyncTaskKey key;
        private final Consumer<TimerHandle> cancellationHandler;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile ScheduledFuture<?> future;

        private TimerHandle(AsyncTaskKey key, Consumer<TimerHandle> cancellationHandler) {
            this.key = key;
            this.cancellationHandler = cancellationHandler;
        }

        private void setFuture(ScheduledFuture<?> future) {
            this.future = future;
            if (cancelled.get()) {
                future.cancel(false);
            }
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                ScheduledFuture<?> scheduled = future;
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                cancellationHandler.accept(this);
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public String toString() {
            return "TimerHandle{" + key + '}';
        }
    }
}
