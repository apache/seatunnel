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

package org.apache.seatunnel.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the checkpoint scheduling machinery under test and the background trigger load it runs
 * under.
 *
 * <p>This state models the scheduling layer only. It deliberately does not create a {@code
 * CheckpointCoordinator}: the quantity being measured is the delay between a checkpoint trigger
 * becoming due and the scheduling thread actually running it, which is a property of the thread
 * pools rather than of the barrier round-trip that follows.
 */
final class CheckpointSchedulingBenchmarkState {

    /** Matches the pool width {@code CheckpointCoordinator} creates for every pipeline. */
    private static final int THREADS_PER_PIPELINE = 2;

    /** Prefix of the thread names {@code CheckpointCoordinator} gives its scheduler threads. */
    static final String SCHEDULER_THREAD_NAME_PREFIX = "checkpoint-coordinator-";

    private static final long JOB_ID = 1L;

    private static final long TRIGGER_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private static final long SHUTDOWN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /** Lowest interval {@code CheckpointConfig} accepts. */
    private static final long MIN_CHECKPOINT_INTERVAL_MILLIS = 10L;

    private final int pipelineNum;
    private final long checkpointIntervalMillis;
    private final long triggerBodyMicros;

    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();
    private final AtomicLong backgroundTriggers = new AtomicLong();
    private final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();

    private long measuredTriggers;

    CheckpointSchedulingBenchmarkState(
            int pipelineNum, long checkpointIntervalMillis, long triggerBodyMicros) {
        this.pipelineNum = pipelineNum;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        this.triggerBodyMicros = triggerBodyMicros;
    }

    /**
     * Creates one scheduler per pipeline and arms its periodic trigger.
     *
     * <p>Pipeline start times are staggered across one interval so the load is a steady stream of
     * triggers rather than a single burst per interval.
     */
    void setUp() {
        validateParameters();
        for (int pipelineId = 0; pipelineId < pipelineNum; pipelineId++) {
            schedulers.add(createPipelineScheduler(pipelineId));
        }
        for (int pipelineId = 0; pipelineId < pipelineNum; pipelineId++) {
            long initialDelay = (checkpointIntervalMillis * pipelineId) / pipelineNum;
            schedulers
                    .get(pipelineId)
                    .scheduleWithFixedDelay(
                            this::runTriggerBody,
                            initialDelay,
                            checkpointIntervalMillis,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Schedules one trigger with no delay and waits for the scheduling thread to run it.
     *
     * <p>The measured task only signals; the per-trigger work lives in the background load. What
     * the sample therefore contains is queueing plus wake-up, which is the part the scheduling
     * model decides.
     *
     * @return the number of measured triggers run so far
     */
    long scheduleAndAwaitTrigger() throws InterruptedException {
        checkBackgroundFailure();
        CountDownLatch fired = new CountDownLatch(1);
        schedulers.get(0).schedule(fired::countDown, 0L, TimeUnit.MILLISECONDS);
        if (!fired.await(TRIGGER_TIMEOUT_NANOS, TimeUnit.NANOSECONDS)) {
            throw new IllegalStateException(
                    "Scheduled checkpoint trigger did not run within "
                            + TimeUnit.NANOSECONDS.toSeconds(TRIGGER_TIMEOUT_NANOS)
                            + "s for "
                            + pipelineNum
                            + " pipelines");
        }
        return ++measuredTriggers;
    }

    void tearDown() throws InterruptedException {
        for (ScheduledExecutorService scheduler : schedulers) {
            scheduler.shutdownNow();
        }
        for (ScheduledExecutorService scheduler : schedulers) {
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Checkpoint scheduler did not stop");
            }
        }
        schedulers.clear();
        checkBackgroundFailure();
    }

    long getMeasuredTriggers() {
        return measuredTriggers;
    }

    long getBackgroundTriggers() {
        return backgroundTriggers.get();
    }

    /**
     * Counts the live scheduler threads by name.
     *
     * <p>This is the cost the shared scheduler exists to remove, so it is reported rather than
     * derived: the per-pipeline model grows it with the pipeline count, a shared pool does not.
     */
    static long countSchedulerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(SCHEDULER_THREAD_NAME_PREFIX))
                .count();
    }

    private ScheduledExecutorService createPipelineScheduler(int pipelineId) {
        ScheduledThreadPoolExecutor scheduler =
                new ScheduledThreadPoolExecutor(
                        THREADS_PER_PIPELINE,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName(
                                    String.format(
                                            SCHEDULER_THREAD_NAME_PREFIX + "%s/%s",
                                            pipelineId,
                                            JOB_ID));
                            return thread;
                        });
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    /**
     * Occupies the scheduling thread for the configured time.
     *
     * <p>The wait is a sleep rather than a spin on purpose. A real trigger body waits on the
     * coordinator lock and on checkpoint id allocation; it holds its thread without consuming a
     * core, and that is the shape that decides whether a bounded pool is wide enough.
     */
    private void runTriggerBody() {
        try {
            if (triggerBodyMicros > 0) {
                TimeUnit.MICROSECONDS.sleep(triggerBodyMicros);
            }
            backgroundTriggers.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable throwable) {
            backgroundFailure.compareAndSet(null, throwable);
        }
    }

    private void validateParameters() {
        if (pipelineNum < 1) {
            throw new IllegalArgumentException("pipelineNum must be at least 1");
        }
        if (checkpointIntervalMillis < MIN_CHECKPOINT_INTERVAL_MILLIS) {
            throw new IllegalArgumentException(
                    "checkpointIntervalMillis must be at least "
                            + MIN_CHECKPOINT_INTERVAL_MILLIS
                            + " to match the minimum SeaTunnel accepts");
        }
        if (triggerBodyMicros < 0) {
            throw new IllegalArgumentException("triggerBodyMicros must not be negative");
        }
    }

    private void checkBackgroundFailure() {
        Throwable failure = backgroundFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Background checkpoint trigger failed", failure);
        }
    }
}
