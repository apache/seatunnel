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

package org.apache.seatunnel.engine.e2e.checkpoint;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only synchronization point that keeps {@link GatedBatchSource} running until the test
 * releases it.
 *
 * <p>A BATCH job over a finite source drains on its own, so a failure-injection test that needs the
 * job to be RUNNING at a later point races against the source reaching its end and the job
 * completing. This gate removes that race: the test arms a gate keyed by a per-test hold key before
 * it submits the job, the reader of {@link GatedBatchSource} keeps emitting one row per {@code
 * emit_interval_ms} and only signals "no more element" once the key is released, and the test
 * releases the key in its {@code finally} block after the observation window.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs entirely inside the test JVM, so a static
 * registry keyed by the hold key is enough to coordinate the test thread with the task thread that
 * owns the reader. A hold is bounded by {@link #MAX_HOLD_MILLIS} so a failing test can never block
 * a task thread forever, and a key that was never armed (or was already cleared) counts as released
 * so a task restored by a later test never hangs.
 */
@Slf4j
public final class BatchSourceGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Poll interval of {@link #awaitReached(String, long)}. */
    private static final long REACH_POLL_MILLIS = 50L;

    /** Armed gates keyed by hold key; a missing entry means "already released". */
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

    private BatchSourceGate() {}

    /**
     * Arms the gate: readers that use this key keep running until {@link #release(String)}.
     *
     * @param holdKey unique key shared between the test and the source configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new Gate());
    }

    /**
     * Releases every reader that currently waits for the key and lets later reads of the key pass
     * through immediately. Safe to call repeatedly and for keys that were never armed.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        Gate gate = GATES.get(holdKey);
        if (gate != null) {
            gate.released.countDown();
        }
    }

    /**
     * Releases and forgets the key. Intended for test teardown so that a rerun with the same key
     * starts from a clean registry.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
    }

    /**
     * Returns how many rows the held readers emitted for the key so far.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of emitted rows, zero when the key is unknown
     */
    public static long emittedRows(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? 0L : gate.emittedRows.get();
    }

    /**
     * Waits until a reader for the key has entered the hold and already emitted at least one row,
     * which proves the worker really runs {@link GatedBatchSource} before the test injects a fault.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @param timeoutMillis upper bound for the wait
     * @return {@code true} when the gate was reached, {@code false} on timeout
     */
    public static boolean awaitReached(String holdKey, long timeoutMillis) {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!hasReached(holdKey)) {
            if (System.nanoTime() >= deadlineNanos) {
                return false;
            }
            try {
                Thread.sleep(REACH_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return hasReached(holdKey);
            }
        }
        return true;
    }

    private static boolean hasReached(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate != null && gate.reached && gate.emittedRows.get() > 0L;
    }

    /**
     * Blocks the calling reader for at most {@code waitMillis}, or until the key is released.
     *
     * <p>This is called outside the checkpoint lock: a reader that waits while holding {@code
     * Collector#getCheckpointLock()} would block barrier injection for as long as the hold lasts.
     *
     * <p>An interrupt ends the hold early and is re-asserted for the caller. The engine interrupts
     * the task thread when it cancels the job, and the task can only finish cancelling once the
     * reader returns, so the hold must never swallow that interrupt.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @param waitMillis length of this wait slice, normally the configured emit interval
     * @return {@code true} when the hold is over and the reader must stop emitting
     */
    static boolean awaitRelease(String holdKey, long waitMillis) {
        Gate gate = GATES.get(holdKey);
        if (gate == null) {
            return true;
        }
        gate.reached = true;
        long remainingHoldNanos = gate.remainingHoldNanos();
        if (remainingHoldNanos <= 0L) {
            log.warn(
                    "Hold key {} exceeded its {} ms limit, releasing the gated source",
                    holdKey,
                    MAX_HOLD_MILLIS);
            return true;
        }
        try {
            long sliceNanos =
                    Math.min(
                            TimeUnit.MILLISECONDS.toNanos(Math.max(waitMillis, 1L)),
                            remainingHoldNanos);
            if (gate.released.await(sliceNanos, TimeUnit.NANOSECONDS)) {
                return true;
            }
            if (gate.remainingHoldNanos() <= 0L) {
                log.warn(
                        "Hold key {} exceeded its {} ms limit, releasing the gated source",
                        holdKey,
                        MAX_HOLD_MILLIS);
                return true;
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    /** Records one row emitted by a held reader, for {@link #awaitReached(String, long)}. */
    static void recordEmitted(String holdKey) {
        Gate gate = GATES.get(holdKey);
        if (gate != null) {
            gate.emittedRows.incrementAndGet();
        }
    }

    /** Per-test hold state: a release latch, the emitted row count and a hard hold deadline. */
    private static final class Gate {

        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicLong emittedRows = new AtomicLong();
        private final long holdDeadlineNanos =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);

        /** Set when a reader enters the hold, so a test can tell "reached" from "not started". */
        private volatile boolean reached;

        private long remainingHoldNanos() {
            return holdDeadlineNanos - System.nanoTime();
        }
    }
}
