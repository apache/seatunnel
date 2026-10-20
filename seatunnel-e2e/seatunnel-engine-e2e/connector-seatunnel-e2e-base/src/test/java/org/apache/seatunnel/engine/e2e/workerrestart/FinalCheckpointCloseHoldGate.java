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

package org.apache.seatunnel.engine.e2e.workerrestart;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only synchronization point that keeps a sink writer blocked inside {@code close()} until the
 * test releases it.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs entirely inside the test JVM, so a static
 * registry keyed by a per-test hold key is enough to coordinate the test thread with the task
 * thread that owns the writer. A sink task only closes its writer after it received the {@code
 * CheckpointFinishedOperation} of the final checkpoint, which the master sends only after it stored
 * that checkpoint as {@code latestCompletedCheckpoint}. Holding {@code close()} therefore freezes
 * the pipeline inside the window "final checkpoint completed on the master, but not every task has
 * reported FINISHED yet".
 *
 * <p>An interrupt delivered while holding (for example from {@code TaskExecutionService#shutdown()}
 * when the worker that owns the task is torn down) does not end the hold: from the master's point
 * of view the task must stay un-finished until the test decides otherwise, exactly like a task
 * thread that dies together with its worker process. A hard deadline guarantees that a failing test
 * can never leak a thread that blocks forever.
 */
@Slf4j
public final class FinalCheckpointCloseHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Armed gates keyed by hold key; a missing entry means "never hold". */
    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    /** Number of writers currently blocked inside close() per hold key. */
    private static final Map<String, AtomicInteger> HOLDING_WRITERS = new ConcurrentHashMap<>();

    private FinalCheckpointCloseHoldGate() {}

    /**
     * Arms the gate: every writer close that uses this key blocks until {@link #release(String)}.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new CountDownLatch(1));
        HOLDING_WRITERS.put(holdKey, new AtomicInteger());
    }

    /**
     * Releases every writer currently blocked for the key and lets later closes pass through
     * immediately. Safe to call repeatedly and for keys that were never armed.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch != null) {
            latch.countDown();
        }
    }

    /**
     * Releases and forgets the key. Intended for test teardown so that a rerun with the same key
     * can start from a clean registry.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
        HOLDING_WRITERS.remove(holdKey);
    }

    /**
     * Returns how many writers are blocked inside close() for the key right now.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of held writers, zero when the key is unknown
     */
    public static int holdingWriters(String holdKey) {
        AtomicInteger counter = HOLDING_WRITERS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Blocks the calling writer until the key is released. Returns immediately when the key was
     * never armed or was already released, so the writer of a restored pipeline never holds again.
     *
     * <p>Interrupts are swallowed while holding and re-asserted on exit: a worker that is shut down
     * interrupts its task threads, but the task must still look alive to the master until the test
     * explicitly releases it, otherwise the scenario would collapse into a normal completion.
     *
     * @param holdKey the key configured on the sink
     */
    static void awaitRelease(String holdKey) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch == null) {
            return;
        }
        AtomicInteger counter =
                HOLDING_WRITERS.computeIfAbsent(holdKey, key -> new AtomicInteger());
        counter.incrementAndGet();
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Hold key {} was not released within {} ms, giving up the hold",
                            holdKey,
                            MAX_HOLD_MILLIS);
                    return;
                }
                try {
                    if (latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                } catch (InterruptedException interruptedException) {
                    // Keep holding: the worker teardown interrupts task threads, but the task has
                    // to stay un-finished for the master until the test releases the gate.
                    interrupted = true;
                }
            }
        } finally {
            counter.decrementAndGet();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
