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

package org.apache.seatunnel.engine.e2e.partialfailure;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only synchronization point that keeps the failing pipeline's sink writer parked inside
 * {@code write()} until the test has observed everything it needs before the failure arrives.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs inside the test JVM, so a static registry
 * keyed by a per-test hold key is enough to coordinate the test thread with the task thread that
 * owns the writer. The test arms the key before submitting the job, and the failing writer of the
 * job parks on the first row it receives; the test releases the key only after it observed both
 * pipelines {@code RUNNING} and the healthy sibling's sink really writing rows. The injection point
 * is therefore an observed condition instead of a sleep the test hopes to win the race against.
 *
 * <p>An interrupt delivered while parked (for example from {@code TaskExecutionService#shutdown()}
 * when a worker is torn down) does not end the hold, so the test's assertion path still sees the
 * injected failure at the moment it chose; the interrupt flag is re-asserted on exit. A hard
 * deadline guarantees that a failing test can never leak a thread that blocks forever.
 */
@Slf4j
public final class PartialFailureGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Armed gates keyed by hold key; a missing entry means "do not park". */
    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    /** Number of writers parked on the gate per hold key. */
    private static final Map<String, AtomicInteger> PARKED_WRITERS = new ConcurrentHashMap<>();

    private PartialFailureGate() {}

    /**
     * Arms the gate: every failing writer that uses this key parks until {@link #release(String)}.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new CountDownLatch(1));
        PARKED_WRITERS.put(holdKey, new AtomicInteger());
    }

    /**
     * Releases every writer currently parked for the key and lets later writers pass through
     * immediately, i.e. the injected failure fires as soon as the writer continues. Safe to call
     * repeatedly and for keys that were never armed.
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
     * Releases and forgets the key. Intended for test teardown so that a writer can never stay
     * parked after the test gave up.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
        PARKED_WRITERS.remove(holdKey);
    }

    /**
     * Returns how many writers are parked on the gate for the key right now. A non-zero value is
     * the signal that the failing pipeline actually reached its injection point.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of parked writers, zero when the key is unknown
     */
    public static int parkedWriters(String holdKey) {
        AtomicInteger counter = PARKED_WRITERS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Parks the calling writer until the key is released. Returns immediately when the key was
     * never armed or was already released, so a restored pipeline fails fast instead of holding
     * again.
     *
     * <p>Interrupts are swallowed while parked and re-asserted on exit: a worker that is shut down
     * interrupts its task threads, but the task must keep running until the test explicitly
     * releases it, otherwise the scenario would collapse into a different failure than the injected
     * one.
     *
     * @param holdKey the key configured on the sink
     */
    static void awaitRelease(String holdKey) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch == null) {
            return;
        }
        AtomicInteger counter = PARKED_WRITERS.computeIfAbsent(holdKey, key -> new AtomicInteger());
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
                    // Keep parking: a node teardown interrupts task threads, but the injected
                    // failure must fire at the moment the test chose.
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
