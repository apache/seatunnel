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

package org.apache.seatunnel.engine.e2e.savepointfailover;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only synchronization point that keeps a sink writer blocked inside {@code snapshotState}
 * until the test releases it.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs entirely inside the test JVM, so a static
 * registry keyed by a per-test hold key is enough to coordinate the test thread with the task
 * thread that owns the writer. A sink task calls {@code snapshotState} after it received a
 * checkpoint barrier and before it acknowledges that barrier to the master, so holding this method
 * freezes exactly the window "barrier reached the task, checkpoint not completed yet". That window
 * is what the test needs in order to tear the active master down while a stop-with-savepoint
 * request is still in flight, without relying on timing luck.
 *
 * <p>An interrupt delivered while holding (for example by a cancel/restore of the task group after
 * the master switch) does not end the hold: the task must stay un-acknowledged until the test
 * decides otherwise. A hard deadline guarantees that a broken test can never leak a task thread
 * that blocks forever.
 */
@Slf4j
public final class SavepointBarrierHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Armed gates keyed by hold key; a missing entry means "never hold". */
    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    /** Number of writers currently blocked inside snapshotState per hold key. */
    private static final Map<String, AtomicInteger> HOLDING_WRITERS = new ConcurrentHashMap<>();

    /** Checkpoint id of the barrier each hold key is currently holding. */
    private static final Map<String, AtomicLong> HELD_CHECKPOINT_IDS = new ConcurrentHashMap<>();

    /** Number of rows the writer bound to each hold key has received. */
    private static final Map<String, AtomicLong> WRITTEN_ROWS = new ConcurrentHashMap<>();

    private SavepointBarrierHoldGate() {}

    /**
     * Arms the gate: every writer snapshot of this key blocks until {@link #release(String)}.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new CountDownLatch(1));
        HOLDING_WRITERS.put(holdKey, new AtomicInteger());
        HELD_CHECKPOINT_IDS.put(holdKey, new AtomicLong(-1L));
        WRITTEN_ROWS.put(holdKey, new AtomicLong());
    }

    /**
     * Releases every writer currently blocked for the key and lets later snapshots pass through
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
        HELD_CHECKPOINT_IDS.remove(holdKey);
        WRITTEN_ROWS.remove(holdKey);
    }

    /**
     * Returns how many writers are blocked inside snapshotState for the key right now.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of held writers, zero when the key is unknown
     */
    public static int holdingWriters(String holdKey) {
        AtomicInteger counter = HOLDING_WRITERS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns the checkpoint id of the barrier currently held for the key.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return the held checkpoint id, or {@code -1} when no barrier was reached yet
     */
    public static long heldCheckpointId(String holdKey) {
        AtomicLong checkpointId = HELD_CHECKPOINT_IDS.get(holdKey);
        return checkpointId == null ? -1L : checkpointId.get();
    }

    /**
     * Records that the writer bound to the key received one row.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void onRow(String holdKey) {
        AtomicLong rows = WRITTEN_ROWS.get(holdKey);
        if (rows != null) {
            rows.incrementAndGet();
        }
    }

    /**
     * Returns how many rows the writer bound to the key has received.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return written row count, zero when the key is unknown
     */
    public static long writtenRows(String holdKey) {
        AtomicLong rows = WRITTEN_ROWS.get(holdKey);
        return rows == null ? 0L : rows.get();
    }

    /**
     * Blocks the calling writer until the key is released.
     *
     * <p>Returns immediately when the key was never armed or was already released, so a writer of a
     * restored or later checkpoint never holds again.
     *
     * @param holdKey the key configured on the sink
     * @param checkpointId the checkpoint whose barrier reached this writer
     */
    static void awaitRelease(String holdKey, long checkpointId) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch == null) {
            return;
        }
        AtomicInteger counter =
                HOLDING_WRITERS.computeIfAbsent(holdKey, key -> new AtomicInteger());
        HELD_CHECKPOINT_IDS.computeIfAbsent(holdKey, key -> new AtomicLong(-1L)).set(checkpointId);
        counter.incrementAndGet();
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Hold key {} was not released within {} ms, giving up the hold of checkpoint {}",
                            holdKey,
                            MAX_HOLD_MILLIS,
                            checkpointId);
                    return;
                }
                try {
                    if (latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                } catch (InterruptedException interruptedException) {
                    // Keep holding: a restore after the master switch may interrupt the task
                    // threads, but the task has to stay un-acknowledged until the test releases
                    // the gate.
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
