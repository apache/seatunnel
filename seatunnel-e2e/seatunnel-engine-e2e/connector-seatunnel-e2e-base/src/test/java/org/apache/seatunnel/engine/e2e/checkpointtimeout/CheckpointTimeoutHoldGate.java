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

package org.apache.seatunnel.engine.e2e.checkpointtimeout;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only synchronization point that lets the checkpoint timeout E2E control the sink side of a
 * checkpoint.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs inside the test JVM, so a static registry
 * keyed by a per-test hold key is enough to coordinate the test thread with the task thread that
 * owns the sink writer.
 *
 * <p>Two properties are essential for the scenario:
 *
 * <ul>
 *   <li>The writer must hold the snapshot of exactly one, configurable checkpoint (the
 *       <em>held</em> checkpoint) and must never hold again after that, because the writer of the
 *       restored pipeline is a different instance and has to acknowledge its barriers normally.
 *   <li>Interrupts delivered while holding (for example from the pipeline cancellation that follows
 *       the checkpoint timeout) must not end the hold: from the master's point of view the task
 *       must stay blocked until the test has observed the timeout, otherwise the whole window under
 *       test would collapse into an ordinary checkpoint failure with no pending task left.
 * </ul>
 *
 * <p>A hard deadline guarantees that a broken test can never leak a blocked task thread forever.
 */
@Slf4j
public final class CheckpointTimeoutHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Armed gates keyed by hold key. */
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();

    private CheckpointTimeoutHoldGate() {}

    /**
     * Arms a gate for the given key. Called by the test before the job is submitted; the sink
     * resolves the same gate lazily from its hold key.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new Gate());
    }

    /**
     * Releases the writer currently parked in the snapshot of the held checkpoint. Safe to call
     * repeatedly and for keys that were never armed.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        Gate gate = GATES.get(holdKey);
        if (gate != null) {
            gate.release.countDown();
        }
    }

    /**
     * Releases and forgets the key so that a rerun with the same key starts from a clean registry.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
    }

    /**
     * Blocks the calling writer until the test releases the held snapshot. Returns immediately when
     * the key was never armed, was already released, or the current snapshot is not the configured
     * held snapshot, so the writer of a restored pipeline never holds again.
     *
     * @param holdKey the key configured on the sink
     * @param snapshotNumber one-based number of the checkpoint snapshot that must be held
     * @param checkpointId id of the checkpoint barrier being snapshotted
     * @return {@code true} when this call parked on the gate, {@code false} when it passed through
     */
    static boolean holdSnapshot(String holdKey, int snapshotNumber, long checkpointId) {
        Gate gate = GATES.get(holdKey);
        if (gate == null) {
            return false;
        }
        if (gate.snapshotCount.incrementAndGet() != snapshotNumber) {
            return false;
        }
        gate.heldCheckpointId = checkpointId;
        gate.holding.set(true);
        gate.entered.countDown();
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Checkpoint snapshot hold for key {} was not released within {} ms, giving up the hold",
                            holdKey,
                            MAX_HOLD_MILLIS);
                    return true;
                }
                try {
                    if (gate.release.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return true;
                    }
                } catch (InterruptedException interruptedException) {
                    // Keep holding: the pipeline cancellation following the checkpoint timeout
                    // interrupts task threads, but the test needs the task to stay blocked until
                    // it has observed the timeout on the master.
                    interrupted = true;
                }
            }
        } finally {
            gate.holding.set(false);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Records a row received by any writer of this job. */
    static void addRows(String holdKey, long rows) {
        Gate gate = GATES.get(holdKey);
        if (gate != null) {
            gate.rowsWritten.addAndGet(rows);
        }
    }

    /** Records the state handed to a restored writer. */
    static void recordRestoredState(String holdKey, String state) {
        Gate gate = GATES.get(holdKey);
        if (gate != null) {
            gate.restoredState = state;
            gate.restoreWriterCalls.incrementAndGet();
        }
    }

    /** Waits until a writer is parked in the configured held snapshot. */
    public static void awaitHeld(String holdKey, long timeout, TimeUnit unit)
            throws InterruptedException {
        Gate gate = GATES.get(holdKey);
        if (gate == null) {
            throw new IllegalStateException("No checkpoint timeout gate armed for key " + holdKey);
        }
        if (!gate.entered.await(timeout, unit)) {
            throw new IllegalStateException(
                    "The sink writer did not reach the held checkpoint snapshot within "
                            + unit.toMillis(timeout)
                            + " ms");
        }
    }

    /** Checkpoint id of the held snapshot, or {@code -1} while no snapshot is held. */
    public static long heldCheckpointId(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? -1L : gate.heldCheckpointId;
    }

    /** Number of snapshot calls the sink writer has served so far. */
    public static int snapshotCount(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? 0 : gate.snapshotCount.get();
    }

    /** Number of rows the sink writers have received so far. */
    public static long rowsWritten(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? 0L : gate.rowsWritten.get();
    }

    /** Number of times a restored writer has been created for this job. */
    public static int restoreWriterCalls(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? 0 : gate.restoreWriterCalls.get();
    }

    /** State of the last completed checkpoint handed to a restored writer, or {@code null}. */
    public static String restoredState(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate == null ? null : gate.restoredState;
    }

    /** Whether a writer is parked in the held snapshot right now. */
    public static boolean isHolding(String holdKey) {
        Gate gate = GATES.get(holdKey);
        return gate != null && gate.holding.get();
    }

    /** Mutable per-key state of the gate. */
    private static final class Gate {
        private final AtomicInteger snapshotCount = new AtomicInteger();
        private final AtomicInteger restoreWriterCalls = new AtomicInteger();
        private final AtomicLong rowsWritten = new AtomicLong();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile long heldCheckpointId = -1L;
        private volatile String restoredState;
        private final AtomicBoolean holding = new AtomicBoolean();
    }
}
