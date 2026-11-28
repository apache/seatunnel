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

package org.apache.seatunnel.engine.e2e.cancelcheckpoint;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only synchronization point that keeps a sink writer blocked inside {@code
 * snapshotState(long)} until the test releases it.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs entirely inside the test JVM, so a static
 * registry keyed by a per-test hold key is enough to coordinate the test thread with the task
 * thread that owns the writer. A checkpoint barrier only reaches the sink writer after every
 * upstream task acknowledged it, so parking the writer keeps the whole checkpoint in the "barrier
 * distributed but not acknowledged by every task" window: the master has allocated the checkpoint
 * id, the tasks hold its subtask state, and nothing is durable yet.
 *
 * <p>The gate also publishes the id of the checkpoint that is currently held, so the test can name
 * the interrupted checkpoint in its assertions without guessing from the checkpoint cadence.
 *
 * <p>An interrupt delivered while holding (for example from {@code TaskExecutionService#shutdown()}
 * when a worker is torn down) does not end the hold: from the master's point of view the task must
 * stay un-acknowledged until the test decides otherwise. A hard deadline guarantees that a failing
 * test can never leak a thread that blocks forever.
 */
@Slf4j
public final class CheckpointInFlightHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Armed gates keyed by hold key; a missing entry means "never hold". */
    private static final Map<String, CountDownLatch> GATES = new ConcurrentHashMap<>();

    /** Number of writers currently blocked inside snapshotState() per hold key. */
    private static final Map<String, AtomicInteger> HOLDING_WRITERS = new ConcurrentHashMap<>();

    /** Id of the checkpoint currently held per hold key. */
    private static final Map<String, AtomicLong> HELD_CHECKPOINT_IDS = new ConcurrentHashMap<>();

    private CheckpointInFlightHoldGate() {}

    /**
     * Arms the gate: every writer that snapshots after this call blocks until {@link
     * #release(String)}.
     *
     * @param holdKey unique key shared between the test and the sink configuration
     */
    public static void arm(String holdKey) {
        GATES.put(holdKey, new CountDownLatch(1));
        HOLDING_WRITERS.putIfAbsent(holdKey, new AtomicInteger());
        HELD_CHECKPOINT_IDS.putIfAbsent(holdKey, new AtomicLong(-1L));
    }

    /**
     * Releases every writer currently blocked for the key. Snapshots taken after the release pass
     * through until the key is armed again.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        CountDownLatch latch = GATES.get(holdKey);
        if (latch != null) {
            latch.countDown();
        }
    }

    /** Releases and forgets the key, used by test teardown. */
    public static void clear(String holdKey) {
        release(holdKey);
        GATES.remove(holdKey);
        HOLDING_WRITERS.remove(holdKey);
        HELD_CHECKPOINT_IDS.remove(holdKey);
    }

    /**
     * Returns how many writers are blocked inside snapshotState() for the key right now.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of held writers, zero when the key is unknown
     */
    public static int holdingWriters(String holdKey) {
        AtomicInteger counter = HOLDING_WRITERS.get(holdKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Returns the id of the checkpoint that the gate is currently holding, or {@code -1} when no
     * snapshot entered the gate yet.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static long heldCheckpointId(String holdKey) {
        AtomicLong id = HELD_CHECKPOINT_IDS.get(holdKey);
        return id == null ? -1L : id.get();
    }

    /**
     * Blocks the calling writer until the key is released. Returns immediately when the key was
     * never armed or its current latch was already released, so checkpoints triggered after the
     * test's window no longer hold.
     *
     * @param holdKey the key configured on the sink
     * @param checkpointId the checkpoint whose barrier is being acknowledged
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
        log.warn("Holding checkpoint {} snapshot on key {}", checkpointId, holdKey);
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
                    // Must keep the task un-acknowledged; see the class comment.
                    interrupted = true;
                }
            }
        } finally {
            counter.decrementAndGet();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            log.warn("Released checkpoint {} snapshot hold on key {}", checkpointId, holdKey);
        }
    }
}
