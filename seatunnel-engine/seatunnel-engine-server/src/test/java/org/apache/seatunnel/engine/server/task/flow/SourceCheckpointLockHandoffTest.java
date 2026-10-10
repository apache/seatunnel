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

package org.apache.seatunnel.engine.server.task.flow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers the reader-to-injector checkpoint lock handoff that {@link SourceFlowLifeCycle} relies on
 * to keep checkpoint barriers from starving behind a busy source.
 *
 * <p>Regression risk guarded here: a reader that holds the checkpoint lock for long polls and
 * immediately re-enters the next poll used to win the unfair monitor race against the barrier
 * injector indefinitely, so checkpoints under sustained backpressure completed only when the
 * scheduler happened to favor the injector (observed as
 * BackpressureSlowSinkIT#testCheckpointsKeepCompletingUnderSustainedBackpressure timing out on
 * dev). With the handoff, the injector must acquire the lock within a single poll cycle.
 */
public class SourceCheckpointLockHandoffTest {

    /** Simulated lock hold time of one poll; long enough that a starved injector is visible. */
    private static final long POLL_HOLD_MS = 100L;

    /** Upper bound on how long any thread in these tests may block before the test fails. */
    private static final long JOIN_TIMEOUT_MS = 10_000L;

    /**
     * Without any injector the reader must not pause at all, otherwise the handoff would add
     * latency to every poll of every source.
     */
    @Test
    public void awaitReturnsImmediatelyWhenNoInjectorIsPending() throws InterruptedException {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        Assertions.assertFalse(handoff.hasPendingInjector());
        Assertions.assertEquals(0L, handoff.awaitInjectors());
    }

    /**
     * Each injector announcement must be matched by its own completion: the reader may only resume
     * once every announced injector has finished.
     */
    @Test
    public void pendingStateTracksEveryInjectorIndependently() {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        handoff.injectorArriving();
        Assertions.assertTrue(handoff.hasPendingInjector());
        handoff.injectorFinished();
        Assertions.assertTrue(
                handoff.hasPendingInjector(), "one injector is still pending after the first left");
        handoff.injectorFinished();
        Assertions.assertFalse(handoff.hasPendingInjector());
    }

    /**
     * The core guarantee: a reader that re-acquires the lock in a tight loop yields to an injector
     * within one poll cycle. The injector's acquisition is measured in reader poll cycles, so the
     * assertion does not depend on wall-clock scheduling noise.
     */
    @Test
    public void injectorAcquiresLockWithinOnePollCycle() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        Object checkpointLock = new Object();
        AtomicInteger completedPolls = new AtomicInteger();
        AtomicBoolean readerRunning = new AtomicBoolean(true);
        CountDownLatch readerHoldsLock = new CountDownLatch(1);
        AtomicInteger pollsWhenInjectorAcquired = new AtomicInteger(-1);

        Thread reader =
                new Thread(
                        () -> {
                            try {
                                while (readerRunning.get()) {
                                    synchronized (checkpointLock) {
                                        readerHoldsLock.countDown();
                                        Thread.sleep(POLL_HOLD_MS);
                                    }
                                    completedPolls.incrementAndGet();
                                    // Same call site as SourceFlowLifeCycle#collect: outside the
                                    // lock, before the next poll.
                                    handoff.awaitInjectors();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "reader");
        reader.start();
        Assertions.assertTrue(
                readerHoldsLock.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "reader never started polling");

        AtomicInteger pollsBeforeInjector = new AtomicInteger(-1);
        Thread injector =
                new Thread(
                        () -> {
                            // Sampled right before announcing, on this thread, so the bound below
                            // cannot be inflated by polls that completed while the thread started.
                            pollsBeforeInjector.set(completedPolls.get());
                            handoff.injectorArriving();
                            try {
                                synchronized (checkpointLock) {
                                    pollsWhenInjectorAcquired.set(completedPolls.get());
                                }
                            } finally {
                                handoff.injectorFinished();
                            }
                        },
                        "injector");
        injector.start();
        injector.join(JOIN_TIMEOUT_MS);
        readerRunning.set(false);
        reader.join(JOIN_TIMEOUT_MS);

        Assertions.assertFalse(injector.isAlive(), "injector never acquired the checkpoint lock");
        Assertions.assertFalse(reader.isAlive(), "reader did not stop");
        int pollsWaited = pollsWhenInjectorAcquired.get() - pollsBeforeInjector.get();
        Assertions.assertTrue(
                pollsWaited >= 0 && pollsWaited <= 1,
                "injector should acquire the lock after at most the poll in flight, but waited "
                        + pollsWaited
                        + " poll cycles");
        Assertions.assertFalse(handoff.hasPendingInjector());
    }

    /**
     * A reader parked in the handoff must be released when the injector finishes, including when
     * the injection failed, which is why callers deregister in a finally block.
     */
    @Test
    public void waitingReaderResumesWhenInjectorFinishes() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        CountDownLatch readerResumed = new CountDownLatch(1);
        Thread reader =
                new Thread(
                        () -> {
                            try {
                                handoff.awaitInjectors();
                                readerResumed.countDown();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "reader");
        reader.start();
        Assertions.assertFalse(
                readerResumed.await(
                        5 * SourceCheckpointLockHandoff.WAIT_SLICE_MS + 50L, TimeUnit.MILLISECONDS),
                "reader must stay parked while an injector is pending");

        handoff.injectorFinished();
        Assertions.assertTrue(
                readerResumed.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "reader must resume once the injector finished");
        reader.join(JOIN_TIMEOUT_MS);
        Assertions.assertFalse(reader.isAlive());
    }

    /**
     * Task cancellation interrupts the reader thread; a reader parked in the handoff must surface
     * that interrupt instead of swallowing it and looping forever.
     */
    @Test
    public void waitingReaderHonoursInterrupt() throws Exception {
        SourceCheckpointLockHandoff handoff = new SourceCheckpointLockHandoff();
        handoff.injectorArriving();
        AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread reader =
                new Thread(
                        () -> {
                            try {
                                handoff.awaitInjectors();
                            } catch (InterruptedException e) {
                                interrupted.set(true);
                            }
                        },
                        "reader");
        reader.start();
        reader.interrupt();
        reader.join(JOIN_TIMEOUT_MS);
        Assertions.assertFalse(reader.isAlive(), "interrupted reader did not exit the handoff");
        Assertions.assertTrue(interrupted.get(), "InterruptedException must propagate");
    }
}
