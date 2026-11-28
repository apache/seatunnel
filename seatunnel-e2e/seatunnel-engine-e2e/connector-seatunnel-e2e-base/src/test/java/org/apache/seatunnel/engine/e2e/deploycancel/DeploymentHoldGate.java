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

package org.apache.seatunnel.engine.e2e.deploycancel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cross-thread handshake between {@link DeploymentHoldSinkWriter} (worker side, inside the action
 * restore function) and {@link CancelDuringDeploymentIT} (test thread).
 *
 * <p>The engine E2E cluster runs inside the test JVM, so the writer can park the task deployment on
 * a latch owned by the test and report exactly what happened to that parked call instead of relying
 * on sleeps to guess that deployment is still in flight.
 */
public final class DeploymentHoldGate {

    /**
     * Upper bound for the parked writer creation. It stays far above the test's cancellation bound,
     * so a deployment that a cancel waits for surfaces as a failed assertion instead of a silently
     * self-released gate.
     */
    private static final long HOLD_TIMEOUT_MILLIS = 120_000L;

    private static final AtomicInteger OPEN_ATTEMPTS = new AtomicInteger();
    private static final AtomicInteger WRITTEN_ROWS = new AtomicInteger();
    private static final AtomicBoolean TIMED_OUT = new AtomicBoolean();

    private static volatile CountDownLatch openEntered = new CountDownLatch(1);
    private static volatile CountDownLatch release = new CountDownLatch(1);

    private DeploymentHoldGate() {}

    /** Clears every observation of a previous job before the next submission. */
    public static void reset() {
        openEntered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        OPEN_ATTEMPTS.set(0);
        WRITTEN_ROWS.set(0);
        TIMED_OUT.set(false);
    }

    /** Completed once a sink writer creation reached the hold. */
    public static CountDownLatch openEntered() {
        return openEntered;
    }

    /**
     * Parks the calling worker thread inside the sink writer creation, which the engine runs from
     * the action's restore function before the task can report {@code READY_START}.
     *
     * <p>Returns immediately once {@link #release()} was called, so every writer created after the
     * cancelled deployment proceeds normally. A job cancellation does not interrupt this call: the
     * engine resolves the cancel independently, which is exactly the property the test pins.
     */
    static void awaitDeploymentHold() {
        CountDownLatch entered = openEntered;
        CountDownLatch gate = release;
        OPEN_ATTEMPTS.incrementAndGet();
        entered.countDown();
        long deadline = System.currentTimeMillis() + HOLD_TIMEOUT_MILLIS;
        try {
            while (System.currentTimeMillis() < deadline) {
                if (gate.await(
                        Math.min(200L, deadline - System.currentTimeMillis()),
                        TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
            TIMED_OUT.set(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Sink writer creation was interrupted before the deployment was released", e);
        }
    }

    /** Releases a parked writer creation and lets every later writer be created immediately. */
    public static void release() {
        release.countDown();
    }

    static void recordRow() {
        WRITTEN_ROWS.incrementAndGet();
    }

    public static int openAttempts() {
        return OPEN_ATTEMPTS.get();
    }

    public static int writtenRows() {
        return WRITTEN_ROWS.get();
    }

    public static boolean timedOut() {
        return TIMED_OUT.get();
    }
}
