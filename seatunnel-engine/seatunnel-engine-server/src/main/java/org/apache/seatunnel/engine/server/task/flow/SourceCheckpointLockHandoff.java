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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cooperative handoff of the source checkpoint lock from the reader thread to barrier injectors.
 *
 * <p>The reader thread holds the checkpoint lock for the whole duration of {@code
 * SourceReader#pollNext} and, under backpressure, every record it emits inside that call can block
 * on the bounded intermediate queue; one poll can therefore hold the lock for many seconds. A
 * barrier injector ({@link SourceFlowLifeCycle#triggerBarrier}) needs the same monitor. Java
 * monitors are not fair: when the reader releases the lock and immediately re-enters the next poll,
 * it wins the race against the parked injector almost every time, so a barrier can wait an
 * unbounded number of poll cycles. {@code Thread.sleep(0)} between polls does not change that.
 *
 * <p>This class turns the unfair race into an explicit handoff without touching the public {@code
 * Collector#getCheckpointLock()} contract: an injector registers itself before contending for the
 * lock and deregisters after releasing it, and the reader thread, at a point where it holds no
 * lock, waits until no injector is registered before starting its next poll. The barrier latency is
 * thereby bounded by a single poll instead of by luck.
 *
 * <p>Deadlock safety: the reader only waits while it holds no lock, and an injector never waits for
 * the reader, so an injector that is blocked (for example on the intermediate queue while
 * forwarding the barrier) still makes progress independently of the waiting reader. Injector
 * registration must always be released in a {@code finally} block so a failed injection cannot park
 * the reader forever.
 */
final class SourceCheckpointLockHandoff {

    /**
     * Sleep slice used by the reader while an injector is registered. Injection normally finishes
     * within a few milliseconds once it owns the lock, so a short slice keeps the added reader
     * latency negligible while avoiding a busy spin.
     */
    static final long WAIT_SLICE_MS = 1L;

    /**
     * Number of injectors that have announced themselves and not yet finished. More than one is
     * possible when a checkpoint and a savepoint barrier are triggered close together.
     */
    private final AtomicInteger pendingInjectors = new AtomicInteger();

    /**
     * Announces an injector that is about to contend for the checkpoint lock. Must be paired with
     * {@link #injectorFinished()} in a {@code finally} block.
     */
    void injectorArriving() {
        pendingInjectors.incrementAndGet();
    }

    /**
     * Withdraws an injector announcement after the injector released the checkpoint lock, whether
     * the injection succeeded or failed.
     */
    void injectorFinished() {
        pendingInjectors.decrementAndGet();
    }

    /**
     * Returns whether at least one injector is registered.
     *
     * @return true when an injector is waiting for or holding the checkpoint lock
     */
    boolean hasPendingInjector() {
        return pendingInjectors.get() > 0;
    }

    /**
     * Blocks the calling reader thread until no injector is registered. The caller must not hold
     * the checkpoint lock, otherwise the injector it waits for can never acquire it.
     *
     * @return nanoseconds spent waiting, so the caller can account the time as idle
     * @throws InterruptedException if the reader thread is interrupted, e.g. by task cancellation
     */
    long awaitInjectors() throws InterruptedException {
        if (!hasPendingInjector()) {
            return 0L;
        }
        long startNs = System.nanoTime();
        while (hasPendingInjector()) {
            Thread.sleep(WAIT_SLICE_MS);
        }
        return System.nanoTime() - startNs;
    }
}
