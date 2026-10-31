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

package org.apache.seatunnel.engine.server.execution;

import lombok.NonNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cooperative test task that records its own progress, so a test can tell which tasks ran instead
 * of only how many calls happened in total.
 *
 * <p>Each call takes longer than the cooperative call timer allows, either by sleeping or, when a
 * gate is given, by blocking until that gate opens. A gated task therefore holds its worker exactly
 * the way a task blocked on an external resource does.
 */
public class CooperativeProbeTask implements Task {

    private final long taskId;
    private final long callTimeMillis;
    private final CountDownLatch gate;
    private final CountDownLatch gateToOpen;
    private final AtomicBoolean stop;
    private final CountDownLatch started = new CountDownLatch(1);
    private final AtomicInteger callCount = new AtomicInteger();

    /** A task whose every call takes {@code callTimeMillis}. */
    public static CooperativeProbeTask slowTask(
            long taskId, long callTimeMillis, AtomicBoolean stop) {
        return new CooperativeProbeTask(taskId, callTimeMillis, stop, null, null);
    }

    /** A task whose every call blocks until {@code gate} opens. */
    public static CooperativeProbeTask gatedTask(
            long taskId, AtomicBoolean stop, CountDownLatch gate) {
        return new CooperativeProbeTask(taskId, 0, stop, gate, null);
    }

    /** A task that opens {@code gateToOpen} as soon as one of its calls starts. */
    public static CooperativeProbeTask gateOpeningTask(
            long taskId, long callTimeMillis, AtomicBoolean stop, CountDownLatch gateToOpen) {
        return new CooperativeProbeTask(taskId, callTimeMillis, stop, null, gateToOpen);
    }

    private CooperativeProbeTask(
            long taskId,
            long callTimeMillis,
            AtomicBoolean stop,
            CountDownLatch gate,
            CountDownLatch gateToOpen) {
        this.taskId = taskId;
        this.callTimeMillis = callTimeMillis;
        this.stop = stop;
        this.gate = gate;
        this.gateToOpen = gateToOpen;
    }

    @NonNull @Override
    public ProgressState call() throws Exception {
        started.countDown();
        callCount.incrementAndGet();
        if (gateToOpen != null) {
            gateToOpen.countDown();
        }
        if (gate == null) {
            Thread.sleep(callTimeMillis);
        } else if (!gate.await(2, TimeUnit.MINUTES)) {
            throw new IllegalStateException("Task " + taskId + " was never released by its gate");
        }
        return stop.get() ? ProgressState.DONE : ProgressState.MADE_PROGRESS;
    }

    @NonNull @Override
    public Long getTaskID() {
        return taskId;
    }

    @Override
    public boolean isThreadsShare() {
        return true;
    }

    /** Waits until this task has entered its first call. */
    public boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return started.await(timeout, unit);
    }

    public boolean isStarted() {
        return started.getCount() == 0;
    }

    public int getCallCount() {
        return callCount.get();
    }
}
