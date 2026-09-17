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

package org.apache.seatunnel.engine.server.checkpoint.scheduler;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * One checkpoint timer task, handed to callers as a {@link ScheduledFuture}.
 *
 * <p>The task crosses two thread pools: a shared timer thread fires it, and the body then runs on
 * the shared dispatch pool. Cancellation therefore has to work in both phases, which is why the
 * body checks {@link #cancelled} again at run time rather than relying on the timer future alone.
 */
final class PipelineCheckpointTask implements ScheduledFuture<Void>, Runnable {

    private final Runnable body;
    private final Consumer<PipelineCheckpointTask> onSettled;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    /**
     * The timer-side future, used only for {@link #getDelay(TimeUnit)} and to stop the task before
     * it fires. Assigned immediately after scheduling, so it may still be {@code null} for a task
     * that is cancelled in that window.
     */
    private volatile ScheduledFuture<?> timerFuture;

    PipelineCheckpointTask(Runnable body, Consumer<PipelineCheckpointTask> onSettled) {
        this.body = body;
        this.onSettled = onSettled;
    }

    void attachTimerFuture(ScheduledFuture<?> future) {
        this.timerFuture = future;
        // Losing the race against cancel() would leave a live timer entry behind.
        if (cancelled.get()) {
            future.cancel(false);
        }
    }

    /**
     * Runs on the dispatch pool after the timer has fired.
     *
     * <p>The cancellation check is a single read taken before the body starts, and that is
     * deliberate. A task already handed to the dispatch pool can still run its body once if {@link
     * #cancel(boolean)} lands after this check, so cancellation is best-effort for a task that has
     * already been dispatched, and guaranteed only for one that is still waiting on the timer.
     *
     * <p>That is safe because no caller relies on cancellation to prevent the body from running.
     * Both scheduled bodies re-validate coordinator and checkpoint state under the coordinator's
     * own lock before they act: the periodic trigger re-checks the completed/shutdown and pending
     * state, and the timeout watchdog re-checks that its checkpoint is still pending and not fully
     * acknowledged. A late body therefore finds the state it is asked to act on already gone and
     * does nothing.
     *
     * <p>Do not tighten this into a guarantee by holding a lock across {@code body.run()}. The body
     * can block on an RPC, so a lock held here would be held for the length of a checkpoint and
     * would serialise unrelated pipelines on the shared dispatch pool.
     *
     * <p>The body's own side effects therefore become visible before {@code onSettled} unregisters
     * this task, so an observer that watches for a side effect can still see this task as
     * outstanding. That ordering is deliberate: unregistering first would drop the task from the
     * set {@code cancelAll()} walks and leave a running body uncancellable. Nothing in the
     * coordinator depends on the reverse order, and the window closes as soon as the dispatch
     * thread leaves this method.
     */
    @Override
    public void run() {
        if (cancelled.get()) {
            return;
        }
        try {
            body.run();
            completion.complete(null);
        } catch (Throwable t) {
            completion.completeExceptionally(t);
            throw t;
        } finally {
            onSettled.accept(this);
        }
    }

    /**
     * Cancels the task in whichever phase it is currently in.
     *
     * <p>Before the timer fires this stops the body outright. After it has been dispatched the
     * outcome is a race with {@link #run()}, which is the intended contract rather than a defect;
     * see {@link #run()} for why a late body is harmless.
     *
     * <p>Returns {@code true} only for the caller that wins the transition, so repeated calls from
     * {@code cancelAll()} and from a coordinator holding the same future are idempotent.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!cancelled.compareAndSet(false, true)) {
            return false;
        }
        ScheduledFuture<?> timer = timerFuture;
        if (timer != null) {
            timer.cancel(mayInterruptIfRunning);
        }
        completion.cancel(mayInterruptIfRunning);
        onSettled.accept(this);
        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public boolean isDone() {
        return cancelled.get() || completion.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return completion.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return completion.get(timeout, unit);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        ScheduledFuture<?> timer = timerFuture;
        return timer == null ? 0L : timer.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed other) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
}
