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

import java.util.concurrent.CompletableFuture;
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

    /** Runs on the dispatch pool after the timer has fired. */
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
