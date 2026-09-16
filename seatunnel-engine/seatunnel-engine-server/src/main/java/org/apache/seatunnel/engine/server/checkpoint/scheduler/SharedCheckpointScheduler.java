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

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Node-wide scheduler shared by every checkpoint coordinator on this member.
 *
 * <p>Before this existed, each {@code CheckpointCoordinator} built its own two-thread scheduled
 * pool, so timer threads grew with the number of active pipelines. The coordinators only ever used
 * that pool as a timer: to re-arm the periodic checkpoint trigger and to arm the checkpoint-timeout
 * watchdog. This class keeps a single small timer pool for the whole member and hands each
 * coordinator a {@link PipelineCheckpointScheduler} lease.
 *
 * <p>Timing and execution are deliberately split across two pools. A timer thread does nothing but
 * hand the task to the dispatch pool, so a checkpoint body that blocks on an RPC delays neither its
 * own pipeline's next timer nor any other pipeline's. The dispatch pool grows on demand up to a
 * bound and reaps idle threads, so it is not a fixed cost per pipeline either.
 *
 * <p>Checkpoint work is intentionally not dispatched onto the coordinator executor: that pool is
 * bounded over a {@code SynchronousQueue} and rejects work when saturated, which would silently
 * drop a checkpoint trigger or a timeout watchdog. The dispatch pool here is bounded but never
 * rejects, for the same reason: see {@link #MAX_DISPATCH_THREAD_NUM}.
 *
 * <p>One instance is held per member by {@code SeaTunnelEngineContext}. It is not a singleton
 * because several members run in one JVM during tests and E2E runs.
 */
@Slf4j
public class SharedCheckpointScheduler implements AutoCloseable {

    /**
     * Timer threads only dispatch, so a small fixed pool is enough regardless of pipeline count.
     * Two threads keep one slow dispatch hand-off from stalling the other timers.
     */
    private static final int TIMER_THREAD_NUM = 2;

    /** Dispatch threads scale with the host rather than with the pipeline count. */
    private static final int MIN_DISPATCH_THREAD_NUM = 8;

    private static final int DISPATCH_THREADS_PER_CORE = 2;

    /**
     * Upper bound on dispatch threads for the whole member, so that a member cannot grow threads
     * without limit when many pipelines block at once.
     *
     * <p>The bound is paired with an unbounded queue on purpose. A bounded queue would have to
     * reject once full, and a rejected task here is a dropped checkpoint trigger or a dropped
     * timeout watchdog, which is a correctness problem rather than a resource one. Queueing instead
     * trades an unbounded thread count for delay, and that delay is reachable only when every
     * dispatch thread is blocked at the same moment.
     *
     * <p>Running out of dispatch threads is not expected in normal operation: the heavy barrier
     * work already runs on the coordinator's own executor via {@code thenApplyAsync}, so a
     * dispatched body is short-lived.
     */
    private static final int MAX_DISPATCH_THREAD_NUM =
            Math.max(
                    MIN_DISPATCH_THREAD_NUM,
                    Runtime.getRuntime().availableProcessors() * DISPATCH_THREADS_PER_CORE);

    /**
     * Idle dispatch threads are reaped after this long, so an idle member pays for none of them.
     */
    private static final long DISPATCH_THREAD_KEEP_ALIVE_SECONDS = 60L;

    private final ScheduledThreadPoolExecutor timer;
    private final ThreadPoolExecutor dispatcher;
    private volatile boolean closed = false;

    public SharedCheckpointScheduler() {
        this.timer =
                new ScheduledThreadPoolExecutor(
                        TIMER_THREAD_NUM,
                        new ThreadFactoryBuilder()
                                .setNameFormat("checkpoint-timer-%d")
                                .setDaemon(true)
                                .build());
        // Coordinators cancel the timeout watchdog on every acknowledged checkpoint. Without this,
        // cancelled entries would sit in the shared queue until their delay elapsed.
        this.timer.setRemoveOnCancelPolicy(true);
        // Core and maximum are equal because a ThreadPoolExecutor backed by an unbounded queue
        // never grows past its core size; allowCoreThreadTimeOut then gives back the elasticity,
        // so threads are still created on demand and reaped once idle.
        this.dispatcher =
                new ThreadPoolExecutor(
                        MAX_DISPATCH_THREAD_NUM,
                        MAX_DISPATCH_THREAD_NUM,
                        DISPATCH_THREAD_KEEP_ALIVE_SECONDS,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ThreadFactoryBuilder()
                                .setNameFormat("checkpoint-dispatcher-%d")
                                .setDaemon(true)
                                .build());
        this.dispatcher.allowCoreThreadTimeOut(true);
    }

    /**
     * Creates a timer lease for one pipeline.
     *
     * <p>The lease is the cancellation unit: it is identified by {@code (jobId, pipelineId)} and
     * holds only its own outstanding tasks. Nothing is registered on this object, so dropping a
     * lease when its job ends leaks nothing.
     *
     * @param jobId the owning job
     * @param pipelineId the owning pipeline
     * @return a lease that borrows this member's shared timer and dispatch threads
     */
    public PipelineCheckpointScheduler lease(long jobId, int pipelineId) {
        return new PipelineCheckpointScheduler(this, jobId, pipelineId);
    }

    /**
     * Arms {@code task} on the shared timer so that it is handed to the dispatch pool after {@code
     * delay}.
     *
     * @return the timer-side future, or {@code null} if this scheduler is already shutting down
     */
    ScheduledFuture<?> dispatchAfter(PipelineCheckpointTask task, long delay, TimeUnit unit) {
        try {
            return timer.schedule(() -> submit(task), delay, unit);
        } catch (RejectedExecutionException e) {
            // Only reachable once close() has begun; the coordinator is stopping anyway.
            log.debug("Shared checkpoint scheduler is shut down, dropping timer task", e);
            task.cancel(false);
            return null;
        }
    }

    private void submit(PipelineCheckpointTask task) {
        try {
            dispatcher.execute(task);
        } catch (RejectedExecutionException e) {
            log.debug("Shared checkpoint dispatcher is shut down, dropping checkpoint task", e);
            task.cancel(false);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    /** Exposed for tests asserting that the timer thread count does not grow with pipelines. */
    public int getTimerPoolSize() {
        return timer.getPoolSize();
    }

    /** Exposed for tests asserting that the dispatch pool respects its bound. */
    int getDispatcherPoolSize() {
        return dispatcher.getPoolSize();
    }

    /** The member-wide ceiling on dispatch threads. */
    static int getMaxDispatchThreadNum() {
        return MAX_DISPATCH_THREAD_NUM;
    }

    /** Shuts down the shared threads. Called once per member, when the engine context closes. */
    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
        dispatcher.shutdownNow();
    }
}
