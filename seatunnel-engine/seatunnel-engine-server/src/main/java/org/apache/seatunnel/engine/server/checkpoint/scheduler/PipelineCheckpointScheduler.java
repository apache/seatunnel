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

import org.apache.seatunnel.api.tracing.MDCTracer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The timer view that a single checkpoint coordinator owns.
 *
 * <p>A lease borrows the node-wide threads held by {@link SharedCheckpointScheduler} but keeps its
 * own set of outstanding tasks, so cancelling one pipeline never touches another pipeline's timers.
 * Instances are obtained from {@link SharedCheckpointScheduler#lease(long, int)}.
 */
public final class PipelineCheckpointScheduler {

    private final SharedCheckpointScheduler parent;
    private final long jobId;
    private final int pipelineId;

    /**
     * Tasks that have been scheduled and have not finished yet. Entries remove themselves once the
     * body has run or the task has been cancelled, so a long-lived pipeline does not accumulate
     * completed tasks.
     */
    private final Set<PipelineCheckpointTask> outstanding = ConcurrentHashMap.newKeySet();

    PipelineCheckpointScheduler(SharedCheckpointScheduler parent, long jobId, int pipelineId) {
        this.parent = parent;
        this.jobId = jobId;
        this.pipelineId = pipelineId;
    }

    /**
     * Schedules {@code command} to run after {@code delay}.
     *
     * <p>The shared timer thread only dispatches the task; the body itself runs on the shared
     * dispatch pool so that a blocking checkpoint or RPC cannot delay another pipeline's timers.
     * The returned future stops the body whether it is cancelled before the timer fires or after
     * the body has been handed to the dispatch pool.
     *
     * @param command the body to run
     * @param delay the delay before the body runs
     * @param unit the unit of {@code delay}
     * @return a future that cancels the body, never {@code null}
     */
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        PipelineCheckpointTask task =
                new PipelineCheckpointTask(MDCTracer.tracing(command), outstanding::remove);
        if (parent.isClosed()) {
            task.cancel(false);
            return task;
        }
        outstanding.add(task);
        // The parent may have been closed between the check above and the schedule call, in which
        // case dispatchAfter has already cancelled the task and returns no timer future.
        ScheduledFuture<?> timerFuture = parent.dispatchAfter(task, delay, unit);
        if (timerFuture != null) {
            task.attachTimerFuture(timerFuture);
        }
        return task;
    }

    /**
     * Cancels every task this pipeline still has outstanding, leaving other pipelines untouched.
     *
     * <p>Called on terminal cleanup and on master-failover reset. The lease stays usable
     * afterwards, so a coordinator that is restored after a reset can schedule again without
     * rebuilding any thread pool.
     */
    public void cancelAll() {
        // Copy first: cancelling removes entries from the same set.
        for (PipelineCheckpointTask task : outstanding.toArray(new PipelineCheckpointTask[0])) {
            task.cancel(false);
        }
        outstanding.clear();
    }

    /** Returns whether the underlying shared scheduler has been shut down. */
    public boolean isShutdown() {
        return parent.isClosed();
    }

    public long getJobId() {
        return jobId;
    }

    public int getPipelineId() {
        return pipelineId;
    }

    /** Exposed for tests that assert per-pipeline cancellation. */
    int outstandingCount() {
        return outstanding.size();
    }
}
